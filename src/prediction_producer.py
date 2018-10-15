import json
import socket
import time
from contextlib import contextmanager
from multiprocessing import Process

import cv2
import face_recognition
import numpy as np
from kafka import KafkaConsumer, KafkaProducer
from kafka.coordinator.assignors.range import RangePartitionAssignor
from kafka.coordinator.assignors.roundrobin import RoundRobinPartitionAssignor
from kafka.partitioner import RoundRobinPartitioner, Murmur2Partitioner
from kafka.structs import OffsetAndMetadata, TopicPartition

from src.params import *
from src.utils import np_from_json, np_to_json


class ConsumeFrames(Process):

    def __init__(self,
                 frame_topic,
                 processed_frame_topic,
                 topic_partitions=8,
                 scale=1.0,
                 verbose=False,
                 rr_distribute=False,
                 group=None,
                 target=None,
                 name=None):
        """
        FACE DETECTION IN FRAMES --> Consuming encoded frame messages, detect faces and their encodings [PRE PROCESS],
        publish it to processed_frame_topic where these values are used for face matching with query faces.

        :param frame_topic: kafka topic to consume stamped encoded frames.
        :param processed_frame_topic: kafka topic to publish stamped encoded frames with face detection and encodings.
        :param topic_partitions: number of partitions processed_frame_topic topic has, for distributing messages among partitions
        :param scale: (0, 1] scale image before face recognition, but less accurate, trade off!!
        :param verbose: print logs on stdout
        :param rr_distribute:  use round robin partitioner and assignor, should be set same as respective producers or consumers.
        :param group: group should always be None; it exists solely for compatibility with threading.
        :param target: Process Target
        :param name: Process name
        """

        super().__init__(group=group, target=target, name=name)

        self.iam = "{}-{}".format(socket.gethostname(), self.name)
        self.frame_topic = frame_topic

        self.verbose = verbose
        self.scale = scale
        self.topic_partitions = topic_partitions
        self.processed_frame_topic = processed_frame_topic
        self.rr_distribute = rr_distribute
        print("[INFO] I am ", self.iam)

    def run(self):
        """Consume raw frames, detects faces, finds their encoding [PRE PROCESS],
           predictions Published to processed_frame_topic fro face matching."""

        # Connect to kafka, Consume frame obj bytes deserialize to json
        partition_assignment_strategy = [RoundRobinPartitionAssignor] if self.rr_distribute else [
            RangePartitionAssignor,
            RoundRobinPartitionAssignor]

        frame_consumer = KafkaConsumer(group_id="consume", client_id=self.iam,
                                       bootstrap_servers=["0.0.0.0:9092"],
                                       key_deserializer=lambda key: key.decode(),
                                       value_deserializer=lambda value: json.loads(value.decode()),
                                       partition_assignment_strategy=partition_assignment_strategy,
                                       auto_offset_reset="earliest")

        frame_consumer.subscribe([self.frame_topic])

        # partitioner for processed frame topic
        if self.rr_distribute:
            partitioner = RoundRobinPartitioner(partitions=
                                                [TopicPartition(topic=self.frame_topic, partition=i)
                                                 for i in range(self.topic_partitions)])

        else:

            partitioner = Murmur2Partitioner(partitions=
                                             [TopicPartition(topic=self.frame_topic, partition=i)
                                              for i in range(self.topic_partitions)])
        #  Produces prediction object
        processed_frame_producer = KafkaProducer(bootstrap_servers=["localhost:9092"],
                                                 key_serializer=lambda key: str(key).encode(),
                                                 value_serializer=lambda value: json.dumps(value).encode(),
                                                 partitioner=partitioner)

        try:
            while True:

                if self.verbose:
                    print("[ConsumeFrames {}] WAITING FOR NEXT FRAMES..".format(socket.gethostname()))

                raw_frame_messages = frame_consumer.poll(timeout_ms=10, max_records=10)

                for topic_partition, msgs in raw_frame_messages.items():

                    # Get the predicted Object, JSON with frame and meta info about the frame
                    for msg in msgs:
                        # get pre processing result
                        result = self.get_processed_frame_object(msg.value, self.scale)

                        tp = TopicPartition(msg.topic, msg.partition)
                        offsets = {tp: OffsetAndMetadata(msg.offset, None)}
                        frame_consumer.commit(offsets=offsets)

                        # Partition to be sent to
                        processed_frame_producer.send(self.processed_frame_topic,
                                                      key="{}_{}".format(result["camera"], result["frame_num"]),
                                                      value=result)

                    processed_frame_producer.flush()

        except KeyboardInterrupt as e:
            print(e)
            pass

        finally:
            print("Closing Stream")
            frame_consumer.close()

    @staticmethod
    def get_processed_frame_object(frame_obj, scale=1.0):
        """Processes value produced by producer, returns prediction with png image.

        :param frame_obj: frame dictionary with frame information and frame itself
        :param scale: (0, 1] scale image before face recognition, speeds up processing, decreases accuracy
        :return: A dict updated with faces found in that frame, i.e. their location and encoding.
        """

        frame = np_from_json(frame_obj, prefix_name=ORIGINAL_PREFIX)  # frame_obj = json
        # Convert the image from BGR color (which OpenCV uses) to RGB color (which face_recognition uses)
        frame = cv2.cvtColor(frame.astype(np.uint8), cv2.COLOR_BGR2RGB)

        if scale != 1:
            # Resize frame of video to scale size for faster face recognition processing
            rgb_small_frame = cv2.resize(frame, (0, 0), fx=scale, fy=scale)

        else:
            rgb_small_frame = frame

        with timer("PROCESS RAW FRAME {}".format(frame_obj["frame_num"])):
            # Find all the faces and face encodings in the current frame of video
            with timer("Locations in frame"):
                face_locations = np.array(face_recognition.face_locations(rgb_small_frame))
                face_locations_dict = np_to_json(face_locations, prefix_name="face_locations")

            with timer("Encodings in frame"):
                face_encodings = np.array(face_recognition.face_encodings(rgb_small_frame, face_locations))
                face_encodings_dict = np_to_json(face_encodings, prefix_name="face_encodings")

        frame_obj.update(face_locations_dict)
        frame_obj.update(face_encodings_dict)

        return frame_obj


class PredictFrames(Process):

    def __init__(self,
                 processed_frame_topic,
                 query_faces_topic,
                 scale=1.0,
                 verbose=False,
                 rr_distribute=False,
                 group=None,
                 target=None,
                 name=None):
        """
        FACE MATCHING TO QUERY FACES --> Consuming frame objects to produce predictions.

        :param processed_frame_topic: kafka topic to consume from stamped encoded frames with face detection and encodings.
        :param query_faces_topic: kafka topic which broadcasts query face names and encodings.
        :param scale: (0, 1] scale used during pre processing step.
        :param verbose: print log
        :param rr_distribute: use round robin partitioner and assignor, should be set same as respective producers or consumers.
        :param group: group should always be None; it exists solely for compatibility with threading.
        :param target: Process Target
        :param name: Process name
        """
        super().__init__(group=group, target=target, name=name)

        self.iam = "{}-{}".format(socket.gethostname(), self.name)
        self.frame_topic = processed_frame_topic
        self.query_faces_topic = query_faces_topic
        self.verbose = verbose
        self.scale = scale
        self.rr_distribute = rr_distribute
        print("[INFO] I am ", self.iam)

    def run(self):
        """Consume pre processed frames, match query face with faces detected in pre processing step
        (published to processed frame topic) publish results, box added to frame data if in params,
        ORIGINAL_PREFIX == PREDICTED_PREFIX"""

        # Connect to kafka, Consume frame obj bytes deserialize to json
        partition_assignment_strategy = [RoundRobinPartitionAssignor] if self.rr_distribute else [
            RangePartitionAssignor,
            RoundRobinPartitionAssignor]

        frame_consumer = KafkaConsumer(group_id="consume", client_id=self.iam,
                                       bootstrap_servers=["0.0.0.0:9092"],
                                       key_deserializer=lambda key: key.decode(),
                                       value_deserializer=lambda value: json.loads(value.decode()),
                                       partition_assignment_strategy=partition_assignment_strategy,
                                       auto_offset_reset="earliest")

        frame_consumer.subscribe([self.frame_topic])

        #  Produces prediction object
        prediction_producer = KafkaProducer(bootstrap_servers=["localhost:9092"],
                                            key_serializer=lambda key: str(key).encode(),
                                            value_serializer=lambda value: json.dumps(value).encode())

        # Consume known face object to know what faces are the target
        query_faces_consumer = KafkaConsumer(self.query_faces_topic, group_id=self.iam, client_id=self.iam,
                                             bootstrap_servers=["0.0.0.0:9092"],
                                             value_deserializer=lambda value: json.loads(value.decode()))

        print("[PredictFrames {}] WAITING FOR TRACKING INFO..".format(socket.gethostname()))
        query_faces_message = next(query_faces_consumer)
        print("[PredictFrames {}] GOT TRACKING INFO..".format(socket.gethostname()))
        log_file_name = '{}/{}/consumer_{}_workers_{}_cameras_{}.csv'.format(MAIN_PATH, LOG_DIR,
                                                                             self.name,
                                                                             SET_PARTITIONS,
                                                                             TOTAL_CAMERAS)
        log_file = None
        try:
            # log just one process, gives a good estimate of the total lag
            print("Writing header to {}".format(log_file_name))
            log_file = open(log_file_name, 'w')
            log_file.write('camera,frame,prediction,consumers,latency\n')
            while True:

                if self.verbose:
                    print("[PredictFrames {}] WAITING FOR NEXT FRAMES..".format(socket.gethostname()))

                raw_frame_messages = frame_consumer.poll(timeout_ms=10, max_records=10)

                for topic_partition, msgs in raw_frame_messages.items():
                    # Get the predicted Object, JSON with frame and meta info about the frame
                    for msg in msgs:
                        result = self.get_face_object(msg.value, query_faces_message.value, self.scale)

                        tp = TopicPartition(msg.topic, msg.partition)
                        offsets = {tp: OffsetAndMetadata(msg.offset, None)}
                        frame_consumer.commit(offsets=offsets)

                        print("frame_num: {},camera_num: {}, latency: {}, y_hat: {}".format(
                            result["frame_num"],
                            result["camera"],
                            result["latency"],
                            result["prediction"]
                        ))
                        log_file.write("{},{},{},{},{},{}".format(result["camera"],
                                                                  result["frame_num"],
                                                                  result["prediction"],
                                                                  SET_PARTITIONS,
                                                                  result["latency"],
                                                                  "\n"))

                        # camera specific topic
                        prediction_topic = "{}_{}".format(PREDICTION_TOPIC_PREFIX, result["camera"])

                        prediction_producer.send(prediction_topic, key=result["frame_num"], value=result)

                    prediction_producer.flush()

        except KeyboardInterrupt as e:
            print("Closing Stream")
            frame_consumer.close()
            if str(self.name) == "1":
                log_file.close()

        finally:
            print("Closing Stream")
            frame_consumer.close()
            log_file.close()

    @staticmethod
    def get_face_object(frame_obj, query_faces_data, scale=1.0):
        """Match query faces with detected faces in the frame, if matched put box and a tag.
        :param frame_obj: frame dictionary with frame information, frame, face encodings, locations.
        :param query_faces_data: message from query face topic, contains encoding and names of faces.
        Other way was to broadcast raw image and calculate encodings here.
        :param scale: to scale up as 1/scale, if in pre processing frames were scaled down for speedup.
        :return: A dict with modified frame, i.e. bounded box drawn around detected persons face.
        """

        # get frame from message
        frame = np_from_json(frame_obj, prefix_name=ORIGINAL_PREFIX)  # frame_obj = json
        # get processed info from message
        face_locations = np_from_json(frame_obj, prefix_name="face_locations")
        face_encodings = np_from_json(frame_obj, prefix_name="face_encodings")
        # Convert the image from BGR color (which OpenCV uses) to RGB color (which face_recognition uses)
        frame = cv2.cvtColor(frame.astype(np.uint8), cv2.COLOR_BGR2RGB)

        # get info entered by user through UI
        known_face_encodings = np_from_json(query_faces_data,
                                            prefix_name="known_face_encodings").tolist()  # (n, 128)
        known_faces = np_from_json(query_faces_data, prefix_name="known_faces").tolist()  # (n, )

        with timer("\nFACE RECOGNITION {}\n".format(frame_obj["frame_num"])):

            # Faces found in this image
            face_names = []
            with timer("Total Match time"):
                for i, face_encoding in enumerate(face_encodings):
                    # See if the face is a match for the known face(s)
                    with timer("Match {}th face time".format(i)):
                        matches = face_recognition.compare_faces(known_face_encodings, face_encoding)

                    name = "Unknown"
                    # If a match was found in known_face_encodings, just use the first one.
                    if True in matches:
                        first_match_index = matches.index(True)
                        name = known_faces[first_match_index]

                    face_names.append(name.title())

            # Mark the results for this frame
            for (top, right, bottom, left), name in zip(face_locations, face_names):
                # Scale back up face locations since the frame we detected in was scaled to 1/4 size

                if scale != 1:
                    top *= int(1 / scale)
                    right *= int(1 / scale)
                    bottom *= int(1 / scale)
                    left *= int(1 / scale)

                if name == "Unknown":
                    color = (0, 0, 255)  # blue
                else:
                    color = (255, 0, 0)  # red

                # Draw a box around the face
                cv2.rectangle(frame, (left, top), (right, bottom), color, 2)

                # Draw a label with a name below the face
                cv2.rectangle(frame, (left, bottom - 21), (right, bottom), color, cv2.FILLED)
                cv2.putText(frame, name, (left + 6, bottom - 6), cv2.FONT_HERSHEY_SIMPLEX, 0.6, (255, 255, 255), 1)

        frame = cv2.cvtColor(frame.astype(np.uint8), cv2.COLOR_BGR2RGB)
        frame_dict = np_to_json(frame, prefix_name=PREDICTED_PREFIX)
        prediction = None
        if face_names:
            prediction = face_names[0]

        result = {"prediction": prediction,
                  "predict_time": str(time.time()),
                  "latency": str(time.time() - int(frame_obj["timestamp"]))}

        frame_obj.update(frame_dict)  # update frame with prediction
        result.update(frame_obj)  # add prediction results

        return result


@contextmanager
def timer(name):
    """Util function: Logs the time."""
    t0 = time.time()
    yield
    print("[{}] done in {:.3f} s".format(name, time.time() - t0))
