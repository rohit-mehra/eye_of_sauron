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
        # self.processed_frame_topic = processed_frame_topic
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
                        prediction_topic = "{}_{}".format(PREDICTION_TOPIC_PREFIX, result["camera"])
                        processed_frame_producer.send(prediction_topic, key=result["frame_num"], value=result)

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

        # Yolo models
        face_locations = face_recognition.face_locations(frame)

        # Draw a box around the face
        color = (0, 0, 255)  # blue

        for (top, right, bottom, left) in face_locations:
            cv2.rectangle(frame, (left, top), (right, bottom), color, 2)

        # TODO:
        # # Count people
        prediction = 21
        #
        # # Draw count
        # y, x, _ = frame.shape
        # color_blue = (0, 0, 255)  # blue
        # color_white = (255, 255, 255)  # blue
        # frame = cv2.rectangle(frame, (x - 50, y - 30), (x - 1, y - 1), color_white, -1)
        # frame = cv2.putText(frame, str(prediction), (x - 15, y - 5), cv2.FONT_HERSHEY_SIMPLEX, 0.6, color_blue, 1)

        frame = cv2.cvtColor(frame.astype(np.uint8), cv2.COLOR_BGR2RGB)
        frame_dict = np_to_json(frame, prefix_name=PREDICTED_PREFIX)

        # Add additional information
        result = {"prediction": prediction,
                  "predict_time": str(time.time()),
                  "latency": str(time.time() - int(frame_obj["timestamp"]))}

        frame_obj.update(frame_dict)  # update frame with prediction
        result.update(frame_obj)  # add prediction results

        return frame_obj


@contextmanager
def timer(name):
    """Util function: Logs the time."""
    t0 = time.time()
    yield
    print("[{}] done in {:.3f} s".format(name, time.time() - t0))
