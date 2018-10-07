import json
import socket
import time
from contextlib import contextmanager
from multiprocessing import Process

import cv2
import face_recognition
import numpy as np
from kafka import KafkaConsumer, KafkaProducer

from params import *
from utils import np_from_json, np_to_json


class ConsumeFrames(Process):
    """Consuming frame objects to, produce predictions."""

    def __init__(self,
                 frame_topic,
                 query_faces_topic,
                 group=None,
                 target=None,
                 name=None,
                 scale=1.0,
                 verbose=False):
        super().__init__(group=group, target=target, name=name)

        self.iam = "{}-{}".format(socket.gethostname(), self.name)
        self.frame_topic = frame_topic
        self.query_faces_topic = query_faces_topic
        self.verbose = verbose
        self.scale = scale
        self.frames_detection_collection = dict()
        print("[INFO] I am ", self.iam)

    def run(self):
        """CONSUME video frames, predictions Published to respective camera topics"""
        # Connect to kafka, Consume frame obj bytes deserialize to json
        frame_consumer = KafkaConsumer(self.frame_topic, group_id='predict', client_id=self.iam,
                                       bootstrap_servers=['0.0.0.0:9092'],
                                       key_deserializer=lambda key: key.decode(),
                                       value_deserializer=lambda value: json.loads(value.decode()))

        #  Produces prediction object
        prediction_producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                                            key_serializer=lambda key: str(key).encode(),
                                            value_serializer=lambda value: json.dumps(value).encode())

        # Consume known face object to know what faces are the target
        query_faces_consumer = KafkaConsumer(self.query_faces_topic, group_id=self.iam, client_id=self.iam,
                                             bootstrap_servers=['0.0.0.0:9092'],
                                             value_deserializer=lambda value: json.loads(value.decode()))

        print("[CONSUMER {}] WAITING FOR TRACKING INFO..".format(socket.gethostname()))
        query_faces_message = next(query_faces_consumer)
        print("[CONSUMER {}] GOT TRACKING INFO..".format(socket.gethostname()))

        try:
            while True:

                if self.verbose:
                    print("[CONSUMER {}] WAITING FOR NEXT FRAMES..".format(socket.gethostname()))

                raw_frame_messages = frame_consumer.poll(timeout_ms=5, max_records=30)

                for topic_partition, msgs in raw_frame_messages.items():
                    # Get the predicted Object, JSON with frame and meta info about the frame
                    for msg in msgs:
                        result = self.get_face_object(msg.value, query_faces_message.value, self.scale)

                        print("timestamp: {}, frame_num: {},camera_num: {}, latency: {}, y_hat: {}".format(
                            result['timestamp'],
                            result['frame_num'],
                            result['camera'],
                            result['latency'],
                            result['prediction']
                        ))

                        # camera specific topic
                        prediction_topic = "{}_{}".format(PREDICTION_TOPIC_PREFIX, result['camera'])

                        prediction_producer.send(prediction_topic, key=result['frame_num'], value=result)

        except KeyboardInterrupt as e:
            print(e)
            pass

        finally:
            print("Closing Stream")
            frame_consumer.close()

    @staticmethod
    def get_face_object(frame_obj, query_faces_data, scale=1.0):
        """Processes value produced by producer, returns prediction with png image.
        Args:
            frame_obj: frame dictionary with frame information and frame itself
            query_faces_data: a specialized dictionary with info about query face encodings and names
            scale: (0, 1] scale image before face recognition, speeds up processing, decreases accuracy
        Returns:
            A dict with modified frame, i.e. bounded box drawn around detected persons face.
        """

        with timer("*Pseudo Work*"):
            frame = np_from_json(frame_obj, prefix_name=ORIGINAL_PREFIX)  # frame_obj = json
            # Convert the image from BGR color (which OpenCV uses) to RGB color (which face_recognition uses)
            frame = cv2.cvtColor(frame.astype(np.uint8), cv2.COLOR_BGR2RGB)

            if scale != 1:
                # Resize frame of video to scale size for faster face recognition processing
                rgb_small_frame = cv2.resize(frame, (0, 0), fx=scale, fy=scale)

            else:
                rgb_small_frame = frame

            known_face_encodings = np_from_json(query_faces_data,
                                                prefix_name="known_face_encodings").tolist()  # (n, 128)
            known_faces = np_from_json(query_faces_data, prefix_name="known_faces").tolist()  # (n, )

        with timer("*FACE RECOGNITION*"):
            # Find all the faces and face encodings in the current frame of video
            with timer("Locations in frame"):
                face_locations = face_recognition.face_locations(rgb_small_frame)

            with timer("Encodings in frame"):
                face_encodings = face_recognition.face_encodings(rgb_small_frame, face_locations)

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

                # Draw a box around the face
                cv2.rectangle(frame, (left, top), (right, bottom), (0, 0, 255), 2)

                # Draw a label with a name below the face
                cv2.rectangle(frame, (left, bottom - 27), (right, bottom), (0, 0, 255), cv2.FILLED)
                cv2.putText(frame, name, (left + 6, bottom - 6), cv2.FONT_HERSHEY_SIMPLEX, 1.0, (255, 255, 255), 1)

        frame = cv2.cvtColor(frame.astype(np.uint8), cv2.COLOR_BGR2RGB)
        frame_dict = np_to_json(frame, prefix_name=PREDICTED_PREFIX)
        prediction = None
        if face_names:
            prediction = face_names[0]

        result = {"prediction": prediction,
                  "predict_time": str(time.time()),
                  "latency": str(time.time() - int(frame_obj['timestamp']))}

        frame_obj.update(frame_dict)  # update frame with prediction
        result.update(frame_obj)  # add prediction results

        return result


@contextmanager
def timer(name):
    """Util function: Logs the time."""
    t0 = time.time()
    yield
    print('[{}] done in {:.3f} s'.format(name, time.time() - t0))


if __name__ == '__main__':

    HM_PROCESSESS = 2
    CONSUMERS = [ConsumeFrames(frame_topic=FRAME_TOPIC,
                               query_faces_topic=KNOWN_FACE_TOPIC,
                               scale=1) for _ in
                 range(HM_PROCESSESS)]

    for c in CONSUMERS:
        c.start()

    for c in CONSUMERS:
        c.join()
