import json
import socket
import time
from configparser import ConfigParser
from contextlib import contextmanager
from multiprocessing import Process

import cv2
# import face_recognition
import numpy as np
from confluent_kafka import Consumer
from kafka import KafkaConsumer, KafkaProducer
from kafka.coordinator.assignors.range import RangePartitionAssignor
from kafka.coordinator.assignors.roundrobin import RoundRobinPartitionAssignor
# from kafka.partitioner import RoundRobinPartitioner, Murmur2Partitioner
from kafka.structs import OffsetAndMetadata, TopicPartition

from src.params import *
from src.utils import np_from_json, np_to_json
from demo_yolo import *


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
        # Producer object, set desired partitioner
        config_parser = ConfigParser()
        # config_parser.read
        config_parser.read("./confluent_config.ini", encoding='utf-8-sig')
        config = dict(config_parser['default'])
        bootstrap_servers = config['bootstrap.servers']
        security_protocol = config['security.protocol']
        sasl_mechanisms = config['sasl.mechanisms']
        sasl_username = config['sasl.username']
        sasl_password = config['sasl.password']
        group_id = config['group.id']

        frame_consumer = Consumer(config)
        processed_frame_producer = KafkaProducer(bootstrap_servers=[bootstrap_servers],
                                                 key_serializer=lambda key: str(key).encode(),
                                                 value_serializer=lambda value: json.dumps(value).encode(),
                                                 security_protocol=security_protocol,
                                                 sasl_mechanism=sasl_mechanisms,
                                                 sasl_plain_username=sasl_username,
                                                 sasl_plain_password=sasl_password)

        load_model('./best_20_epochs_DataB.pt')

        # Set up a callback to handle the '--reset' flag.
        def reset_offset(consumer, partitions):
            consumer.assign(partitions)

        # Subscribe to topic
        topic = "raw_frame_topic"
        frame_consumer.subscribe([topic], on_assign=reset_offset)
        # Poll for new messages from Kafka and print them.
        try:
            while True:
                msg = frame_consumer.poll(1.0)
                if msg is None:
                    # Initial message consumption may take up to
                    # `session.timeout.ms` for the consumer group to
                    # rebalance and start consuming
                    print("Waiting...")
                elif msg.error():
                    print("ERROR: %s".format(msg.error()))
                else:
                    # Get pre processing result
                    decoded_value = json.loads(msg.value().decode())
                    result = self.get_processed_frame_object(decoded_value, self.scale)

                    # Partition to be sent to
                    prediction_topic = "processed_frame_topic"

                    key = "{}_{}".format(self.camera_num, result['frame_num'])
                    processed_frame_producer.send(prediction_topic, key=key, value=result)
                    processed_frame_producer.flush()
        except KeyboardInterrupt:
            pass
        finally:
            # Leave group and commit final offsets
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
        # frame = cv2.cvtColor(frame.astype(np.uint8), cv2.COLOR_BGR2RGB)

        # TODO:
        # # Count people
        # prediction = 21

        # # Draw count
        frame, bboxes = predict(frame, view_img=True)

        frame_dict = np_to_json(frame, prefix_name=PREDICTED_PREFIX)

        # Add additional information
        num_people = len(bboxes)
        result = {"num_people": num_people,
                  "bboxes": bboxes,
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
