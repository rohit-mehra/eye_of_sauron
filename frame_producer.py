import json
import re
import sys
import time
from multiprocessing import Process

import cv2
import imutils
import numpy as np
from imutils.video import VideoStream
from kafka import KafkaProducer, TopicPartition
from kafka.partitioner import RoundRobinPartitioner, Murmur2Partitioner

from utils import np_to_json


class StreamVideo(Process):
    """Video Streaming Producer Process Class."""

    def __init__(self, video_path,
                 topic,
                 topic_partitions=8,
                 use_cv2=False,
                 mnist=False,
                 pub_obj_key="original",
                 group=None,
                 target=None,
                 name=None,
                 verbose=False,
                 rr_distribute=False):

        super().__init__(group=group, target=target, name=name)

        # URL for streaming video
        self.video_path = video_path
        # TOPIC TO PUBLISH
        self.frame_topic = topic
        self.mnist = mnist
        self.topic_partitions = topic_partitions
        self.camera_num = int(re.findall(r"StreamVideo-([0-9]*)", self.name)[0])
        self.use_cv2 = use_cv2
        self.object_key = pub_obj_key
        self.verbose = verbose
        self.rr_distribute = rr_distribute

    def run(self):
        """Publish video frames as json objects, timestamped, marked with camera number.
        Source:
            self.video_path: URL for streaming video
            self.kwargs["use_cv2"]: use raw cv2 streaming, set to false to use smart fast streaming --> not every frame is sent.
        Publishes:
            A dict {"frame": string(base64encodedarray), "dtype": obj.dtype.str, "shape": obj.shape,
                    "timestamp": time.time(), "camera": camera, "frame_num": frame_num}
        """

        if self.rr_distribute:
            partitioner = RoundRobinPartitioner(partitions=
                                                [TopicPartition(topic=self.frame_topic, partition=i)
                                                 for i in range(self.topic_partitions)])

        else:

            partitioner = Murmur2Partitioner(partitions=
                                             [TopicPartition(topic=self.frame_topic, partition=i)
                                              for i in range(self.topic_partitions)])

        frame_producer = KafkaProducer(bootstrap_servers=["localhost:9092"],
                                       key_serializer=lambda key: str(key).encode(),
                                       value_serializer=lambda value: json.dumps(value).encode(),
                                       partitioner=partitioner)

        print("[CAM {}] URL: {}, SET PARTITIONS FOR FRAME TOPIC: {}".format(self.camera_num,
                                                                            self.video_path,
                                                                            frame_producer.partitions_for(
                                                                                self.frame_topic)))
        # Use either option
        video = cv2.VideoCapture(self.video_path) if self.use_cv2 else VideoStream(self.video_path).start()

        # Track frame number
        frame_num = 0
        start_time = time.time()
        print("[CAM {}] START TIME {}: ".format(self.camera_num, start_time))

        # Read URL, Transform, Publish
        while True:

            # using raw cv2, frame by frame
            if self.use_cv2:
                success, image = video.read()
                # check if the file has read
                if not success:
                    if self.verbose:
                        print("[CAM {}] URL: {}, END FRAME: {}".format(self.name,
                                                                       self.video_path,
                                                                       frame_num))
                    break

            # using smart, only unique frames, skips frames, faster fps
            else:
                image = video.read()
                # check if the file has read
                if image is None:
                    if self.verbose:
                        print("[CAM {}] URL: {}, END FRAME: {}".format(self.name,
                                                                       self.video_path,
                                                                       frame_num))
                    break

            # Attach metadata to frame, transform into JSON
            message = self.transform(frame=image,
                                     frame_num=frame_num,
                                     mnist=self.mnist,
                                     object_key=self.object_key,
                                     camera=self.camera_num,
                                     verbose=self.verbose)

            # Partition to be sent to
            part = frame_num % self.topic_partitions
            # Logging
            if self.verbose:
                print("\r[PRODUCER][Cam {}] FRAME: {} TO PARTITION: {}".format(message["camera"],
                                                                               frame_num, part), end="")
            # Publish to specific partition
            frame_producer.send(self.frame_topic, key="{}_{}".format(self.camera_num, frame_num), value=message)

            # if frame_num % 1000 == 0:
            frame_producer.flush()

            # To reduce CPU usage create sleep time of 0.1 sec
            if self.mnist:
                time.sleep(0.1)

            frame_num += 1

        # clear the capture
        if self.use_cv2:
            video.release()
        else:
            video.stop()

        if self.verbose:
            print("[CAM {}] FINISHED. STREAM TIME {}: ".format(self.camera_num, time.time() - start_time))

        return True if frame_num > 0 else False

    @staticmethod
    def transform(frame, frame_num, mnist=False, object_key="original", camera=0, verbose=False):
        """Serialize frame, create json message with serialized frame, camera number and timestamp.
        Args:
            frame: numpy.ndarray, raw frame
            frame_num: frame number in the particular video/camera
            mnist: if video feed is mnist (used for prototyping)
            camera: Camera Number the frame is from
            object_key: identifier for these objects
            verbose: print out logs
        Returns:
            A dict {"frame": string(base64encodedarray), "dtype": obj.dtype.str, "shape": obj.shape,
                    "timestamp": time.time(), "camera": camera, "frame_num": frame_num}
        """

        if mnist:
            frame = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)  # (28, 28)
        else:
            # to a standard size for UI
            frame = imutils.resize(frame, width=400)

        if verbose:
            # print raw frame size
            print("\nRAW ARRAY SIZE: ", sys.getsizeof(frame))

        # serialize frame
        frame_dict = np_to_json(frame.astype(np.uint8), prefix_name=object_key)
        # Metadata for frame
        message = {"timestamp": time.time(), "camera": camera, "frame_num": frame_num}
        # add frame and metadata related to frame
        message.update(frame_dict)

        if verbose:
            # print message size
            print("\nMESSAGE SIZE: ", sys.getsizeof(message))

        return message
