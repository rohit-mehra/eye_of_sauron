import json
import time
import re
import cv2
import imutils
from kafka import KafkaProducer
from utils import np_to_json, cleanup_topics, init_frame_topic, get_video_feed_url
from imutils.video import VideoStream
from params import *

from multiprocessing import Process


class StreamVideo(Process):
    def __init__(self, video_path,
                 topic,
                 topic_partitions=4,
                 use_cv2=False,
                 pub_obj_key='original',
                 group=None,
                 target=None,
                 name=None):

        super().__init__(group=group, target=target, name=name)

        # URL for streaming video
        self.video_path = video_path
        # TOPIC TO PUBLISH
        self.frame_topic = topic
        self.topic_partitions = topic_partitions
        # self.frame_producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
        #                                     key_serializer=lambda key: str(key).encode(),
        #                                     value_serializer=lambda value: json.dumps(value).encode())

        self.camera_num = int(re.findall(r'StreamVideo-([0-9]*)', self.name)[0])
        self.use_cv2 = use_cv2
        self.object_key = pub_obj_key

    def run(self):
        """Publish video frames as json objects, timestamped, marked with camera number.
        Source:
            self.video_path: URL for streaming video
            self.kwargs['use_cv2']: use raw cv2 streaming, set to false to use smart fast streaming --> not every frame is sent.
        Publishes:
            A dict {'frame': string(base64encodedarray), 'dtype': obj.dtype.str, 'shape': obj.shape,
                    "timestamp": time.time(), "camera": camera, "frame_num": frame_num}
        """

        frame_producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                                       key_serializer=lambda key: str(key).encode(),
                                       value_serializer=lambda value: json.dumps(value).encode())

        print('[CAM {}] URL: {}, TOPIC PARTITIONS: {}'.format(self.camera_num,
                                                              self.video_path,
                                                              frame_producer.partitions_for(
                                                                  self.frame_topic)))
        # Use either option
        video = cv2.VideoCapture(self.video_path) if self.use_cv2 else VideoStream(self.video_path).start()

        # Track frame number
        frame_num = 0
        start_time = time.time()
        print('[CAM {}] START TIME {}: '.format(self.camera_num, start_time))

        # Read URL, Transform, Publish
        while True:

            # using raw cv2, frame by frame
            if self.use_cv2:
                success, image = video.read()
                # check if the file has read
                if not success:
                    print('[CAM {}] URL: {}, END FRAME: {}'.format(self.name,
                                                                   self.video_path,
                                                                   frame_num))
                    break

            # using smart, only unique frames, skips frames, faster fps
            else:
                image = video.read()
                # check if the file has read
                if image is None:
                    print('[CAM {}] URL: {}, END FRAME: {}'.format(self.name,
                                                                   self.video_path,
                                                                   frame_num))
                    break

            # Attach metadata to frame, transform into JSON
            message = self.transform(frame=image,
                                     frame_num=frame_num,
                                     object_key=self.object_key,
                                     camera=self.camera_num)

            # Partition to be sent to
            part = frame_num % self.topic_partitions
            # Logging
            print("\r[PRODUCER][Cam {}] FRAME: {} TO PARTITION: {}".format(message['camera'],
                                                                           frame_num, part), end='')
            # Publish to specific partition
            frame_producer.send(self.frame_topic, key=frame_num, value=message, partition=part)
            # To reduce CPU usage create sleep time of 0.01 sec
            # time.sleep(0.1)
            frame_num += 1

        # clear the capture
        if self.use_cv2:
            video.release()
        else:
            video.stop()

        print('[CAM {}] FINISHED. STREAM TIME {}: '.format(self.camera_num, time.time() - start_time))

        return True if frame_num > 0 else False

    @staticmethod
    def transform(frame, frame_num, object_key='original', camera=0):
        """Serialize frame, create json message with serialized frame, camera number and timestamp.
        Args:
            frame: numpy.ndarray, raw frame
            frame_num: frame number in the particular video/camera
            camera: Camera Number the frame is from
            object_key: identifier for these objects
        Returns:
            A dict {'frame': string(base64encodedarray), 'dtype': obj.dtype.str, 'shape': obj.shape,
                    "timestamp": time.time(), "camera": camera, "frame_num": frame_num}
        """

        # to a standard size for UI
        frame = imutils.resize(frame, width=400)
        # serialize frame
        frame_dict = np_to_json(frame.astype(np.uint8), prefix_name=object_key)
        # Metadata for frame
        message = {"timestamp": time.time(), "camera": camera, "frame_num": frame_num}
        # add frame and metadata related to frame
        message.update(frame_dict)

        return message


if __name__ == '__main__':
    # DELETE FRAME TOPIC, TO AVOID USING PREVIOUS JUNK DATA
    cleanup_topics()
    # INIT TOPIC WITH DESIRED PARTITIONS
    init_frame_topic()
    # GET IPs OF CAMERAS
    CAMERA_URLS = [get_video_feed_url(i, FPS) for i in range(3, 6)]
    PRODUCERS = [StreamVideo(url, FRAME_TOPIC, SET_PARTITIONS, pub_obj_key=ORIGINAL_PREFIX) for url in CAMERA_URLS]

    for p in PRODUCERS:
        p.start()

    for p in PRODUCERS:
        p.join()

    print("Done....")
