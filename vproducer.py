import json
import os
import time

import cv2
import numpy as np

from kafka import KafkaProducer
from utils import np_to_json

CAMERA_NUM = 0
FPS = 10
GRAY = True

#  connect to Kafka
producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda hashmap: json.dumps(hashmap).encode())

# Assign a topic
topic = 'frame_objs'

# serving from s3 bucket via cloudFront: url to the object
cfront_endpoint = "http://d3tj01z94i74qz.cloudfront.net/"
cfront_url = cfront_endpoint + "cam{}/videos/cam{}_{}_fps.mp4".format(CAMERA_NUM, CAMERA_NUM, FPS)


def transform(frame, frame_num, camera=0, gray=False):
    """Serialize frame, create json message with serialized frame, camera number and timestamp."""
    
    if gray:
        frame = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)  # (28, 28)

    # serialize numpy array --> model
    # {'frame': string(base64encodedarray), 'dtype': obj.dtype.str, 'shape': obj.shape}
    frame_dict = np_to_json(frame.astype(np.uint8))
    
    # Convert the image to bytes, create json message and send to kafka
    message = {"timestamp": time.time(), "camera": camera, "frame_num": frame_num}
    
    # add frame and metadata related to frame
    message.update(frame_dict)
    return message


def video_emitter(video):
    """Reads frame by frame, attaches meta data, publishes"""
    # Open the video
    print('Monitoring Stream from: ', video)
    video = cv2.VideoCapture(video)
    print('Publishing.....')

    # monitor frame number
    i = 0

    # read the file
    while (video.isOpened):

        # read the image in each frame
        success, image = video.read()

        # check if the file has read to the end
        if not success:
            print("BREAK AT FRAME: {}".format(i))
            break

        message = transform(image, i, camera=CAMERA_NUM, gray=GRAY)
        producer.send(topic, message)

        if i == 1:
            print(message.keys())

        # To reduce CPU usage create sleep time of 0.01 sec
        time.sleep(0.01)
        i += 1

    # clear the capture
    video.release()
    print('Done Publishing...')


if __name__ == '__main__':
    video_emitter(cfront_url)
