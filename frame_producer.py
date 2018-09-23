import json
import time
import re
import cv2
import numpy as np
from multiprocessing import Pool
from kafka import KafkaProducer
from utils import np_to_json

TOTAL_CAMERAS = 3
FPS = 2

GRAY = True
CAFFEE = False
C_FRONT_ENDPOINT = "http://d3tj01z94i74qz.cloudfront.net/"


# TOPIC USED TO PUBLISH ALL FRAMES TO
FRAME_TOPIC = 'frame_objects'


def get_video_feed_url(camera_num=0, fps=10):
    """Gets the camera IP, where video is being streamed"""
    # serving from s3 bucket via cloudFront: url to the object
    return C_FRONT_ENDPOINT + "cam{}/videos/cam{}_{}_fps.mp4".format(camera_num, camera_num, fps)


def transform(frame, frame_num, camera=0, gray=False, caffee=False):
    """Serialize frame, create json message with serialized frame, camera number and timestamp."""
    
    if gray:
        frame = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)  # (28, 28)

    if caffee:
        frame = cv2.resize(frame, (300, 300))

    print(frame.shape)

    # serialize numpy array --> model
    # {'frame': string(base64encodedarray), 'dtype': obj.dtype.str, 'shape': obj.shape}
    frame_dict = np_to_json(frame.astype(np.uint8))
    
    # Convert the image to bytes, create json message and send to kafka
    message = {"timestamp": time.time(), "camera": camera, "frame_num": frame_num}
    
    # add frame and metadata related to frame
    message.update(frame_dict)
    return message


def video_emitter(video_url):
    # Connect to Kafka, new producer for each thread!!!! important
    FRAME_PRODUCER = KafkaProducer(bootstrap_servers=['localhost:9092'],
                                   value_serializer=lambda hash_map: json.dumps(hash_map).encode())

    """Reads frame by frame, attaches meta data, publishes"""
    # Open the video
    print('Monitoring Stream from: ', video_url)
    video = cv2.VideoCapture(video_url)
    print('Publishing.....')

    # monitor frame number
    i = 0

    # read the file
    while video.isOpened:

        # read the image in each frame
        success, image = video.read()

        # check if the file has read to the end
        if not success:
            print("BREAK AT FRAME: {}".format(i))
            break

        message = transform(frame=image,
                            frame_num=i,
                            camera=re.findall(r'cam([0-9][0-9]*?)/', video_url)[0],
                            gray=GRAY,
                            caffee=CAFFEE)

        print("\rCam{}_{}".format(message['camera'], i), end='')

        FRAME_PRODUCER.send(FRAME_TOPIC, value=message)

        if i == 1:
            print(message.keys())

        # To reduce CPU usage create sleep time of 0.01 sec
        time.sleep(0.01)
        i += 1

    # clear the capture
    video.release()
    print('Done Publishing...')
    return True


if __name__ == '__main__':
    CAMERA_URLS = [get_video_feed_url(i, FPS) for i in range(TOTAL_CAMERAS)]
    # video_emitter(CAMERA_URLS[0])
    # TODO: THREADING CAUSING ISSUE, CONSUMER NOT ABLE TO READ ANYTHING!!
    producer_pool = Pool(len(CAMERA_URLS))
    try:
        statuses = producer_pool.map(video_emitter, CAMERA_URLS)
        producer_pool.close()  # close pool
        producer_pool.join()  # wait to join
    except KeyboardInterrupt as e:
        print(e)
        producer_pool.terminate()

