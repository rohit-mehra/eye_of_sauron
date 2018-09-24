import json
import time
import re
import cv2
import os
import imutils
from multiprocessing import Pool
from kafka import KafkaProducer
from utils import np_to_json, cleanup_topics, init_frame_topic
from imutils.video import VideoStream
from params import *


def get_video_feed_url(camera_num=0, fps=10):
    """Get CAMERA IP from where video is being streamed.
    Args:
        camera_num: camera number
        fps: fps os stream
    Returns:
        A URL to the stream.
    """
    # serving from s3 bucket via cloudFront: url to the object
    return C_FRONT_ENDPOINT + "cam{}/videos/cam{}_{}_fps.mp4".format(camera_num, camera_num, fps)


def transform(frame, frame_num, camera=0, gray=False, caffee=False):
    """Serialize frame, create json message with serialized frame, camera number and timestamp.
    Args:
        frame: numpy.ndarray, raw frame
        frame_num: frame number in the particular video/camera
        camera: Camera Number the frame is from
        gray: convert frame to grayscale?
        caffee: resize frame to fit caffee model
    Returns:
        A dict {'frame': string(base64encodedarray), 'dtype': obj.dtype.str, 'shape': obj.shape,
                "timestamp": time.time(), "camera": camera, "frame_num": frame_num}
    """

    if gray:
        frame = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)  # (28, 28)
    if caffee:
        frame = imutils.resize(frame, width=400)
    # serialize frame
    frame_dict = np_to_json(frame.astype(np.uint8))

    # Metadata for frame
    message = {"timestamp": time.time(), "camera": camera, "frame_num": frame_num}

    # add frame and metadata related to frame
    message.update(frame_dict)

    return message


def video_emitter(video_url, use_cv2=True):
    """Publish video frames as json objects, timestamped, marked with camera number.
    Args:
        video_url: URL for streaming video
        use_cv2: use raw cv2 streaming, set to false to use smart fast streaming --> not every frame is sent.
    Returns:
        A dict {'frame': string(base64encodedarray), 'dtype': obj.dtype.str, 'shape': obj.shape,
                "timestamp": time.time(), "camera": camera, "frame_num": frame_num}
    """
    # Connect to Kafka, new producer for each thread, each thread corresponds to a camera feed.
    frame_producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                                   key_serializer=lambda key: str(key).encode(),
                                   value_serializer=lambda value: json.dumps(value).encode(),
                                   )
    # Print producer information
    print(frame_producer.partitions_for(FRAME_TOPIC))
    # Open the video
    print('Monitoring Stream from: ', video_url)
    # Use either option
    video = cv2.VideoCapture(video_url) if use_cv2 else VideoStream(video_url).start()
    print('Publishing.....')

    # monitor frame number
    frame_num = 0

    # Read URL, Transform, Publish
    while True:

        # using raw cv2, frame by frame
        if use_cv2:
            success, image = video.read()
            # check if the file has read
            if not success:
                print("BREAK AT FRAME: {}".format(frame_num))
                break

        # using smart, only unique frames, skips frames
        else:
            image = video.read()
            # check if the file has read
            if image is None:
                print("BREAK AT FRAME: {}".format(frame_num))
                break

        # Attach metadata to frame, transform into JSON
        message = transform(frame=image,
                            frame_num=frame_num,
                            camera=re.findall(r'cam([0-9][0-9]*?)/', video_url)[0],
                            gray=GRAY,
                            caffee=CAFFEE)

        # Partition to be sent to
        part = i % SET_PARTITIONS
        # Logging
        print("\rCam{}_{}_partition_{} ".format(message['camera'], frame_num, part), end='')
        # Publish to specific partition
        frame_producer.send(FRAME_TOPIC, key=frame_num, value=message, partition=part)
        # To reduce CPU usage create sleep time of 0.01 sec
        time.sleep(0.01)
        frame_num += 1

    # clear the capture
    if use_cv2:
        video.release()
    else:
        video.stop()

    print('Done Publishing...')
    return True


if __name__ == '__main__':
    # DELETE FRAME TOPIC, TO AVOID USING PREVIOUS JUNK DATA
    cleanup_topics()
    # INIT TOPIC WITH DESIRED PARTITIONS
    init_frame_topic()
    # GET IPs OF CAMERAS
    CAMERA_URLS = [get_video_feed_url(i, FPS) for i in range(TOTAL_CAMERAS)]
    # CREATE POOL
    producer_pool = Pool(len(CAMERA_URLS))
    try:
        statuses = producer_pool.map(video_emitter, CAMERA_URLS)
        producer_pool.close()  # close pool
        producer_pool.join()  # wait to join
    except KeyboardInterrupt as e:
        print(e)
        producer_pool.terminate()
        os.system("/usr/local/kafka/bin/kafka-topics.sh  --zookeeper localhost:2181 --delete --topic {}".format(FRAME_TOPIC))
        print("Done....")

