import numpy as np
import time
import cv2
from kafka import KafkaProducer
import os
import json
from utils import np_to_json

CAMERA_NUM = 0
FPS = 5
GRAY = True

#  connect to Kafka
producer = KafkaProducer(bootstrap_servers=['localhost:9092'], value_serializer=lambda hashmap: json.dumps(hashmap))

# Assign a topic
topic = 'frames'

# serving from s3 bucket via cloudFront: url to the object
cfront_endpoint = "http://d3tj01z94i74qz.cloudfront.net/"
cfront_url = cfront_endpoint + "cam{}/videos/cam{}_{}_fps.mp4".format(CAMERA_NUM, CAMERA_NUM, FPS)

# print(os.listdir("/home/ubuntu/eye_of_sauron/data/cam1/videos/"))


def video_emitter(video):
    
    # Open the video
    print('Monitoring Stream from: ', video)
    video = cv2.VideoCapture(video)
    print('Emitting.....')
    
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
        
        if GRAY:
            image = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY) # (28, 28)
        
        # serialize numpy array --> model
        frame_dict = np_to_json(image.astype(np.uint8)) # {'frame': base64.b64encode(obj.tostring()).decode("utf-8"),
                                                        #  'dtype': obj.dtype.str,
                                                        #  'shape': obj.shape}
        
        # Convert the image to bytes, create json message and send to kafka
        message = {"timestamp":time.time(), "camera":CAMERA_NUM}
        
        message.update(frame_dict)
        
        producer.send(topic, message)
        
        if i == 1:
            print(message.keys())
        
        # To reduce CPU usage create sleep time of 0.1sec  
        time.sleep(0.1)
        i += 1

    # clear the capture
    video.release()
    print('Done Emitting...')


if __name__ == '__main__':

    # video_emitter(video_path)
    video_emitter(cfront_url)
