<<<<<<< 6d51bda2fbc8e06d03522226b27f6eeac416cb37
import os
import sys
import time
from io import BytesIO

import cv2

import boto3
from kafka import KafkaClient, SimpleProducer

aws_access_key_id = os.environ.get("AWS_ACCESS_KEY_ID")
aws_secret_access_key = os.environ.get("AWS_SECRET_ACCESS_KEY")
resource = boto3.resource('s3')  # high-level object-oriented API
my_bucket = resource.Bucket('camera-data-eye')  # s3 bucket name.

# get object meta data in the directory
files = list(my_bucket.objects.filter(Prefix='cam1/videos/'))

print(files)

# choose and object and get its content
#choice = 0
#vid_obj = files[choice].get()['Body']
#vid_key = files[choice].key
#print("Reading: {}".format(vid_key))
#vid_stream = BytesIO(vid_obj.read())

# sys.exit(0)
=======
import time
import cv2
from kafka import KafkaProducer
import os
>>>>>>> frame bytes to numpy array in vconsumer.py

#  connect to Kafka
producer = KafkaProducer(bootstrap_servers=['localhost:9092'])

# Assign a topic
topic = 'frames'

<<<<<<< 6d51bda2fbc8e06d03522226b27f6eeac416cb37
video_path = "/home/ubuntu/eye_of_sauron/data/cam1/videos/cam1_2_fps.mp4"
=======
# video_path = "/home/ubuntu/eye_of_sauron/data/cam1/videos/cam1_2_fps.mp4" 
>>>>>>> frame bytes to numpy array in vconsumer.py

# serving from s3 bucket via cloudFront: url to the object
cfront_endpoint = "http://d3tj01z94i74qz.cloudfront.net/"
cfront_url = cfront_endpoint + "cam0/videos/cam0_5_fps.mp4"

# print(os.listdir("/home/ubuntu/eye_of_sauron/data/cam1/videos/"))


def video_emitter(video):
    # Open the video
    video = cv2.VideoCapture(video)
    print('Emitting.....')
    i = 0
    # read the file
    while (video.isOpened):
        # read the image in each frame
        success, image = video.read()
        # check if the file has read to the end
        if not success:
            print("BREAK AT FRAME: {}".format(i))
            break
        # convert the image png
        ret, jpeg = cv2.imencode('.png', image)
        # Convert the image to bytes and send to kafka
<<<<<<< 6d51bda2fbc8e06d03522226b27f6eeac416cb37
        producer.send_messages(topic, jpeg.tobytes())
        # To reduce CPU usage create sleep time of 0.2sec
=======
        producer.send(topic, jpeg.tobytes())
        # To reduce CPU usage create sleep time of 0.1sec  
>>>>>>> frame bytes to numpy array in vconsumer.py
        time.sleep(0.1)
        i += 1

    # clear the capture
    video.release()
    print('Done Emitting...')


if __name__ == '__main__':

    # video_emitter(video_path)
    video_emitter(cfront_url)
