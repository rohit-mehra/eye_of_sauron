import time
import cv2
from kafka import SimpleProducer, KafkaClient
import os
import boto3
import sys
from io import BytesIO

aws_access_key_id = os.environ.get("AWS_ACCESS_KEY_ID")
aws_secret_access_key = os.environ.get("AWS_SECRET_ACCESS_KEY")
resource = boto3.resource('s3') # high-level object-oriented API
my_bucket = resource.Bucket('camera-data-eye') # s3 bucket name. 

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

#  connect to Kafka
kafka = KafkaClient('localhost:9092')
producer = SimpleProducer(kafka)

# Assign a topic
topic = 'frames'

video_path = "/home/ubuntu/eye_of_sauron/data/cam1/videos/cam1_2_fps.mp4" 

# serving from s3 bucket via cloudFront: url to the object
cfront_endpoint = "http://d3tj01z94i74qz.cloudfront.net/"
cfront_url = cfront_endpoint + "cam0/videos/cam0_5_fps.mp4"

print(os.listdir("/home/ubuntu/eye_of_sauron/data/cam1/videos/"))

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
        producer.send_messages(topic, jpeg.tobytes())
        # To reduce CPU usage create sleep time of 0.2sec  
        time.sleep(0.1)
        i += 1
    
    # clear the capture
    video.release()
    print('Done emitting...')

if __name__ == '__main__':
    
    # video_emitter(video_path)
    video_emitter(cfront_url)
