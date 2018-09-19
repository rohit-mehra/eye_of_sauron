import time
import cv2
from kafka import KafkaProducer
import os

#  connect to Kafka
producer = KafkaProducer(bootstrap_servers=['localhost:9092'])

# Assign a topic
topic = 'frames'

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
        producer.send(topic, jpeg.tobytes())
        # To reduce CPU usage create sleep time of 0.1sec  
        time.sleep(0.1)
        i += 1

    # clear the capture
    video.release()
    print('Done Emitting...')


if __name__ == '__main__':

    # video_emitter(video_path)
    video_emitter(cfront_url)
