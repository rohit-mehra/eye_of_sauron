import time
import cv2
from kafka import SimpleProducer, KafkaClient
import os
#  connect to Kafka
kafka = KafkaClient('localhost:9092')
producer = SimpleProducer(kafka)

# Assign a topic
topic = 'frames'

video_path = "/home/ubuntu/eye_of_sauron/data/cam1/videos/cam1_2_fps.mp4" 

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
    
    video_emitter(video_path)
