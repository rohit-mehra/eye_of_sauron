import time
import cv2
from kafka import SimpleProducer, KafkaClient

#  connect to Kafka
kafka = KafkaClient('localhost:9092')
producer = SimpleProducer(kafka)

# Assign a topic
topic = 'frames'

video_path = "/home/ubuntu/video_stream_kafka/data/cam1/cam1_5_fps.mp4" 

def video_emitter(video):
    # Open the video
    video = cv2.VideoCapture(video)
    print('Emitting.....')

    # read the file
    while (video.isOpened):
        # read the image in each frame
        success, image = video.read()
        # check if the file has read to the end
        if not success:
            break
        # convert the image png
        ret, jpeg = cv2.imencode('.png', image)
        # Convert the image to bytes and send to kafka
        producer.send_messages(topic, jpeg.tobytes())
        # To reduce CPU usage create sleep time of 0.2sec  
        time.sleep(0.1)
    
    # clear the capture
    video.release()
    print('Done emitting...')

if __name__ == '__main__':
    
    video_emitter(video_path)