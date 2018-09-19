import os
import sys
import cv2
from keras.models import load_model
import numpy as np
import wget
from flask import Flask, Response
from kafka import KafkaConsumer
from utils import np_from_json
import json


model_name = "mnist_model.h5"
topic = "frame_objs"

# get model from s3--> cloudfront --> dowmload
cfront_endpoint = "http://d3tj01z94i74qz.cloudfront.net/"
cfront_url = cfront_endpoint + model_name

if not os.path.isfile(model_name):
    wget.download(cfront_url, './mnist_model.h5')

model = load_model(model_name)
print("**Model Loaded from: {}".format(cfront_url))

# connect to Kafka server and pass the topic we want to consume
consumer = KafkaConsumer(topic, group_id='view', bootstrap_servers=['0.0.0.0:9092'])

# Continuously listen to the connection and print messages as recieved
app = Flask(__name__)


@app.route('/')
def index():
    # return a multipart response
    return Response(kafkastream(),
                    mimetype='multipart/x-mixed-replace; boundary=frame')


def kafkastream():
    for msg in consumer:
        frame_obj = json.loads(msg.value.decode('latin1')) # {"timestamp":time.time(), "frame":serialized_image, "camera":CAMERA_NUM, "display":jpeg.tobytes()}
        
        frame = np_from_json(frame_obj)
        timestamp = int(frame_obj['timestamp'])
        camera = int(frame_obj['camera'])

        # convert the image png --> display
        ret, png = cv2.imencode('.png', frame)
        print(frame.shape, timestamp, camera)
        
        yield (b'--frame\r\n'
               b'Content-Type: image/png\r\n\r\n' + png.tobytes() + b'\r\n\r\n')


if __name__ == '__main__':
    app.run(host='0.0.0.0', debug=True)

