import os
import sys
from io import BytesIO

from keras.models import load_model

import boto3
import wget
from flask import Flask, Response
from kafka import KafkaConsumer

model_name = "mnist_model.h5"
topic = "frames"

# model from s3--> cloudfront --> dowmload
cfront_endpoint = "http://d3tj01z94i74qz.cloudfront.net/"
cfront_url = cfront_endpoint + model_name

if not os.path.isfile(model_name):
    wget.download(cfront_url, './mnist_model.h5')

model = load_model(model_name)
print("**Model Loaded from: {}".format(cfront_url))
print(model.summary())
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
        yield (b'--frame\r\n'
               b'Content-Type: image/png\r\n\r\n' + msg.value + b'\r\n\r\n')


if __name__ == '__main__':
    app.run(host='0.0.0.0', debug=True)
