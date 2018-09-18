from flask import Flask, Response
from kafka import KafkaConsumer

from keras.models import load_model

import os
import boto3
import sys
from io import BytesIO

aws_access_key_id = os.environ.get("AWS_ACCESS_KEY_ID")
aws_secret_access_key = os.environ.get("AWS_SECRET_ACCESS_KEY")
resource = boto3.resource('s3') # high-level object-oriented API
my_bucket = resource.Bucket('camera-data-eye') # s3 bucket name. 

## model from s3--> cloudfront
cfront_endpoint = "http://d3tj01z94i74qz.cloudfront.net/"
cfront_url = cfront_endpoint + "mnist_model.h5"
model = load_model(cfront_url)
print("**Model Loaded from: {}".format(cfront_url))
print(model.summary())
topic = "frames"
#connect to Kafka server and pass the topic we want to consume
consumer = KafkaConsumer(topic, group_id='view', bootstrap_servers=['0.0.0.0:9092'])

#Continuously listen to the connection and print messages as recieved
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
