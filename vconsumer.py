import os
import cv2
from keras.models import load_model
import tensorflow as tf
import numpy as np
from flask import Flask, Response, render_template
from kafka import KafkaConsumer
from utils import np_from_json
import json
import time
import wget


model_name = "mnist_model.h5"
topic = "frame_objs"

# connect to Kafka server and pass the topic we want to consume
consumer = KafkaConsumer(topic, group_id='view', 
                         bootstrap_servers=['0.0.0.0:9092'],
                         value_deserializer= lambda value: json.loads(value.decode()))

# get model from s3--> cloudfront --> dowmload
cfront_endpoint = "http://d3tj01z94i74qz.cloudfront.net/"
cfront_url = cfront_endpoint + model_name

if not os.path.isfile(model_name):
    wget.download(cfront_url, './mnist_model.h5')

model = load_model(model_name)
graph = tf.get_default_graph()

print("**Model Loaded from: {}".format(cfront_url))
print(model.summary())

# Continuously listen to the connection and print messages as recieved
app = Flask(__name__)


@app.route('/')
def index():
    return render_template('index.html')


@app.route('/cam')
def cam():
    # return a multipart response
    return Response(kafkastream(),
                    mimetype='multipart/x-mixed-replace; boundary=frame')


def get_result(frame_obj):
    
    """Processes value produced by producer, returns prediction with png image."""
    
    frame = np_from_json(frame_obj)

    # MNIST SPECIFIC
    frame = frame.reshape(28, 28, 1)

    # batch
    model_in = np.expand_dims(frame, axis=0)

    # predict
    with graph.as_default():
        model_out = np.argmax(np.squeeze(model.predict(model_in)))

    timestamp = int(frame_obj['timestamp'])
    camera = int(frame_obj['camera'])

    # convert the image png --> display
    _, png = cv2.imencode('.png', frame)

    result = {"timestamp": timestamp,
              "camera": camera,
              "frame_num": int(frame_obj['frame_num']),
              "prediction": model_out,
              "latency": time.time() - timestamp}
    
    return result, png


def kafkastream():
    for msg in consumer:
        result, png = get_result(msg.value)
        print("timestamp: {}, frame_num: {},camera_num: {}, latency: {}, y_hat: {}".format(result['timestamp'],
                                                                                           result['frame_num'],
                                                                                           result['camera'], 
                                                                                           result['latency'],
                                                                                           result['prediction'],))
        
        yield (b'--frame\r\n'
               b'Content-Type: image/png\r\n\r\n' + png.tobytes() + b'\r\n\r\n')


if __name__ == '__main__':
    app.run(host='0.0.0.0', debug=True)

