import os
from keras.models import load_model
import tensorflow as tf
import numpy as np
from kafka import KafkaConsumer, KafkaProducer
from utils import np_from_json
import json
import time
import wget


# MODEL
MODEL_NAME = "mnist_model.h5"
# get model from s3--> cloudfront --> dowmload
cfront_endpoint = "http://d3tj01z94i74qz.cloudfront.net/"
cfront_url = cfront_endpoint + MODEL_NAME

if not os.path.isfile(MODEL_NAME):
    wget.download(cfront_url, './mnist_model.h5')

MODEL = load_model(MODEL_NAME)
GRAPH = tf.get_default_graph()
print(MODEL.summary())
print("**Model Loaded from: {}".format(cfront_url))

# KAFKA TODO: Check kafka compression, multiple consumer, threads safe producer

frame_topic = "frame_objs"
# COnnect to kafka, Consume frame obj bytes deserialize to json
frame_consumer = KafkaConsumer(frame_topic, group_id='predict',
                               bootstrap_servers=['0.0.0.0:9092'],
                               value_deserializer=lambda value: json.loads(value.decode()))

#  connect to Kafka
prediction_producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                                    value_serializer=lambda hashmap: json.dumps(hashmap).encode())

prediction_topic = 'predicted_objs'


def get_prediction_object(frame_obj):
    """Processes value produced by producer, returns prediction with png image."""

    frame = np_from_json(frame_obj)  # frame_obj = json

    # MNIST SPECIFIC
    frame = frame.reshape(28, 28, 1)

    # batch
    model_in = np.expand_dims(frame, axis=0)

    # predict
    with GRAPH.as_default():
        model_out = np.argmax(np.squeeze(MODEL.predict(model_in)))

    result = {"prediction": str(model_out),
              "predict_time": str(time.time()),
              "latency": str(time.time() - int(frame_obj['timestamp']))}

    result.update(frame_obj)
    return result


def process_stream(msg_stream):
    try:
        while True:
            try:
                msg = next(msg_stream)
                result = get_prediction_object(msg.value)
                prediction_producer.send(prediction_topic, value=result)
            except StopIteration:
                continue

    except KeyboardInterrupt:
        pass

    finally:
        msg_stream.close()


if __name__ == '__main__':
    process_stream(frame_consumer)

