import os
from keras.models import load_model
import tensorflow as tf
import numpy as np
from kafka import KafkaConsumer, KafkaProducer
from utils import np_from_json
import json
import time
import wget

# Gets new task from a pool of frame objects
# TODO: publish to camera specific topic, DONE

# MODEL
MODEL_NAME = "mnist_model.h5"
# get model from s3--> cloudfront --> dowmload
cfront_endpoint = "http://d3tj01z94i74qz.cloudfront.net/"
model_url = cfront_endpoint + MODEL_NAME

if not os.path.isfile(MODEL_NAME):
    wget.download(model_url, './mnist_model.h5')

MODEL = load_model(MODEL_NAME)
GRAPH = tf.get_default_graph()
print(MODEL.summary())
print("**Model Loaded from: {}".format(model_url))

# KAFKA TODO: Check kafka compression, multiple consumer, threads safe producer

# COMMON TOPIC
FRAME_TOPIC = 'frame_objects'

# Connect to kafka, Consume frame obj bytes deserialize to json
frame_consumer = KafkaConsumer(FRAME_TOPIC, group_id='predict',
                               bootstrap_servers=['0.0.0.0:9092'],
                               value_deserializer=lambda value: json.loads(value.decode()))

#  connect to Kafka
prediction_producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                                    value_serializer=lambda hashmap: json.dumps(hashmap).encode())

prediction_topic_prefix = 'predicted_objs'


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

    print(result)
    result.update(frame_obj)
    return result


def process_stream(msg_stream):
    try:
        while True:
            try:
                msg = next(msg_stream)
                result = get_prediction_object(msg.value)
                print("timestamp: {}, frame_num: {},camera_num: {}, latency: {}, y_hat: {}".format(result['timestamp'],
                                                                                                   result['frame_num'],
                                                                                                   result['camera'],
                                                                                                   result['latency'],
                                                                                                   result['prediction']
                                                                                                   ))
                # camera specific topic
                prediction_topic = "{}_{}".format(prediction_topic_prefix, result['camera'])
                prediction_producer.send(prediction_topic, value=result)

            except StopIteration as e:
                print(e)
                continue

    except KeyboardInterrupt as e:
        print(e)
        pass

    finally:
        print("Closing Stream")
        msg_stream.close()


if __name__ == '__main__':
    process_stream(frame_consumer)

