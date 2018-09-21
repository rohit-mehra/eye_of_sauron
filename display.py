import cv2
import numpy as np
from flask import Flask, Response, render_template
from kafka import KafkaConsumer
from utils import np_from_json
import json

# TODO: add three prediction topics, three retreival

TOTAL_CAMERAS = 3
prediction_topic_prefix = 'predicted_objs'
prediction_topics = ["{}_{}".format(prediction_topic_prefix, i) for i in range(TOTAL_CAMERAS)]

prediction_consumers = [KafkaConsumer(topic, group_id='view',
                                      bootstrap_servers=['0.0.0.0:9092'],
                                      value_deserializer=lambda value: json.loads(value.decode())) for topic in prediction_topics]


def get_prediction(frame_obj):

    """Processes value produced by producer, returns prediction with png image."""

    frame = np_from_json(frame_obj)

    # MNIST SPECIFIC
    frame = frame.reshape(28, 28, 1)

    # convert the image png --> display
    _, png = cv2.imencode('.png', frame)
    
    return frame_obj, png


# camera specific consumer
def get_frame(consumer):
    for msg in consumer:
        result, png = get_prediction(msg.value)
        yield (b'--frame\r\n'
               b'Content-Type: image/png\r\n\r\n' + png.tobytes() + b'\r\n\r\n')


# Continuously listen to the connection and print messages as received

app = Flask(__name__)


@app.route('/cam/<number>')
def cam(number):
    # return a multipart response
    return Response(get_frame(prediction_consumers[int(number)]),
                    mimetype='multipart/x-mixed-replace; boundary=frame')


@app.route('/')
def index():
    return render_template('index.html', posts=list(range(TOTAL_CAMERAS)))


if __name__ == '__main__':
    app.run(host='0.0.0.0', debug=True)

