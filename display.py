import cv2
from flask import Flask, Response, render_template
from kafka import KafkaConsumer
from utils import np_from_json
import json
from frame_producer_v2 import GRAY
# TODO: add three prediction topics, three retreival

TOTAL_CAMERAS = 3
prediction_topic_prefix = 'predicted_objs'
prediction_topics = ["{}_{}".format(prediction_topic_prefix, i) for i in range(TOTAL_CAMERAS)]

prediction_consumers = [KafkaConsumer(topic, group_id='view',
                                      bootstrap_servers=['0.0.0.0:9092'],
                                      auto_offset_reset='latest',
                                      fetch_max_bytes=15728640,
                                      max_partition_fetch_bytes=15728640,
                                      value_deserializer=lambda value: json.loads(value.decode())) for topic in prediction_topics]


def get_png(prediction_obj):

    """Processes value produced by producer, returns prediction with png image."""

    frame = np_from_json(prediction_obj)

    # MNIST SPECIFIC
    if GRAY:
        frame = frame.reshape(28, 28, 1)

    # convert the image png --> display
    _, png = cv2.imencode('.png', frame)
    
    return png


# camera specific consumer
def get_frame(consumer):
    for msg in consumer:
        prediction_obj = msg.value
        png = get_png(prediction_obj)
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

