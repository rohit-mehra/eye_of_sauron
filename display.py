import cv2
from flask import Flask, Response, render_template
from kafka import KafkaConsumer
# from utils import np_from_json_v2 as np_from_json
# from utils import np_to_json_v2 as np_to_json
from utils import np_from_json
import json
from frame_producer_v2 import GRAY, TOTAL_CAMERAS
from heapq import heappush, heappop
import threading
import time
from collections import defaultdict
# TODO: add three prediction topics, three retreival

buffer_size = 500
buffer_dict = defaultdict(list)
data = dict()

prediction_topic_prefix = 'predicted_objs'
prediction_topics = ["{}_{}".format(prediction_topic_prefix, i) for i in range(TOTAL_CAMERAS)]

prediction_consumers = [KafkaConsumer(topic, group_id='view',
                                      bootstrap_servers=['0.0.0.0:9092'],
                                      # auto_offset_reset='earliest',
                                      # enable_auto_commit=False,
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
    # print(frame.shape) # (300, 300, 3)
    _, png = cv2.imencode('.png', frame)
    
    return png


def populate_buffer(msg_stream, number):
    try:
        while True:
            try:
                buffer = buffer_dict[number]
                msg = next(msg_stream)
                prediction_obj = msg.value
                frame_num = int(prediction_obj['frame_num'])
                png = get_png(prediction_obj)
                heappush(buffer, frame_num)
                data[frame_num] = png.tobytes()
                print('\rPushed: {} {}/{}'.format(prediction_obj['frame_num'], len(buffer), buffer_size), end='')
                if len(buffer) == buffer_size:
                    # Start Consuming event
                    print("Start Consuming from buffer..")
                    event.set()
            except StopIteration as e:
                print(e)
                continue

    except KeyboardInterrupt as e:
        print(e)
        event.clear()
        pass

    finally:
        print("Closing Stream")
        msg_stream.close()
        event.clear()


def consume_buffer(number):
    buffer = buffer_dict[number]
    print("Waiting for buffer to fill..")
    event.wait()
    print("Pushing to Flask..")
    err_count = 0
    while True:
        time.sleep(0.01)
        try:
            fn = heappop(buffer)
            print(fn)
            yield (b'--frame\r\n'
                   b'Content-Type: image/png\r\n\r\n' + data[fn] + b'\r\n\r\n')
        except Exception as e:
            print(e)
            err_count += 1
            if err_count > 18:
                break


# # camera specific consumer
# def get_frame(msg_stream, number):
#     try:
#
#         while True:
#             try:
#                 buffer = buffer_dict[number]
#                 msg = next(msg_stream)
#                 prediction_obj = msg.value
#                 frame_num = int(prediction_obj['frame_num'])
#                 png = get_png(prediction_obj)
#
#                 if len(buffer) < buffer_size:
#                     heappush(buffer, frame_num)
#                     data[frame_num] = png.tobytes()
#                     print('\rPushed: {} {}/{}'.format(prediction_obj['frame_num'], len(buffer), buffer_size), end='')
#                     continue
#
#                 fn = heappop(buffer)
#                 yield (b'--frame\r\n'
#                        b'Content-Type: image/png\r\n\r\n' + data[fn] + b'\r\n\r\n')
#
#             except StopIteration as e:
#                 print(e)
#                 continue
#
#     except KeyboardInterrupt as e:
#         print(e)
#         pass
#
#     finally:
#         print("Closing Stream")
#         msg_stream.close()
#

# Continuously listen to the connection and print messages as received
app = Flask(__name__)


@app.route('/cam/<number>')
def cam(number):
    # return a multipart response
    buffer_dict[number] = []
    b_thread = threading.Thread(target=populate_buffer, args=[prediction_consumers[int(number)], number])
    b_thread.start()
    return Response(consume_buffer(number),  # get_frame(prediction_consumers[int(number)]),
                    mimetype='multipart/x-mixed-replace; boundary=frame')
    # return Response(get_frame(prediction_consumers[int(number)], number),
    #                 mimetype='multipart/x-mixed-replace; boundary=frame')


@app.route('/')
def index():
    return render_template('index.html', posts=list(range(TOTAL_CAMERAS)))


if __name__ == '__main__':
    event = threading.Event()
    app.run(host='0.0.0.0', debug=True)

