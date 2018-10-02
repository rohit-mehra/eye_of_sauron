import cv2
from flask import Flask, Response, render_template
from kafka import KafkaConsumer
from utils import np_from_json
import json
from heapq import heappush, heappop
import time
from collections import defaultdict

from params import *


def get_png(prediction_obj):
    """Processes value produced by producer, returns prediction with png image.
    :param prediction_obj:
    :return: original frame, predicted frame with bounding box or prediction written over it
    """

    original_frame = np_from_json(prediction_obj, prefix_name=ORIGINAL_PREFIX)
    predicted_frame = np_from_json(prediction_obj, prefix_name=PREDICTED_PREFIX)

    # convert the image png --> display
    _, original_png = cv2.imencode('.png', original_frame)
    _, predicted_png = cv2.imencode('.png', predicted_frame)

    return original_png, predicted_png


def consume_buffer(cam_num):
    """Generator to yield frames from the respective camera.
    :param cam_num: camera number.
    """
    # Global data base, could scale if these are external, for simplicity declared as global data structures.
    global buffer_dict
    global data_dict

    topic = "{}_{}".format(PREDICTION_TOPIC_PREFIX, cam_num)
    msg_stream = KafkaConsumer(topic, group_id='view',
                               bootstrap_servers=['0.0.0.0:9092'],
                               auto_offset_reset='earliest',
                               value_deserializer=lambda value: json.loads(value.decode()
                                                                           ))
    try:
        # start consuming msg stream
        while True:
            frame_number = 0
            original_frame, predicted_frame = bytes(0), bytes(0)
            try:
                raw_messages = msg_stream.poll(timeout_ms=100, max_records=50)

                for topic_partition, msgs in raw_messages.items():
                    # Get the predicted Object, JSON with frame and meta info about the frame
                    for msg in msgs:
                        prediction_obj = msg.value
                        # frame cam_num
                        frame_num = int(prediction_obj['frame_num'])
                        # extract images from the prediction message
                        original_png, predicted_png = get_png(prediction_obj)

                        # HEAP BUFFER TO MAINTAIN THE ORDER: ONLY FRAME NUMBERS ARE PUSHED
                        heappush(buffer_dict[cam_num], frame_num)
                        # DATA DICT: TO COLLECT REAL FRMAES
                        data_dict[cam_num][frame_num] = (original_png.tobytes(), predicted_png.tobytes())
                        # print log
                        print('\r[CAM {}][BUFFER] Pushed: {} {}/{}'.format(cam_num, prediction_obj['frame_num'],
                                                                           len(buffer_dict[cam_num]), buffer_size),
                              end='')

                # as soon as buffer is full for the first time, start consuming/display event on flask
                if len(buffer_dict[cam_num]) >= buffer_size:
                    if DL == "mnist":
                        time.sleep(0.5)
                    else:
                        time.sleep(0.03)

                    last_frame_num = frame_number

                    if len(buffer_dict[cam_num]):

                        frame_number = heappop(buffer_dict[cam_num])
                        original_frame, predicted_frame = data_dict[cam_num][frame_number]

                        yield (
                                b'--frame\r\n'
                                b'Content-Type: image/png\r\n\r\n' + predicted_frame + b'\r\n\r\n')

                    else:
                        print("[CAM {}] STREAM ENDED AT FRAME {}".format(cam_num, last_frame_num))
                        yield (
                                b'--frame\r\n'
                                b'Content-Type: image/png\r\n\r\n' + predicted_frame + b'\r\n\r\n')

            except StopIteration as e:
                print(e)
                continue

    except KeyboardInterrupt as e:
        print(e)
        pass

    finally:
        print("Closing Stream")
        msg_stream.close()


# Continuously listen to the connection and print messages as received
app = Flask(__name__)


@app.route('/cam/<cam_num>')
def cam(cam_num):
    # return a multipart response
    return Response(consume_buffer(int(cam_num)),
                    mimetype='multipart/x-mixed-replace; boundary=frame')


@app.route('/')
def index():
    global cam_nums
    half = len(cam_nums) // 2
    camera_numbers_1 = cam_nums[:half]
    camera_numbers_2 = cam_nums[half:]
    return render_template('index.html', camera_numbers_1=camera_numbers_1, camera_numbers_2=camera_numbers_2)


if __name__ == '__main__':
    buffer_size = 150
    buffer_dict = defaultdict(list)
    data_dict = defaultdict(dict)

    """--------------CONSUMPTION--------------"""
    cam_nums = [i for i in range(1, TOTAL_CAMERAS + 1)]
    prediction_topics = {cam_num: "{}_{}".format(PREDICTION_TOPIC_PREFIX, cam_num) for cam_num in cam_nums}

    print('\n', prediction_topics)

    """--------------WEB APP--------------"""
    app.run(host='0.0.0.0', debug=True, port=3333)

    print("Done....")
