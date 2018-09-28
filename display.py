import cv2
from flask import Flask, Response, render_template
from kafka import KafkaConsumer
from utils import np_from_json
import json
from heapq import heappush, heappop
import time
from collections import defaultdict

from params import *
from utils import cleanup_topics, init_frame_topic, get_video_feed_url
from frame_producer_v2 import StreamVideo

import threading
import logging


# logging.basicConfig(level=logging.INFO,
#                     format='[%(threadName)-6s] %(message)s')


def get_png(prediction_obj):
    """Processes value produced by producer, returns prediction with png image."""

    original_frame = np_from_json(prediction_obj, prefix_name=ORIGINAL_PREFIX)
    predicted_frame = np_from_json(prediction_obj, prefix_name=PREDICTED_PREFIX)

    # convert the image png --> display
    _, original_png = cv2.imencode('.png', original_frame)
    _, predicted_png = cv2.imencode('.png', predicted_frame)

    return original_png, predicted_png


def populate_buffer(msg_stream, number):
    global buffer_dict
    global data_dict
    global event_threads

    try:
        while True:
            try:
                msg = next(msg_stream)
                prediction_obj = msg.value
                frame_num = int(prediction_obj['frame_num'])
                original_png, predicted_png = get_png(prediction_obj)

                # HEAP
                heappush(buffer_dict[number], frame_num)
                # DATA DICT
                data_dict[number][frame_num] = (original_png.tobytes(), predicted_png.tobytes())

                print('\r[DISPLAY][CAM {}] Pushed: {} {}/{}'.format(number, prediction_obj['frame_num'],
                                                                    len(buffer_dict[number]), buffer_size), end='')

                if len(buffer_dict[number]) == buffer_size and not event_threads[number].is_set():
                    # Start Consuming event
                    print("\n[DISPLAY][CAM {}] Set Event Start Consuming from buffer..".format(number))
                    event_threads[number].set()

            except StopIteration as e:
                print(e)
                continue

    except KeyboardInterrupt as e:
        print(e)
        event_threads[number].clear()
        pass

    finally:
        print("Closing Stream")
        msg_stream.close()
        event_threads[number].clear()


def consume_buffer(number):
    global buffer_dict
    global data_dict
    global event_threads

    print("\n[CAM {}] Waiting for buffer to fill..[{}/{}]".format(number, len(buffer_dict[number]), buffer_size))

    event_threads[number].wait()
    print("\n[CAM {}] Pushing to Flask..".format(number))
    frame_number = heappop(buffer_dict[number])
    last_frame_num = frame_number
    original_frame, predicted_frame = data_dict[number][frame_number]

    while True:
        time.sleep(0.01)
        lock.acquire()

        if len(buffer_dict[number]):

            frame_number = heappop(buffer_dict[number])
            original_frame, predicted_frame = data_dict[number][frame_number]

            lock.release()

            yield (
                    b'--frame\r\n'
                    b'Content-Type: image/png\r\n\r\n' + predicted_frame + b'\r\n\r\n')

        else:
            lock.release()
            print("[CAM {}] STREAM ENDED AT FRAME {}".format(number, last_frame_num))
            yield (
                    b'--frame\r\n'
                    b'Content-Type: image/png\r\n\r\n' + predicted_frame + b'\r\n\r\n')

        # exit_barrier.wait()
        # break


# Continuously listen to the connection and print messages as received
app = Flask(__name__)


@app.route('/cam/<number>')
def cam(number):
    # return a multipart response
    return Response(consume_buffer(int(number)),
                    mimetype='multipart/x-mixed-replace; boundary=frame')


@app.route('/')
def index():
    return render_template('index.html', posts=cam_nums)


if __name__ == '__main__':

    """PRODUCTION"""
    # DELETE FRAME TOPIC, TO AVOID USING PREVIOUS JUNK DATA
    cleanup_topics()
    # INIT TOPIC WITH DESIRED PARTITIONS
    init_frame_topic()
    # GET IPs OF CAMERAS
    CAMERA_URLS = [get_video_feed_url(i, FPS) for i in [0, 1, 2, 3, 4, 5]]

    print(CAMERA_URLS)

    PRODUCERS = [StreamVideo(url, FRAME_TOPIC, SET_PARTITIONS, use_cv2=False, pub_obj_key=ORIGINAL_PREFIX) for url in
                 CAMERA_URLS]
    cam_nums = [p.camera_num for p in PRODUCERS]
    prediction_topics = {cam_num: "{}_{}".format(PREDICTION_TOPIC_PREFIX, cam_num) for cam_num in cam_nums}

    for p in PRODUCERS:
        p.start()

    """CONSUMPTION"""
    # prediction_topics = ["{}_{}".format(PREDICTION_TOPIC_PREFIX, i) for i in range(TOTAL_CAMERAS)]
    print('\n', prediction_topics)

    prediction_consumers = {cam_num: KafkaConsumer(topic, group_id='view',
                                                   bootstrap_servers=['0.0.0.0:9092'],
                                                   auto_offset_reset='earliest',
                                                   value_deserializer=lambda value: json.loads(value.decode()
                                                                                               )) for cam_num, topic in
                            prediction_topics.items()}

    buffer_size = 270
    buffer_dict = defaultdict(list)
    data_dict = defaultdict(dict)

    buffer_threads = dict()
    event_threads = dict()

    # exit_barrier = threading.Barrier(TOTAL_CAMERAS)
    lock = threading.Lock()

    # Thread
    for cam_num in cam_nums:
        event_threads[cam_num] = threading.Event()
        bt = threading.Thread(target=populate_buffer, args=[prediction_consumers[int(cam_num)], cam_num])
        buffer_threads[cam_num] = bt
        bt.start()

    app.run(host='0.0.0.0', debug=False, threaded=True)

    for p in PRODUCERS:
        p.join()

    for k, t in buffer_threads.items():
        t.join()

    print("Done....")
