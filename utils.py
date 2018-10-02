import base64
import json
import os
import time
from heapq import heappush, heappop

import cv2
import wget
from kafka import KafkaConsumer

from params import *


def consumer(cam_num, buffer_dict, data_dict, buffer_size=180):
    """Generator to yield frames from the respective camera.
    :param buffer_size:
    :param data_dict:
    :param buffer_dict:
    :param cam_num: camera number.
    """

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
                raw_messages = msg_stream.poll(timeout_ms=1000, max_records=30)

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
                        print('\r[CAM {}][PART 1][BUFFER] Pushed: {} {}/{}'.format(cam_num, prediction_obj['frame_num'],
                                                                                   len(buffer_dict[cam_num]),
                                                                                   buffer_size),
                              end='')

                # as soon as buffer is full for the first time, start consuming/display event on flask
                if len(buffer_dict[cam_num]) >= buffer_size:
                    print("[CAM {}][PART 2] YIELD".format(cam_num))

                    if DL == "mnist":
                        time.sleep(0.5)
                    else:
                        time.sleep(0.03)

                    last_frame_num = frame_number

                    if len(buffer_dict[cam_num]):

                        frame_number = heappop(buffer_dict[cam_num])
                        print(frame_number)
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


def consume_buffer(cam_num, buffer_dict, data_dict, event_threads, lock, buffer_size=180):
    """Generator to yield frames from the respective camera.
    :param buffer_size:
    :param lock:
    :param event_threads:
    :param data_dict:
    :param buffer_dict:
    :param cam_num: camera number.
    """
    # print log
    print(
        "\n[CAM {}][FLASK] Waiting for buffer to fill..[{}/{}]".format(cam_num, len(buffer_dict[cam_num]), buffer_size))
    event_threads[cam_num].wait()

    # start consumption event as soon as the buffer hits the threshold.
    print("\n\n[CAM {}][FLASK] Pushing to Flask..".format(cam_num))
    # init variables
    frame_number = 0
    original_frame, predicted_frame = bytes(0), bytes(0)

    while True:
        if DL == "mnist":
            time.sleep(0.5)
        else:
            time.sleep(0.03)

        lock.acquire()
        last_frame_num = frame_number
        if len(buffer_dict[cam_num]):

            frame_number = heappop(buffer_dict[cam_num])
            original_frame, predicted_frame = data_dict[cam_num][frame_number]
            lock.release()

            yield (
                    b'--frame\r\n'
                    b'Content-Type: image/png\r\n\r\n' + predicted_frame + b'\r\n\r\n')

        else:
            lock.release()
            print("[CAM {}] STREAM ENDED AT FRAME {}".format(cam_num, last_frame_num))
            yield (
                    b'--frame\r\n'
                    b'Content-Type: image/png\r\n\r\n' + predicted_frame + b'\r\n\r\n')


def populate_buffer(msg_stream, cam_num, buffer_dict, data_dict, event_threads, buffer_size=180):
    """Fills the heap buffer, sets of an event to display as soon as set buffer limit hits.
    :param buffer_size:
    :param event_threads:
    :param data_dict:
    :param buffer_dict:
    :param msg_stream: message stream from respective camera topic, topic in format [PREDICTION_TOPIC_PREFIX]_[cam_num]
    :param cam_num: camera number, used to access respective buffer, or data
    """

    try:
        # start populating the buffer
        while True:
            try:
                msg = next(msg_stream)
                # Get the predicted Object, JSON with frame and meta info about the frame
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
                                                                   len(buffer_dict[cam_num]), buffer_size), end='')

                # as soon as buffer is full for the first time, start consuming/display event on flask
                if len(buffer_dict[cam_num]) == buffer_size and not event_threads[cam_num].is_set():
                    print("\n[CAM {}][BUFFER] Starting Consuming loop..".format(cam_num))
                    event_threads[cam_num].set()

            except StopIteration as e:
                print(e)
                continue

    except KeyboardInterrupt as e:
        print(e)
        event_threads[cam_num].clear()
        pass

    finally:
        print("Closing Stream")
        msg_stream.close()
        event_threads[cam_num].clear()


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


def clear_frame_topic(frame_topic=FRAME_TOPIC, partitions=SET_PARTITIONS):
    os.system("/usr/local/kafka/bin/kafka-topics.sh --zookeeper localhost:2181 --delete --topic {}".format(frame_topic))
    # SETTING UP TOPIC WITH DESIRED PARTITIONS
    init_cmd = "/usr/local/kafka/bin/kafka-topics.sh --create --zookeeper localhost:2181 " \
               "--replication-factor 3 --partitions {} --topic {}".format(partitions, frame_topic)

    print('\n', init_cmd, '\n')
    os.system(init_cmd)

    time.sleep(5)

    # SETTING UP TOPIC WITH DESIRED PARTITIONS
    alter_cmd = "/usr/local/kafka/bin/kafka-topics.sh --alter --zookeeper localhost:2181 " \
                "--topic frame_objects_v2 --partitions {}".format(partitions)

    # SANITY CHECK ALTER TOPIC WITH DESIRED PARTITIONS
    print('\n', alter_cmd, '\n')
    os.system(alter_cmd)


def clear_prediction_topics(prediction_prefix=PREDICTION_TOPIC_PREFIX, ):
    for i in range(TOTAL_CAMERAS + 1, 0, -1):
        print()
        # DELETE PREDICTION TOPICs, TO AVOID USING PREVIOUS JUNK DATA
        os.system("/usr/local/kafka/bin/kafka-topics.sh --zookeeper localhost:2181 --delete --topic {}_{}".format(
            prediction_prefix, i))


def check_or_get_file(file_path, file_name):
    if not os.path.isfile(file_path):
        print("[DOWNLOADING] to file_path")
        url = C_FRONT_ENDPOINT + file_name
        wget.download(url, file_path)

    print("[INFO]{} present..".format(file_path))


def get_model_proto(target):
    if target == "object_detection":
        check_or_get_file(OD_MODEL_PATH, OD_MODEL_NAME)
        check_or_get_file(OD_PROTO_PATH, OD_PROTO_NAME)
        check_or_get_file(LABEL_PATH, LABEL_NAME)
        return OD_MODEL_PATH, OD_PROTO_PATH, None

    if target == "image_classification":
        check_or_get_file(MODEL_PATH, MODEL_NAME)
        check_or_get_file(PROTO_PATH, PROTO_NAME)
        check_or_get_file(LABEL_PATH, LABEL_NAME)
        return MODEL_PATH, PROTO_PATH, LABEL_PATH

    if target == "mnist":
        check_or_get_file(M_MODEL_PATH, M_MODEL_NAME)
        return M_MODEL_PATH, None, None


def np_to_json(obj, prefix_name=''):
    """Serialize numpy.ndarray obj"""
    return {'{}_frame'.format(prefix_name): base64.b64encode(obj.tostring()).decode("utf-8"),
            '{}_dtype'.format(prefix_name): obj.dtype.str,
            '{}_shape'.format(prefix_name): obj.shape}


def np_from_json(obj, prefix_name=''):
    """Deserialize numpy.ndarray obj"""
    return np.frombuffer(base64.b64decode(obj['{}_frame'.format(prefix_name)].encode("utf-8")),
                         dtype=np.dtype(obj['{}_dtype'.format(prefix_name)])).reshape(
        obj['{}_shape'.format(prefix_name)])


def np_to_json_v2(obj):
    """Serialize numpy.ndarray obj"""
    return {'frame': obj.tostring().decode("latin1"),
            'dtype': obj.dtype.str,
            'shape': obj.shape}


def np_from_json_v2(obj):
    """Deserialize numpy.ndarray obj"""
    return np.frombuffer(obj['frame'].encode("latin1"),
                         dtype=np.dtype(obj['dtype'])
                         ).reshape(obj['shape'])


def get_video_feed_url(camera_num=0, fps=30):
    """Get CAMERA IP from where video is being streamed.
    Args:
        camera_num: camera number
        fps: fps os stream
    Returns:
        A URL to the stream.
    """
    # serving from s3 bucket via cloudFront: url to the object
    return C_FRONT_ENDPOINT + "videos/cam{}_{}_fps.mp4".format(camera_num, fps)


def get_mnist_feed_url(camera_num=0, fps=2):
    """Get CAMERA IP from where video is being streamed.
    Args:
        camera_num: camera number
        fps: fps os stream
    Returns:
        A URL to the stream.
    """
    # serving from s3 bucket via cloudFront: url to the object
    return C_FRONT_ENDPOINT + "mvideos/cam{}_{}_fps.mp4".format(camera_num, fps)
