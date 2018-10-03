import base64
import json
import time
from heapq import heappush, heappop

import cv2
import numpy as np
from kafka import KafkaConsumer

from params import *


def consumer(cam_num, buffer_dict, data_dict, buffer_size=180):
    """Generator to yield frames from the respective camera.
    :param buffer_size: Buffer Size
    :param data_dict: Data Stored here, buffer only stores keys
    :param buffer_dict: Collection of buffers for different cameras
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
                raw_messages = msg_stream.poll(timeout_ms=1000, max_records=60)

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

                    time.sleep(0.01)

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
    """Generator to yield frames from the respective camera. Threaded Concept.
    :param buffer_size: Buffer Size
    :param lock: To ensure no deadlock while accessing buffer, as in population and consumption
    :param event_threads: To set specific consumption event, when specific buffer is full, specific to camera/stream
    :param data_dict: Data Stored here, buffer only stores keys
    :param buffer_dict: Collection of buffers for different cameras
    :param cam_num: camera number.
    """
    # Print log
    print(
        "\n[CAM {}][FLASK] Waiting for buffer to fill..[{}/{}]".format(cam_num, len(buffer_dict[cam_num]), buffer_size))
    event_threads[cam_num].wait()

    # Start consumption event as soon as the buffer hits the threshold.
    print("\n\n[CAM {}][FLASK] Pushing to Flask..".format(cam_num))
    # Init variables
    frame_number = 0
    original_frame, predicted_frame = bytes(0), bytes(0)

    while True:
        time.sleep(0.02)
        # Acquire sync lock, prevents deadlock and maintains consistency
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
    :param buffer_size: Buffer Size
    :param event_threads: To set specific consumption event, when specific buffer is full, specific to camera/stream
    :param data_dict: Data Stored here, buffer only stores keys
    :param buffer_dict: Collection of buffers for different cameras
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
    """Util function to clear frame topic.
    :param partitions:
    :param frame_topic:
    """

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


def clear_known_face_topic():
    """Util function to clear known face broadcasting topic."""
    os.system(
        "/usr/local/kafka/bin/kafka-topics.sh --zookeeper localhost:2181 --delete --topic {}".format(KNOWN_FACE_TOPIC))


def clear_prediction_topics(prediction_prefix=PREDICTION_TOPIC_PREFIX):
    """Clear prediction topics. Specific to Camera Number.
    :param prediction_prefix: Just a stamp for this class of topics
    """

    for i in range(TOTAL_CAMERAS + 1, 0, -1):
        print()
        # DELETE PREDICTION TOPICs, TO AVOID USING PREVIOUS JUNK DATA
        os.system("/usr/local/kafka/bin/kafka-topics.sh --zookeeper localhost:2181 --delete --topic {}_{}".format(
            prediction_prefix, i))


def np_to_json(obj, prefix_name=''):
    """Serialize numpy.ndarray obj
    :param prefix_name: unique name for this array.
    :param obj: numpy.ndarray"""
    return {'{}_frame'.format(prefix_name): base64.b64encode(obj.tostring()).decode("utf-8"),
            '{}_dtype'.format(prefix_name): obj.dtype.str,
            '{}_shape'.format(prefix_name): obj.shape}


def np_from_json(obj, prefix_name=''):
    """Deserialize numpy.ndarray obj
    :param prefix_name: unique name for this array.
    :param obj: numpy.ndarray"""
    return np.frombuffer(base64.b64decode(obj['{}_frame'.format(prefix_name)].encode("utf-8")),
                         dtype=np.dtype(obj['{}_dtype'.format(prefix_name)])).reshape(
        obj['{}_shape'.format(prefix_name)])


def get_video_feed_url(camera_num=0, folder="videos"):
    """Get CAMERA IP from where video is being streamed.
    :returns A URL to the stream.
    """
    # serving from s3 bucket via cloudFront: url to the object
    return C_FRONT_ENDPOINT + "{}/{}.mp4".format(folder, camera_num)
