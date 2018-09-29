from params import *
import base64
import os
import wget
import time


def cleanup_topics():
    os.system("/usr/local/kafka/bin/kafka-topics.sh --zookeeper localhost:2181 --delete --topic {}".format(FRAME_TOPIC))
    for i in range(8):
        # DELETE PREDICTION TOPICs, TO AVOID USING PREVIOUS JUNK DATA
        os.system("/usr/local/kafka/bin/kafka-topics.sh --zookeeper localhost:2181 --delete --topic {}_{}".format(PREDICTION_TOPIC_PREFIX, i))


def init_frame_topic():
    os.system(INIT_CMD)
    time.sleep(5)
    # SANITY CHECK ALTER TOPIC WITH DESIRED PARTITIONS
    os.system(ALTER_CMD)


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
        return OD_MODEL_PATH, OD_PROTO_PATH, LABEL_PATH

    if target == "image_classification":
        check_or_get_file(MODEL_PATH, MODEL_NAME)
        check_or_get_file(PROTO_PATH, PROTO_NAME)
        check_or_get_file(LABEL_PATH, LABEL_NAME)
        return MODEL_PATH, PROTO_PATH, LABEL_PATH


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
