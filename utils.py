import numpy as np
import base64
import os
import wget

ENDPOINT = "http://d3tj01z94i74qz.cloudfront.net/"
MODEL_NAME = "MobileNetSSD_deploy.caffemodel"
PROTO_NAME = "MobileNetSSD_deploy.prototxt.txt"

MODEL_PATH = "models/{}".format(MODEL_NAME)
PROTO_PATH = "models/{}".format(PROTO_NAME)

# initialize the list of class labels MobileNet SSD was trained to
# detect, then generate a set of bounding box colors for each class
CLASSES = ["background", "aeroplane", "bicycle", "bird", "boat",
           "bottle", "bus", "car", "cat", "chair", "cow", "diningtable",
           "dog", "horse", "motorbike", "person", "pottedplant", "sheep",
           "sofa", "train", "tvmonitor"]

COLORS = np.random.uniform(0, 255, size=(len(CLASSES), 3))
CONFIDENCE = 0.3


def check_or_get_file(file_path, file_name):
    if not os.path.isfile(file_path):
        url = ENDPOINT + file_name
        wget.download(url, file_path)

    print("[INFO]{} present..".format(file_path))


def np_to_json(obj):
    """Searialize numpy.ndarray obj"""
    return {'frame': base64.b64encode(obj.tostring()).decode("utf-8"),
            'dtype': obj.dtype.str,
            'shape': obj.shape}


def np_from_json(obj):
    """Desearialize numpy.ndarray obj"""
    return np.frombuffer(base64.b64decode(obj['frame'].encode("utf-8")), 
                         dtype=np.dtype(obj['dtype'])
                        ).reshape(obj['shape'])