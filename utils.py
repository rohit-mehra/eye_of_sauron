import numpy as np
import base64
import os
import wget


SET_PARTITIONS = 4

ENDPOINT = "http://d3tj01z94i74qz.cloudfront.net/"
MODEL_NAME = "MobileNetSSD_deploy.caffemodel"
PROTO_NAME = "MobileNetSSD_deploy.prototxt.txt"

MODEL_PATH = "models/{}".format(MODEL_NAME)
PROTO_PATH = "models/{}".format(PROTO_NAME)

TARGET = "person"
# initialize the list of class labels MobileNet SSD was trained to
# detect, then generate a set of bounding box colors for each class
CLASSES = ["background",
           "aeroplane",
           "bicycle",
           "bird",
           "boat",
           "bottle",
           "bus",
           "car",
           "cat",
           "chair",
           "cow",
           "diningtable",
           "dog",
           "horse",
           "motorbike",
           "person",
           "pottedplant",
           "sheep",
           "sofa",
           "train",
           "tvmonitor"]

TARGET_IDX = CLASSES.index(TARGET)

# COLORS = np.random.uniform(0, 255, size=(len(CLASSES), 3))
COLORS = np.array([[217.63093899006873, 39.443811774140016, 181.07467002104076],
                   [22.852839055613575, 144.21820767899936, 191.4664495659867],
                   [91.37876682500746, 81.0656519368408, 7.689263671387896],
                     [154.53783307766344, 184.17558819037657, 243.09347851353857],
                     [56.569106780389475, 193.87289312113188, 120.27628003837243],
                     [142.32584189028591, 180.9114776927323, 8.327147093629915],
                     [200.3954344909704, 226.06683042113693, 223.02729762031203],
                     [209.61495397855808, 228.89144272460263, 136.35096620523692],
                     [21.19870440290346, 197.19857352940718, 52.65642614276703],
                     [248.4035624386067, 252.17317435833232, 228.00097659858793],
                     [84.37592108737798, 67.84689919374591, 236.42566801061318],
                     [2.2922399073885624, 147.0229560877954, 233.06209735567316],
                     [230.11377710596082, 206.62784603182712, 71.34553738394803],
                     [211.37389097513318, 160.1731818878586, 178.19199287453415],
                     [83.11365415090869, 4.116987644879445, 237.0531177081247],
                     [52.9175892462011, 71.74236797079084, 139.10105045353123],
                     [35.04144959229029, 85.91273662542486, 124.5400255863491],
                     [99.62518781654794, 87.90434925113598, 52.036263751028635],
                     [109.02287994197309, 173.9319305124548, 149.7242396993405],
                     [141.16826220502148, 94.28041355300422, 66.89700205976852],
                     [208.40212503426048, 192.03213822500192, 63.443801234881654]])

CONFIDENCE = 0.3


def check_or_get_file(file_path, file_name):
    if not os.path.isfile(file_path):
        print("[DOWNLOADING] to file_path")
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


def np_to_json_v2(obj):
    """Searialize numpy.ndarray obj"""
    return {'frame': obj.tostring().decode("latin1"),
            'dtype': obj.dtype.str,
            'shape': obj.shape}


def np_from_json_v2(obj):
    """Desearialize numpy.ndarray obj"""
    return np.frombuffer(obj['frame'].encode("latin1"),
                         dtype=np.dtype(obj['dtype'])
                         ).reshape(obj['shape'])