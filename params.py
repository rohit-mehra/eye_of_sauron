"""Set Parameters for the APP here...."""
import numpy as np
import os

file_dir = os.path.dirname(os.path.realpath(__file__))

# TOPIC USED TO PUBLISH ALL FRAME OBJECTS
FRAME_TOPIC = 'frame_objects_v2'
# PREDICTION TOPIC PREFIX, EACH CAMERA GETS NEW TOPIC FOR PUBLISHED PREDICTIONS
PREDICTION_TOPIC_PREFIX = 'predicted_objects'
# ENDPOINT FOR VIDEO STREAMS
C_FRONT_ENDPOINT = "http://d3tj01z94i74qz.cloudfront.net/"
# TOTAL CAMERAS TO BE USED --> USED FOR FULL URL
TOTAL_CAMERAS = 4
# FPS OF STREAM --> USED FOR FULL URL
FPS = 30

# USE RAW CV2 STREAMING or FAST BUT LESS FRAMES

USE_RAW_CV2_STREAMING = False

# IF THE VIDEO IS GRAY, MNIST WAS USED TO TEST STREAMING --> USED TO SET FRAME DIMENSIONS TO (28, 28)
GRAY = False
# IF THE MODEL USED IS CAFFEE --> SET FRAME DIMENSIONS TO (300, 300)
CAFFEE = True
# TOPIC PARTITIONS
SET_PARTITIONS = 4
# SETTING UP TOPIC WITH DESIRED PARTITIONS
INIT_CMD = "/usr/local/kafka/bin/kafka-topics.sh --create --zookeeper localhost:2181 " \
           "--replication-factor 3 --partitions {} --topic {}".format(SET_PARTITIONS, FRAME_TOPIC)
# SETTING UP TOPIC WITH DESIRED PARTITIONS
ALTER_CMD = "/usr/local/kafka/bin/kafka-topics.sh --alter --zookeeper localhost:2181 " \
            "--topic frame_objects_v2 --partitions {}".format(SET_PARTITIONS)

# OBJECT DETECTION MODEL NAME/LOCATION
MODEL_NAME = "mobilenet_v2.caffemodel"  # ""bvlc_googlenet.caffemodel"
PROTO_NAME = "mobilenet_v2_deploy.prototxt"  # "bvlc_googlenet.prototxt"
LABEL_NAME = "synset_words.txt"

MODEL_PATH = file_dir + "/models/{}".format(MODEL_NAME)
PROTO_PATH = file_dir + "/models/{}".format(PROTO_NAME)
LABEL_PATH = file_dir + "/models/{}".format(LABEL_NAME)

TARGET = "person"

# CLASS LABELS ML MODEL(MobileNet SSD) WAS TRAINED ON
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
# RGB COLOR FOR EACH CLASS
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

# MINIMUM CONFIDENCE TO CONSIDER A CLASS
CONFIDENCE = 0.5

ORIGINAL_PREFIX = 'predicted'
PREDICTED_PREFIX = 'predicted'
