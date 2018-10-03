"""Set Parameters for the APP here...."""

import os


file_dir = os.path.dirname(os.path.realpath(__file__))

# BROADCAST KNOWN FACE ENCODINGS OR PEOPLE TO FIND
KNOWN_FACE_TOPIC = "target_faces"
# TOPIC USED TO PUBLISH ALL FRAME OBJECTS
FRAME_TOPIC = "tracking"
# PREDICTION TOPIC PREFIX, EACH CAMERA GETS NEW TOPIC FOR PUBLISHED PREDICTIONS
PREDICTION_TOPIC_PREFIX = "{}_{}".format('predicted_objects', FRAME_TOPIC)
# ENDPOINT FOR VIDEO STREAMS
C_FRONT_ENDPOINT = "http://d3tj01z94i74qz.cloudfront.net/"
# CAMERA URL INDEXES
CAMERAS = [2,3]
# TOTAL CAMERAS TO BE USED --> USED FOR FULL URL
TOTAL_CAMERAS = len(CAMERAS)


# USE RAW CV2 STREAMING or FAST BUT LESS FRAMES
USE_RAW_CV2_STREAMING = False

# TOPIC PARTITIONS
SET_PARTITIONS = 4
# SETTING UP TOPIC WITH DESIRED PARTITIONS
INIT_CMD = "/usr/local/kafka/bin/kafka-topics.sh --create --zookeeper localhost:2181 " \
           "--replication-factor 3 --partitions {} --topic {}".format(SET_PARTITIONS, FRAME_TOPIC)

# SETTING UP TOPIC WITH DESIRED PARTITIONS
ALTER_CMD = "/usr/local/kafka/bin/kafka-topics.sh --alter --zookeeper localhost:2181 " \
            "--topic frame_objects_v2 --partitions {}".format(SET_PARTITIONS)

ORIGINAL_PREFIX = 'predicted'
PREDICTED_PREFIX = 'predicted'
