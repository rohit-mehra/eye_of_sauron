#!/usr/bin/env python3

import time

from frame_producer import StreamVideo
from params import *
from utils import clear_topic, set_topic, get_video_feed_url
from web import app

# Clear Broadcast topic, new query images to be used.
clear_topic(KNOWN_FACE_TOPIC)
# Clear raw frame topic
clear_topic(FRAME_TOPIC)
# Clear processed frame topic
clear_topic(PROCESSED_FRAME_TOPIC)

# set partitions
set_topic(FRAME_TOPIC, SET_PARTITIONS)
set_topic(PROCESSED_FRAME_TOPIC, SET_PARTITIONS)
# Wait
time.sleep(3)

"""--------------STREAMING--------------"""
# GET IPs OF CAMERAS
CAMERA_URLS = [get_video_feed_url(i, folder="tracking") for i in CAMERAS]

# Init StreamVideo processes, these publish frames from respective camera to the same topic
PRODUCERS = [StreamVideo(url, FRAME_TOPIC, SET_PARTITIONS,
                         use_cv2=USE_RAW_CV2_STREAMING,
                         verbose=True,
                         pub_obj_key=ORIGINAL_PREFIX,
                         rr_distribute=ROUND_ROBIN) for url in
             CAMERA_URLS]

# Start Publishing frames from cameras to the frame topic
for p in PRODUCERS:
    p.start()

"""--------------WEB APP--------------"""
print("[MAIN]", CAMERA_URLS)

# start the UI
app.run(host="0.0.0.0", debug=False, threaded=True, port=3333)

# wait for producer processes to end
for p in PRODUCERS:
    p.join()
