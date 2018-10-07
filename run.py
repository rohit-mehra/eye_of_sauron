#!/usr/bin/env python3

import time

from frame_producer import StreamVideo
from params import *
from utils import clear_frame_topic, clear_known_face_topic, get_video_feed_url
from web import app

# Clear Broadcast topic, new query images to be used.
clear_known_face_topic()
# Clear frame topic
clear_frame_topic(FRAME_TOPIC)
# Wait
time.sleep(3)

"""--------------STREAMING--------------"""
# GET IPs OF CAMERAS
CAMERA_URLS = [get_video_feed_url(i, folder="tracking") for i in CAMERAS]

# Init StreamVideo processes, these publish frames from respective camera to the same topic
PRODUCERS = [StreamVideo(url, FRAME_TOPIC, SET_PARTITIONS,
                         use_cv2=USE_RAW_CV2_STREAMING,
                         verbose=True,
                         pub_obj_key=ORIGINAL_PREFIX) for url in
             CAMERA_URLS]

for p in PRODUCERS:
    p.start()

"""--------------WEB APP--------------"""
print("[MAIN]", CAMERA_URLS)

app.run(host='0.0.0.0', debug=False, threaded=True, port=3333)

for p in PRODUCERS:
    p.join()
