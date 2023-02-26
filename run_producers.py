#!/usr/bin/env python3

import time

from src.frame_producer import StreamVideo
from src.params import *
from src.utils import get_video_feed_url

# from web import app

"""--------------STREAMING--------------"""
# GET IPs OF CAMERAS, you can have your own function to get urls of video streams
CAMERA_URLS = [get_video_feed_url(i, folder="tracking") for i in CAMERAS]
CAMERA_URLS = ["./demo_videos/demo_1.mp4"]

# Init StreamVideo processes, these publish frames from respective camera to the same topic
PRODUCERS = [StreamVideo(url, FRAME_TOPIC, SET_PARTITIONS,
                         use_cv2=USE_RAW_CV2_STREAMING,
                         verbose=True,
                         pub_obj_key=ORIGINAL_PREFIX,
                         rr_distribute=ROUND_ROBIN) for url in
             CAMERA_URLS]

if __name__ == "__main__":
    # Start Publishing frames from cameras to the frame topic
    for p in PRODUCERS:
        p.start()

    """--------------WEB APP--------------"""

    print("[MAIN]", CAMERA_URLS)

    # start the UI
    # app.run(host="0.0.0.0", debug=False, threaded=True, port=3334)

    # wait for producer processes to end
    for p in PRODUCERS:
        p.join()
