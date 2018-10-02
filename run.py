#!/usr/bin/env python3

from web import app
from utils import clear_frame_topic, clear_prediction_topics, clear_known_face_topic
from params import *

clear_frame_topic(DL)
clear_known_face_topic()
# clear_prediction_topics()

app.run(host='0.0.0.0', debug=False, threaded=True, port=3333)
