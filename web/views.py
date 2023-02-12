import json
import shutil
import threading
from collections import defaultdict

from flask import Response, render_template, request, url_for, redirect, session
from flask_dropzone import Dropzone
from flask_uploads import UploadSet, configure_uploads, IMAGES, patch_request_class
from kafka import KafkaConsumer, KafkaProducer

from src.params import *
from src.utils import populate_buffer, consume_buffer, consumer, np_to_json, clear_prediction_topics
from web import app

# Clear prediction topic, new view, new app instance.
# clear_prediction_topics()

# Buffer properties
BUFFER_SIZE = 0
BUFFER_DICT = defaultdict(list)
DATA_DICT = defaultdict(dict)
BUFFER_THREADS = dict()
EVENT_THREADS = dict()
THREADED_BUFFER_CONCEPT = False  # Use

# UPLOAD SETTINGS
save_dir = os.getcwd() + "/data/faces"

if os.path.isdir(save_dir):
    shutil.rmtree(save_dir)
    os.makedirs(save_dir)
else:
    os.makedirs(save_dir)

print(save_dir)

# APP SETTINGS
dropzone = Dropzone(app)
app.config["SECRET_KEY"] = "dupersecretkeygoeshere"
app.config["DROPZONE_UPLOAD_MULTIPLE"] = True
app.config["DROPZONE_ALLOWED_FILE_CUSTOM"] = True
app.config["DROPZONE_ALLOWED_FILE_TYPE"] = "image/*"
app.config["DROPZONE_REDIRECT_VIEW"] = "results"
app.config["UPLOADED_PHOTOS_DEST"] = save_dir
photos = UploadSet("photos", IMAGES)
configure_uploads(app, photos)
patch_request_class(app)  # set maximum file size, default is 16MB

# INSTANTIATE BROADCAST TOPIC, USED TO SEND QUERY FACE MESSAGES
broadcast_known_faces = KafkaProducer(bootstrap_servers=["localhost:9092"],
                                      value_serializer=lambda value: json.dumps(value).encode())


# Individual camera stream
@app.route("/cam/<cam_num>")
def cam(cam_num):
    # return a multipart response
    # if THREADED_BUFFER_CONCEPT:
    #     return Response(
    #             consume_buffer(int(cam_num), BUFFER_DICT, DATA_DICT, EVENT_THREADS, LOCK, buffer_size=BUFFER_SIZE),
    #             mimetype="multipart/x-mixed-replace; boundary=frame")

    return Response(consumer(int(cam_num), BUFFER_DICT, DATA_DICT, buffer_size=BUFFER_SIZE),
                    mimetype="multipart/x-mixed-replace; boundary=frame")


# Render individual camera streams in one view
@app.route("/")
def index():
    return render_template("videos.html", camera_numbers=list(range(1, TOTAL_CAMERAS + 1)))
