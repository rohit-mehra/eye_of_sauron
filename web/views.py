import json
import shutil
import threading
from collections import defaultdict

import cv2
import face_recognition
import numpy as np
from flask import Response, render_template, request, url_for, redirect, session
from flask_dropzone import Dropzone
from flask_uploads import UploadSet, configure_uploads, IMAGES, patch_request_class
from kafka import KafkaConsumer, KafkaProducer

from src.params import *
from src.utils import populate_buffer, consume_buffer, consumer, np_to_json, clear_prediction_topics
from web import app

# Clear prediction topic, new view, new app instance.
clear_prediction_topics()

# Buffer properties
BUFFER_SIZE = 600
BUFFER_DICT = defaultdict(list)
DATA_DICT = defaultdict(dict)
BUFFER_THREADS = dict()
EVENT_THREADS = dict()
THREADED_BUFFER_CONCEPT = True  # Use

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
    if THREADED_BUFFER_CONCEPT:
        return Response(
            consume_buffer(int(cam_num), BUFFER_DICT, DATA_DICT, EVENT_THREADS, LOCK, buffer_size=BUFFER_SIZE),
            mimetype="multipart/x-mixed-replace; boundary=frame")

    return Response(consumer(int(cam_num), BUFFER_DICT, DATA_DICT, buffer_size=BUFFER_SIZE),
                    mimetype="multipart/x-mixed-replace; boundary=frame")


# Render individual camera streams in one view
@app.route("/cameras/<camera_numbers>")
def get_cameras(camera_numbers):
    return render_template("videos.html", camera_numbers=list(range(1, 1 + int(camera_numbers))))


# Index to upload query faces
@app.route("/", methods=["GET", "POST"])
def index():
    # set session for image results
    session["file_urls"] = [] if "file_urls" not in session else session["file_urls"]
    session["known_faces"] = [] if "known_faces" not in session else session["known_faces"]
    session["known_face_encodings"] = [] if "known_face_encodings" not in session else session["known_face_encodings"]
    session["image_file_names"] = [] if "image_file_names" not in session else session["image_file_names"]

    # list to hold our uploaded image urls
    file_urls = session["file_urls"]
    known_faces = session["known_faces"]
    known_face_encodings = session["known_face_encodings"]
    image_file_names = session["image_file_names"]

    # handle image upload from Dropszone
    if request.method == "POST":

        file_obj = request.files

        # COLLECT TARGET FACE NAMES AND ENCODINGS
        for f in file_obj:
            file = request.files.get(f)

            # save the file with to our photos folder
            filename = photos.save(
                file,
                name=file.filename
            )

            # append image name
            image_file_names.append(filename)
            # append image url
            file_urls.append(photos.url(filename))

            file_path = "{}/{}".format(save_dir, filename)
            # Load a picture and learn how to recognize it.
            image = face_recognition.load_image_file(file_path)

            # Get encoding, out of all the first one, assumption just one face in the query image
            image_encoding = face_recognition.face_encodings(image, num_jitters=3)[0]

            # append image encoding in string
            known_face_encodings.append(json.dumps(image_encoding.tolist()))

            # get face name from filename
            dot_idx = filename.index(".")
            try:
                underscore_idx = filename.index("_")
            except ValueError:
                underscore_idx = len(filename)

            to_idx = dot_idx if dot_idx < underscore_idx else underscore_idx
            face_name = filename[:to_idx]

            known_faces.append(face_name.title())

        session["file_urls"] = file_urls
        session["known_faces"] = known_faces
        session["known_face_encodings"] = known_face_encodings
        session["image_file_names"] = image_file_names

        return "Uploading..."

    # show the form, if wasn"t submitted
    return render_template("index.html")


@app.route("/results", methods=["GET", "POST"])
def results():
    if request.method == "POST":
        camera_numbers = int(request.form["camera_numbers"])
        return redirect(url_for("get_cameras", camera_numbers=camera_numbers), code=302)

    # redirect to home if no images to display
    if "file_urls" not in session or session["file_urls"] == []:
        return redirect(url_for("index"), code=302)

    # redirect to home if no images to display
    if "known_faces" not in session or session["known_faces"] == []:
        return redirect(url_for("index"), code=302)

    # set the file_urls and remove the session variable
    file_urls = session["file_urls"]
    known_faces = session["known_faces"]
    known_face_encodings = [np.array(json.loads(kfe)) for kfe in session["known_face_encodings"]]
    image_file_names = session["image_file_names"]

    print("\n", known_faces, "\n")
    # BROADCAST THE TARGET TO LOOK FOR:
    broadcast_message = np_to_json(np.array(known_face_encodings),
                                   prefix_name="known_face_encodings")
    broadcast_message.update(np_to_json(np.array(known_faces), prefix_name="known_faces"))
    broadcast_known_faces.send(TARGET_FACE_TOPIC, value=broadcast_message)

    # loop over uploaded images, in reality loop over test images or frames
    for file_name in image_file_names:

        file_path = "{}/{}".format(save_dir, file_name)

        image = face_recognition.load_image_file(file_path)

        # Find all the faces and face encodings in the current frame of video
        face_locations = face_recognition.face_locations(image)
        face_encodings = face_recognition.face_encodings(image, face_locations)

        # faces found in this image
        face_names = []
        for face_encoding in face_encodings:
            # See if the face is a match for the known face(s)
            matches = face_recognition.compare_faces(known_face_encodings, face_encoding, tolerance=0.3)
            name = "Unknown"

            # If a match was found in known_face_encodings, just use the first one.
            if True in matches:
                first_match_index = matches.index(True)
                name = known_faces[first_match_index]

            face_names.append(name.title())

        # draw boxes for this frame
        for (top, right, bottom, left), name in zip(face_locations, face_names):
            # Draw a box around the face
            color = (0, 0, 255)
            cv2.rectangle(image, (left, top), (right, bottom), color, 2)

            # Draw a label with a name below the face
            cv2.rectangle(image, (left, bottom - 21), (right, bottom), color, cv2.FILLED)
            font = cv2.FONT_HERSHEY_SIMPLEX
            cv2.putText(image, name, (left + 6, bottom - 6), font, 1.0, (255, 255, 255), 1)
            break

        cv2.imwrite(file_path, cv2.cvtColor(image, cv2.COLOR_RGB2BGR))

    session.pop("file_urls", None)
    session.pop("known_faces", None)
    session.pop("known_face_encodings", None)
    session.pop("image_file_names", None)
    session.clear()

    return render_template("results.html", file_urls_names=zip(file_urls, known_faces))


# Start Buffer thread
if THREADED_BUFFER_CONCEPT:
    cam_nums = [i for i in range(1, TOTAL_CAMERAS + 1)]
    prediction_topics = {cam_num: "{}_{}".format(PREDICTION_TOPIC_PREFIX, cam_num) for cam_num in cam_nums}
    prediction_consumers = {cam_num: KafkaConsumer(topic, group_id="view",
                                                   bootstrap_servers=["0.0.0.0:9092"],
                                                   auto_offset_reset="earliest",
                                                   value_deserializer=lambda value: json.loads(value.decode()
                                                                                               )) for cam_num, topic in
                            prediction_topics.items()}

    LOCK = threading.Lock()

    for cam_num in cam_nums:
        EVENT_THREADS[cam_num] = threading.Event()

    # THREADS: POPULATE BUFFERS
    for cam_num in cam_nums:
        print("Starting Consumer thread for [CAM] {}".format(cam_num))
        bt = threading.Thread(target=populate_buffer, args=[prediction_consumers[int(cam_num)],
                                                            cam_num,
                                                            BUFFER_DICT,
                                                            DATA_DICT,
                                                            EVENT_THREADS,
                                                            BUFFER_SIZE])
        BUFFER_THREADS[cam_num] = bt
        bt.start()

print("Done....")
