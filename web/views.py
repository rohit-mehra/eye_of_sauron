import json
import shutil
import sys
import threading
from collections import defaultdict

import cv2
import face_recognition
from flask import Response, render_template, request, url_for, redirect, session
from flask_dropzone import Dropzone
from flask_uploads import UploadSet, configure_uploads, IMAGES, patch_request_class
from kafka import KafkaConsumer, KafkaProducer

from web import app

sys.path.insert(0, '..')
from utils import populate_buffer, consume_buffer, consumer, np_to_json
from params import *

BUFFER_SIZE = 180
BUFFER_DICT = defaultdict(list)
DATA_DICT = defaultdict(dict)
BUFFER_THREADS = dict()
EVENT_THREADS = dict()
BUFFER = False

save_dir = os.getcwd() + '/data/query_faces'

if os.path.isdir(save_dir):
    shutil.rmtree(save_dir)
    os.makedirs(save_dir)
else:
    os.makedirs(save_dir)

print(save_dir)

dropzone = Dropzone(app)

app.config['SECRET_KEY'] = 'supersecretkeygoeshere'

# Dropzone settings
app.config['DROPZONE_UPLOAD_MULTIPLE'] = True
app.config['DROPZONE_ALLOWED_FILE_CUSTOM'] = True
app.config['DROPZONE_ALLOWED_FILE_TYPE'] = 'image/*'
app.config['DROPZONE_REDIRECT_VIEW'] = 'results'

# Uploads settings
app.config['UPLOADED_PHOTOS_DEST'] = os.getcwd() + '/data/query_faces'

photos = UploadSet('photos', IMAGES)
configure_uploads(app, photos)
patch_request_class(app)  # set maximum file size, default is 16MB

broadcast_known_faces = KafkaProducer(bootstrap_servers=['localhost:9092'],
                                      value_serializer=lambda value: json.dumps(value).encode())


@app.route('/cam/<cam_num>')
def cam(cam_num):
    # return a multipart response
    if BUFFER:
        return Response(consume_buffer(int(cam_num), BUFFER_DICT, DATA_DICT, EVENT_THREADS, LOCK),
                        mimetype='multipart/x-mixed-replace; boundary=frame')

    return Response(consumer(int(cam_num), BUFFER_DICT, DATA_DICT),
                    mimetype='multipart/x-mixed-replace; boundary=frame')


@app.route('/cameras/<camera_numbers>')
def get_cameras(camera_numbers):
    return render_template("videos.html", camera_numbers=list(range(1, 1 + int(camera_numbers))))


@app.route('/', methods=['GET', 'POST'])
def index():
    # Start Point, Empty Directory
    shutil.rmtree(save_dir)
    os.makedirs(save_dir)

    # set session for image results
    if "file_urls" not in session:
        session['file_urls'] = []
        session['known_face_names'] = []

    # list to hold our uploaded image urls
    file_urls = session['file_urls']
    known_face_names = session['known_face_names']
    known_face_encodings = []
    image_file_names = []

    # handle image upload from Dropszone
    if request.method == 'POST':
        file_obj = request.files

        # COLLECT TARGET FACE NAMES AND ENCODINGS
        for f in file_obj:
            file = request.files.get(f)

            # save the file with to our photos folder
            filename = photos.save(
                file,
                name=file.filename
            )
            image_file_names.append(filename)

            file_path = "{}/{}".format(save_dir, filename)
            # Load a picture and learn how to recognize it.
            image = face_recognition.load_image_file(file_path)

            # Get encoding, out of all the first one, assumption just one face in the query image
            image_encoding = face_recognition.face_encodings(image)[0]
            # append image encoding
            known_face_encodings.append(image_encoding)
            # append image name
            known_face_names.append(filename[:filename.index(".")].title())
            # append image url
            file_urls.append(photos.url(filename))

        # print(np.array(known_face_encodings), np.array(known_face_names))
        # print(np.array(known_face_encodings).shape, np.array(known_face_names).shape)

        # TODO: BROADCAST THE TARGET TO LOOK FOR:
        broadcast_message = np_to_json(np.array(known_face_encodings), prefix_name="known_face_encodings")
        broadcast_message.update(np_to_json(np.array(known_face_names), prefix_name="known_face_names"))
        broadcast_known_faces.send(KNOWN_FACE_TOPIC, value=broadcast_message)

        # DISPLAY
        face_locations = []  # collect found face location
        face_encodings = []
        face_names = []  # collect found face names

        # loop over uploaded images, in reality loop over test images or frames
        for filename in image_file_names:

            file_path = "{}/{}".format(save_dir, filename)
            image = face_recognition.load_image_file(file_path)

            # Find all the faces and face encodings in the current frame of video
            face_locations = face_recognition.face_locations(image)
            face_encodings = face_recognition.face_encodings(image, face_locations)

            # faces found in this image
            face_names = []
            for face_encoding in face_encodings:
                # See if the face is a match for the known face(s)
                matches = face_recognition.compare_faces(known_face_encodings, face_encoding)
                name = "Unknown"

                # If a match was found in known_face_encodings, just use the first one.
                if True in matches:
                    first_match_index = matches.index(True)
                    name = known_face_names[first_match_index]

                face_names.append(name.title())

            # SAVE the results for this frame
            for (top, right, bottom, left), name in zip(face_locations, face_names):
                # Draw a box around the face
                cv2.rectangle(image, (left, top), (right, bottom), (0, 0, 255), 2)

                # Draw a label with a name below the face
                cv2.rectangle(image, (left, bottom - 27), (right, bottom), (0, 0, 255), cv2.FILLED)
                font = cv2.FONT_HERSHEY_DUPLEX
                cv2.putText(image, name, (left + 6, bottom - 6), font, 1.0, (255, 255, 255), 1)

            cv2.imwrite(file_path, cv2.cvtColor(image, cv2.COLOR_RGB2BGR))

        session['file_urls'] = file_urls
        session['known_face_names'] = known_face_names
        return "uploading..."

    # show the form, if wasn't submitted
    return render_template('index.html')


@app.route('/results', methods=['GET', 'POST'])
def results():
    if request.method == 'POST':
        camera_numbers = int(request.form['camera_numbers'])
        return redirect(url_for('get_cameras', camera_numbers=camera_numbers), code=302)

    # redirect to home if no images to display
    if "file_urls" not in session or session['file_urls'] == []:
        return redirect(url_for('index'))

    # set the file_urls and remove the session variable
    file_urls = session['file_urls']
    known_face_names = session['known_face_names']
    session.pop('file_urls', None)
    session.pop('known_face_names', None)

    return render_template('results.html', file_urls_names=zip(file_urls, known_face_names))


"""----------------------------CONSUMPTION----------------------------"""

if BUFFER:

    cam_nums = [i for i in range(1, TOTAL_CAMERAS + 1)]
    prediction_topics = {cam_num: "{}_{}".format(PREDICTION_TOPIC_PREFIX, cam_num) for cam_num in cam_nums}

    print('\n', prediction_topics)

    prediction_consumers = {cam_num: KafkaConsumer(topic, group_id='view',
                                                   bootstrap_servers=['0.0.0.0:9092'],
                                                   auto_offset_reset='earliest',
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
                                                            EVENT_THREADS])
        BUFFER_THREADS[cam_num] = bt
        bt.start()

print("Done....")
