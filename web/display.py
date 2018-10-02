import json
import sys
import threading
from collections import defaultdict

from flask import Flask, Response, render_template, request, url_for, redirect, session
from flask_dropzone import Dropzone
from flask_uploads import UploadSet, configure_uploads, IMAGES, patch_request_class
from kafka import KafkaConsumer


sys.path.insert(0, '..')
from utils import populate_buffer, consume_buffer, consumer
from params import *

# Continuously listen to the connection and print messages as received
app = Flask(__name__)

BUFFER_SIZE = 180
BUFFER_DICT = defaultdict(list)
DATA_DICT = defaultdict(dict)
BUFFER_THREADS = dict()
EVENT_THREADS = dict()
LOCK = threading.Lock()
BUFFER = False

"""gunicorn -w 20 -b 0.0.0.0:5000 display:app"""

print(os.getcwd() + '/uploads')

dropzone = Dropzone(app)
# Drop zone settings
app.config['DROPZONE_UPLOAD_MULTIPLE'] = True
app.config['DROPZONE_ALLOWED_FILE_CUSTOM'] = True
app.config['DROPZONE_ALLOWED_FILE_TYPE'] = 'image/*'
app.config['DROPZONE_REDIRECT_VIEW'] = 'results'
app.config['SECRET_KEY'] = 'rrqqsupersecretkey'

# Uploads settings
app.config['UPLOADED_PHOTOS_DEST'] = os.getcwd() + '/uploads'
photos = UploadSet('photos', IMAGES)
configure_uploads(app, photos)
patch_request_class(app, size=16)  # set maximum file size to 16MB


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
    # set session for image results
    if "file_urls" not in session:
        session['file_urls'] = []
    # list to hold our uploaded image urls
    file_urls = session['file_urls']
    # handle image upload from Dropzone
    if request.method == 'POST':
        file_obj = request.files
        for f in file_obj:
            file = request.files.get(f)

            # save the file with to our photos folder
            filename = photos.save(file, name=file.filename)
            # append image urls
            file_urls.append(photos.url(filename))

        session['file_urls'] = file_urls
        print(file_urls)
        return "Uploading..."
        # camera_numbers = int(request.form['camera_numbers'])
        # return redirect(url_for('get_cameras', camera_numbers=camera_numbers), code=302)

    # show the form, if wasn't submitted
    return render_template('index.html')


@app.route('/results')
def results():
    # redirect to home if no images to display
    if "file_urls" not in session or session['file_urls'] == []:
        return redirect(url_for('index'))

    # set the file_urls and remove the session variable
    file_urls = session['file_urls']
    session.pop('file_urls', None)

    return render_template('results.html', file_urls=file_urls)


if __name__ == "__main__":

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

    app.run(host='0.0.0.0', debug=False, threaded=True, port=3333)
    print("Done....")
