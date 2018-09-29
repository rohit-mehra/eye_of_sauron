from kafka import KafkaConsumer, KafkaProducer
import json
import time
import cv2
from utils import np_from_json, np_to_json
from utils import check_or_get_file
from params import *
from frame_producer_v3 import FRAME_TOPIC
import socket
from multiprocessing import Pool


def consumer(number):
    """CONSUME video frames, predictions Published to respective camera topics
    Args:
        number: consumer number
    """
    # Declare unique client name
    iam = "{}-{}".format(socket.gethostname(), number)
    print("[INFO] I am ", iam)

    # LABEL NAMES
    label_names = np.loadtxt(LABEL_PATH, str, delimiter='\t')

    # load our serialized model from disk
    print("[INFO] loading model...")
    model = cv2.dnn.readNetFromCaffe(PROTO_PATH, MODEL_PATH)
    print("[INFO] Loaded...")

    # KAFKA TODO: Check kafka compression, multiple consumer, threads safe producer

    # Connect to kafka, Consume frame obj bytes deserialize to json
    frame_consumer = KafkaConsumer(FRAME_TOPIC, group_id='predict', client_id=iam,
                                   bootstrap_servers=['0.0.0.0:9092'],
                                   key_deserializer=lambda key: key.decode(),
                                   value_deserializer=lambda value: json.loads(value.decode()))

    #  connect to Kafka, produces prediction object
    prediction_producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                                        key_serializer=lambda key: str(key).encode(),
                                        value_serializer=lambda value: json.dumps(value).encode())

    def get_classification_object(frame_obj):
        """Processes value produced by producer, returns prediction with png image."""

        frame = np_from_json(frame_obj, prefix_name=ORIGINAL_PREFIX)  # frame_obj = json
        # This CNN requires fixed spatial dimensions for our input image(s)
        # so we need to ensure it is resized to 224x224 pixels while
        # performing mean subtraction (104, 117, 123) to normalize the input;
        # after executing this command our "blob" now has the shape:
        # (1, 3, 224, 224)
        # blob = cv2.dnn.blobFromImage(frame, 1, (224, 224), (104, 117, 123))
        # MOBILE NET
        blob = cv2.dnn.blobFromImage(frame, 0.017, (224, 224), (103.94, 116.78, 123.68), swapRB=True)

        # pass the blob through the network and obtain the detections and
        # predictions
        model.setInput(blob)
        pred_start = time.time()
        predictions = model.forward()
        print("Prediction time: ", time.time() - pred_start)

        # MOBILE NET
        predictions = np.squeeze(predictions)
        idx = np.argsort(-predictions)
        label_name = None

        for i in range(5):
            label = idx[i]
            label_name = label_names[label]
            confidence = predictions[label]
            # print('%.2f - %s' % (confidence, label_name))
            if i == 0 and confidence > CONFIDENCE:
                # TODO: DISPLAY IF ITS LABEL OF INTEREST
                text = "Detected: {}, {:.2f}%".format(label_name,
                                                      confidence * 100)
                cv2.putText(frame, text, (5, 25), cv2.FONT_HERSHEY_SIMPLEX,
                            0.7, (0, 0, 255), 2)
                break

        # frame = cv2.resize(frame, (150, 150))
        frame_dict = np_to_json(frame.astype(np.uint8), prefix_name=PREDICTED_PREFIX)

        result = {"prediction": str(label_name),
                  "predict_time": str(time.time()),
                  "latency": str(time.time() - int(frame_obj['timestamp']))}

        frame_obj.update(frame_dict)  # update frame with prediction

        result.update(frame_obj)

        return result

    def process_stream(msg_stream):
        try:
            while True:
                try:
                    msg = next(msg_stream)
                    result = get_classification_object(msg.value)
                    print("timestamp: {}, frame_num: {},camera_num: {}, latency: {}, y_hat: {}".format(
                        result['timestamp'],
                        result['frame_num'],
                        result['camera'],
                        result['latency'],
                        result['prediction']
                        ))
                    # camera specific topic
                    prediction_topic = "{}_{}".format(PREDICTION_TOPIC_PREFIX, result['camera'])
                    prediction_producer.send(prediction_topic, key=result['frame_num'], value=result)

                except StopIteration as e:
                    print(e)
                    continue

        except KeyboardInterrupt as e:
            print(e)
            pass

        finally:
            print("Closing Stream")
            msg_stream.close()

    process_stream(frame_consumer)

    return True


if __name__ == '__main__':
    # frame_consumer.assign([TopicPartition(FRAME_TOPIC, 3)])
    # print(frame_consumer.assignment())
    # process_stream(frame_consumer)
    # check or get model from s3--> cloud front --> download
    check_or_get_file(MODEL_PATH, MODEL_NAME)
    check_or_get_file(PROTO_PATH, PROTO_NAME)
    check_or_get_file(LABEL_PATH, LABEL_NAME)

    THREADS = 2 if SET_PARTITIONS == 8 else 1
    NUMBERS = [i for i in range(THREADS)]
    consumer_pool = Pool(THREADS)
    try:
        statuses = consumer_pool.map(consumer, NUMBERS)
        consumer_pool.close()  # close pool
        consumer_pool.join()  # wait to join
    except KeyboardInterrupt as e:
        print(e)
        consumer_pool.terminate()
        print("Done....")
