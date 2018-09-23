from kafka import KafkaConsumer, KafkaProducer
import json
import time
import cv2
from utils import *
from frame_producer_v2 import FRAME_TOPIC

# check or get model from s3--> cloudfront --> download
check_or_get_file(MODEL_PATH, MODEL_NAME)
check_or_get_file(PROTO_PATH, PROTO_NAME)

# load our serialized model from disk
print("[INFO] loading model...")
MODEL = cv2.dnn.readNetFromCaffe(PROTO_PATH, MODEL_PATH)

# KAFKA TODO: Check kafka compression, multiple consumer, threads safe producer


# Connect to kafka, Consume frame obj bytes deserialize to json
frame_consumer = KafkaConsumer(FRAME_TOPIC, group_id='predict',
                               bootstrap_servers=['0.0.0.0:9092'],
                               fetch_max_bytes=15728640,
                               max_partition_fetch_bytes=15728640,
                               key_deserializer=lambda key: key.decode(),
                               value_deserializer=lambda value: json.loads(value.decode()))

#  connect to Kafka
prediction_producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                                    value_serializer=lambda value: json.dumps(value).encode())

prediction_topic_prefix = 'predicted_objs'


def get_prediction_object(frame_obj):
    """Processes value produced by producer, returns prediction with png image."""

    frame = np_from_json(frame_obj)  # frame_obj = json
    # grab the frame dimensions and convert it to a blob
    (h, w) = frame.shape[:2]
    blob = cv2.dnn.blobFromImage(cv2.resize(frame, (300, 300)),
                                 0.007843, (300, 300), 127.5)
    # blob = cv2.dnn.blobFromImage(frame,
    #                              0.007843, (300, 300), 127.5)
    # pass the blob through the network and obtain the detections and
    # predictions
    MODEL.setInput(blob)
    detections = MODEL.forward()

    model_out = None
    max_confidence = 0

    # loop over the detections
    for i in np.arange(0, detections.shape[2]):
        # extract the confidence (i.e., probability) associated with
        # the prediction
        confidence = detections[0, 0, i, 2]

        # filter out weak detections by ensuring the `confidence` is
        # greater than the minimum confidence
        if confidence > CONFIDENCE:
            # extract the index of the class label from the
            # `detections`, then compute the (x, y)-coordinates of
            # the bounding box for the object
            idx = int(detections[0, 0, i, 1])
            box = detections[0, 0, i, 3:7] * np.array([w, h, w, h])
            (startX, startY, endX, endY) = box.astype("int")

            # draw the prediction on the frame
            label = "{}: {:.2f}%".format(CLASSES[idx],
                                         confidence * 100)

            if confidence > max_confidence:
                model_out = label

            cv2.rectangle(frame, (startX, startY), (endX, endY),
                          COLORS[idx], 2)
            y = startY - 15 if startY - 15 > 15 else startY + 15

            cv2.putText(frame, label, (startX, y),
                        cv2.FONT_HERSHEY_SIMPLEX, 0.5, COLORS[idx], 2)

    # frame = cv2.resize(frame, (150, 150))
    frame_dict = np_to_json(frame.astype(np.uint8))
    result = {"prediction": str(model_out),
              "predict_time": str(time.time()),
              "latency": str(time.time() - int(frame_obj['timestamp']))}

    print(result)
    frame_obj.update(frame_dict)  # update frame with boundaries
    result.update(frame_obj)
    return result


def process_stream(msg_stream):
    try:
        while True:
            try:
                msg = next(msg_stream)
                result = get_prediction_object(msg.value)
                print("timestamp: {}, frame_num: {},camera_num: {}, latency: {}, y_hat: {}".format(result['timestamp'],
                                                                                                   result['frame_num'],
                                                                                                   result['camera'],
                                                                                                   result['latency'],
                                                                                                   result['prediction']
                                                                                                   ))
                # camera specific topic
                prediction_topic = "{}_{}".format(prediction_topic_prefix, result['camera'])
                prediction_producer.send(prediction_topic, value=result)

            except StopIteration as e:
                print(e)
                continue

    except KeyboardInterrupt as e:
        print(e)
        pass

    finally:
        print("Closing Stream")
        msg_stream.close()


if __name__ == '__main__':
    process_stream(frame_consumer)

