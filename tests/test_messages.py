import unittest
from src.frame_producer import StreamVideo
from src.prediction_producer import ConsumeFrames
from src.params import *
import cv2


class TestStreamVideo(unittest.TestCase):
    def setUp(self):
        self.test_video_uri = "../data/test.mp4"
        self.producer = StreamVideo(self.test_video_uri, FRAME_TOPIC, SET_PARTITIONS,
                                    use_cv2=USE_RAW_CV2_STREAMING,
                                    verbose=True,
                                    pub_obj_key=ORIGINAL_PREFIX,
                                    rr_distribute=ROUND_ROBIN)

    def TestStreamVideoMessages(self):
        video = cv2.VideoCapture(self.test_video_uri)
        frame_num = 0
        while True:
            success, image = video.read()
            if not success or frame_num > 2:
                break

            message = self.producer.transform(frame=image,
                                              frame_num=frame_num,
                                              object_key="test",
                                              camera=self.producer.camera_num,
                                              verbose=self.producer.verbose)

            # Check required keys in the message
            self.assertIn("test_frame", message)
            self.assertIn("test_dtype", message)
            self.assertIn("test_shape", message)

            self.assertIn("frame_num", message)
            self.assertIn("camera", message)
            self.assertIn("timestamp", message)

            # check if the frame is encoded as string or not
            self.assertIsInstance(message["test_frame"], str)

            frame_num += 1


class TestConsumeFrames(unittest.TestCase):
    def setUp(self):
        self.test_video_uri = "../data/test.mp4"
        self.test_frame = "../data/test_frame.jpg"
        self.producer = StreamVideo(self.test_video_uri, FRAME_TOPIC, SET_PARTITIONS,
                                    use_cv2=USE_RAW_CV2_STREAMING,
                                    verbose=True,
                                    pub_obj_key=ORIGINAL_PREFIX,
                                    rr_distribute=ROUND_ROBIN)

        self.preprocessor = ConsumeFrames(frame_topic="test",
                                          processed_frame_topic="test")

    def TestConsumeFramesMessages(self):
        image = cv2.imread(self.test_frame)
        frame_num = 0

        message = self.producer.transform(frame=image,
                                          frame_num=frame_num,
                                          object_key="test",
                                          camera=self.producer.camera_num,
                                          verbose=self.producer.verbose)

        message = self.preprocessor.get_processed_frame_object(message)

        # Check required keys in the message
        self.assertIn("test_frame", message)
        self.assertIn("test_dtype", message)
        self.assertIn("test_shape", message)

        self.assertIn("face_locations_frame", message)
        self.assertIn("face_locations_dtype", message)
        self.assertIn("face_locations_shape", message)

        self.assertIn("face_encodings_frame", message)
        self.assertIn("face_encodings_dtype", message)
        self.assertIn("face_encodings_shape", message)

        self.assertIn("frame_num", message)
        self.assertIn("camera", message)
        self.assertIn("timestamp", message)

        # check if the frame is encoded as string or not
        self.assertIsInstance(message["test_frame"], str)


if __name__ == '__main__':
    unittest.main()
