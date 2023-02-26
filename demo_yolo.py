from pathlib import Path

import cv2
import torch
import torch.backends.cudnn as cudnn
from numpy import random

from models.experimental import attempt_load
from utils.general import check_img_size, non_max_suppression, scale_coords, xyxy2xywh
from utils.plots import plot_one_box
from utils.torch_utils import select_device, TracedModel
from utils.datasets import *

# define global _model
_model = ''


# define func load_model
def load_model(path_model):  # update _model
    global _model, device, stride, imgsz
    device = select_device('')
    _model = attempt_load(path_model, map_location=device)

    device = select_device('')

    # Load model
    stride = int(_model.stride.max())  # model stride
    imgsz = check_img_size(640, s=stride)  # check img_size

    _model = TracedModel(_model, device, 640)


def predict(img_raw, view_img=False, img_size=640, imgsz=640, trace=True, augment=False, conf_thres=0.25, iou_thres=0.45, classes=None, agnostic_nms=False, save_conf=False):
    coordinates_bbox = []

    global _model
    model = _model

    # Get names and colors
    names = model.module.names if hasattr(model, 'module') else model.names
    colors = [[random.randint(0, 255) for _ in range(3)] for _ in names]

    # Run inference
    if device.type != 'cpu':
        model(torch.zeros(1, 3, imgsz, imgsz).to(device).type_as(next(model.parameters())))  # run once
    old_img_w = old_img_h = imgsz
    old_img_b = 1

    img_resized = letterbox(img_raw, 640, stride=32)[0]  # Hard-coded

    # Convert
    # img_resized = img_resized[:, :, ::-1].transpose(2, 0, 1)  # BGR to RGB, to 3x416x416
    img_resized = img_resized.transpose(2, 0, 1)  # to 3x416x416
    img_resized = np.ascontiguousarray(img_resized)
    # print(img_resized.shape)

    img_resized = torch.from_numpy(img_resized).to(device)
    img_resized = img_resized.float()  # uint8 to fp16/32
    img_resized /= 255.0  # 0 - 255 to 0.0 - 1.0
    if img_resized.ndimension() == 3:
        img_resized = img_resized.unsqueeze(0)

    # # Warmup
    # if device.type != 'cpu' and (old_img_b != img_resized.shape[0] or old_img_h != img_resized.shape[2] or old_img_w != img_resized.shape[3]):
    #     old_img_b = img_resized.shape[0]
    #     old_img_h = img_resized.shape[2]
    #     old_img_w = img_resized.shape[3]
    #     for i in range(3):
    #         model(img_resized, augment=augment)[0]

    # Inference
    pred = model(img_resized, augment=augment)[0]
    # Apply NMS
    pred = non_max_suppression(pred, conf_thres, iou_thres, classes=classes, agnostic=agnostic_nms)

    # Process detections
    for i, det in enumerate(pred):  # detections per image
        s = ''

        gn = torch.tensor(img_raw.shape)[[1, 0, 1, 0]]  # normalization gain whwh
        if len(det):
            # Rescale boxes from img_size to im0 size
            det[:, :4] = scale_coords(img_resized.shape[2:], det[:, :4], img_raw.shape).round()

            # Print results
            for c in det[:, -1].unique():
                n = (det[:, -1] == c).sum()  # detections per class
                s += f"{n} {names[int(c)]}{'s' * (n > 1)}, "  # add to string

            # Write results
            for *xyxy, conf, cls in reversed(det):
                xywh = (xyxy2xywh(torch.tensor(xyxy).view(1, 4)) / gn).view(-1).tolist()  # normalized xywh
                line = (cls, *xywh, conf) if save_conf else (cls, *xywh)  # label format
                coordinates_bbox.append((('%g ' * len(line)).rstrip() % line).split(' ')[1:])

                label = f'{names[int(cls)]} {conf:.2f}'
                plot_one_box(xyxy, img_raw, label=label, color=colors[int(cls)], line_thickness=1)

    # Stream results
    # if len(coordinates_bbox):
    #     cv2.imshow('hi', img_raw)
    #     cv2.waitKey(0)  # 1 millisecond
    #     cv2.destroyAllWindows()
    # print(img_raw.shape)

    # print('Danh sach bbox: ')
    # print(coordinates_bbox)
    # print(' => Tong co ' + str(len(coordinates_bbox)) + ' heads')
    return img_raw, coordinates_bbox

# ------------
# load_model(r"C:\Users\MSI I5\PycharmProjects\yolov7\best_35_epoch.pt")
#
# # predict(, view_img=True)
# predict(r'C:\Users\MSI I5\PycharmProjects\eye_of_sauron\PartA_01995.jpg', view_img=True)
#
# img_raw = cv2.imread(r'C:\Users\MSI I5\PycharmProjects\eye_of_sauron\PartA_00000.jpg')  # BGR
# predict(img_raw, view_img=True)
