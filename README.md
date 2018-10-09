<h1 align="center">
  <br>
  <a href=""><img src="/data/logo.png" alt="The Eye of Sauron" width="200"></a>
  <br>
  The Eye of Sauron
  <br>
</h1>

<h4 align="center">A Scalable Face Recognition System for Surveillance built on top of <a href="https://kafka.apache.org/" target="_blank">Kafka</a> using <a href="https://github.com/ageitgey/face_recognition" target="_blank">face_recognition</a> module.</h4>

<p align="center">
    <img src="https://img.shields.io/github/license/mashape/apistatus.svg?maxAge=2592000"
         alt="License">

</p>

<p align="center">
  <a href="#key-features-">Key Features</a> •
  <a href="#how-to-use-">How To Use</a> •
  <a href="#configuration-">Configuration</a> •
  <a href="#credits-">Credits</a> •
  <a href="#examples-">Examples</a> •
</p>

![screenshot](/data/6_2.gif)

## Key Features 📝

-   Scalable - Get desired Frame Rate over multiple cameras, by just spinning more consumer nodes or more consumer processes in the same node. The producers and consumers are designed as python processes, as subclass of [multiprocessing.Process](https://docs.python.org/3.5/library/multiprocessing.html#multiprocessing.Process)
-   Stream Processing in Python - This app essentially processes the stream of frames in python from the "raw frames" topic and publishes them into "predicted frames topic". Kafka [Stream API](https://kafka.apache.org/20/documentation/streams/) not yet available in Python, future work includes implementation of frame processing using stream api in scala.
-   Modular approach - Replace Face recognition model with desired Image processing model to detect entities as per your use case.

## How To Use ▶️

To clone and run this application, you'll need [Git](https://git-scm.com), [python3](https://www.python.org/downloads/) (also install  [pip](https://docs.python.org/3/installing/index.html)) and kafka (v1.0.0 and v1.1.0 with scala v2.11 and v2.12) (all combinations) installed on your cluster. I used [Pegasus](https://github.com/InsightDataScience/pegasus) for the cluster setup on aws with [environment setup](https://github.com/InsightDataScience/pegasus/blob/master/install/environment/install_env.sh) modified to [this custom setup file](/cluster_setup/install_env.sh).

-   Setup Environment as per the commands from [this custom setup file](/cluster_setup/install_env.sh).
-   Install [zookeeper- 3.4.13](https://s3-us-west-2.amazonaws.com/insight-tech/zookeeper/zookeeper-3.4.13.tar.gz)
-   Install [kafka-1.1.0 for scala-2.12](https://s3-us-west-2.amazonaws.com/insight-tech/kafka/kafka_2.12-1.1.0.tar.gz)
-   Start zookeeper service
-   Start kafka service

1.  From your command line (For web app and getting feeds from the camera):

```bash
# Clone this repository
$ git clone https://github.com/rrqq/eye_of_sauron.git

# Go into the repository
$ cd eye_of_sauron

# Install dependencies
$ sudo pip3 install -r requirements.txt

# Change permissions
$ chmod +x run.py

# Run the app
$ ./run.py
```

Note: If you're using Linux Bash you might need to convert run.py as

```bash
$ sudo apt-get install dos2unix
$ dos2unix run.py
```

2.  From your command line (For consumer nodes i.e. face recognition, or consumption of messages - frames from videos):

```bash
# Clone this repository
$ git clone https://github.com/rrqq/eye_of_sauron.git

# Go into the repository
$ cd eye_of_sauron

# Install dependencies
$ sudo pip3 install -r requirements.txt

# Run consumers
$ python3 prediction_producer.py
```

## Configuration ⚙️

## Credits ❤️

This software uses following open source packages.

-   [face_recognition](https://github.com/ageitgey/face_recognition)
-   [opencv](https://github.com/opencv/opencv)
-   [kafka](https://github.com/apache/kafka)
-   [pegasus](https://github.com/InsightDataScience/pegasus)

## Examples 💼

##### A. 3 CAMERAS

![screenshot](/data/3_1.gif)

##### B. 6 CAMERAS

![screenshot](/data/6_1.gif)

#### C. Using the eye for object detection over multiple cameras using [pretrained MobileNet-Caffe model.](https://github.com/shicai/MobileNet-Caffe)

![screenshot](/data/demov0.gif)

* * *

## Contact ✏️

> [Linkedin](https://www.linkedin.com/in/rohitmehra-utsa/)  · 
> GitHub [@rrqq](https://github.com/rrqq)  · 
> Kaggle [@rrqqmm](https://www.kaggle.com/rrqqmm)
