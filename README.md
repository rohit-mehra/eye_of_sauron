<h1 align="center">
  <br>
  <a href=""><img src="https://github.com/rrqq/eye_of_sauron/data/logo.png" alt="The Eye of Sauron" width="200"></a>
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
  <a href="#key-features">Key Features</a> •
  <a href="#how-to-use">How To Use</a> •
  <a href="#download">Download</a> •
  <a href="#credits">Credits</a> •
  <a href="#related">Related</a> •
  <a href="#license">License</a>
</p>

![screenshot](https://github.com/rrqq/eye_of_sauron/data/demov2.gif)

## Key Features

-   Scalable - Get desired Frame Rate over multiple cameras, by just spinning more consumer nodes or more consumer processes in the same node.
-   Stream Processing in Python - Kafka Stream Processing API not yet available in Python
-   Modular approach - Replace Face recognition model with desired Image processing model to detect entities as per your use case.

## How To Use

To clone and run this application, you'll need [Git](https://git-scm.com), [python3](https://www.python.org/downloads/) (also install  [pip](https://docs.python.org/3/installing/index.html)) and kafka (v1.0.0 and v1.1.0 with scala v2.11 and v2.12) (all combinations) installed on your cluster. I used [Pegasus](https://github.com/InsightDataScience/pegasus) for the cluster setup on aws with [environment setup](https://github.com/InsightDataScience/pegasus/blob/master/install/environment/install_env.sh) modified to [this custom setup file](https://github.com/rrqq/eye_of_sauron/cluster_setup/install_env.sh).

Assuming all the environment is setup as per the instructions from above

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

## Download

## Credits

This software uses following open source packages.

-   [face_recognition](https://github.com/ageitgey/face_recognition)
-   [opencv](https://github.com/opencv/opencv)
-   [kafka](https://github.com/apache/kafka)
-   [pegasus](https://github.com/InsightDataScience/pegasus)

## Related

## Support

## You may also like...

* * *

> [Linkedin](https://www.linkedin.com/in/rohitmehra-utsa/)  · 
> GitHub [@rrqq](https://github.com/rrqq)  · 
> Kaggle [@rrqqmm](https://www.kaggle.com/rrqqmm)
