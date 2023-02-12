#!/bin/bash

# Copyright 2015 Insight Data Science
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


sudo add-apt-repository ppa:openjdk-r/ppa -y
sudo apt-get update
wait
sudo dpkg --configure -a
sudo apt-get -y --fix-missing install ssh bc rsync openjdk-8-jdk dos2unix scala python3-dev python-dev python-pip python3-pip gfortran git supervisor ruby awscli python3-tk protobuf-compiler
wait
sudo dpkg --configure -a
# get sbt repository
wget https://dl.bintray.com/sbt/debian/sbt-0.13.7.deb -P ~/Downloads
wait
sudo dpkg -i ~/Downloads/sbt-*
wait
sudo  apt-get install -y --fix-missing \
build-essential \
cmake \
gfortran \
git \
wget \
curl \
graphicsmagick \
libgraphicsmagick1-dev \
libatlas-dev \
libavcodec-dev \
libavformat-dev \
libgtk2.0-dev \
libjpeg-dev \
liblapack-dev \
libswscale-dev \
pkg-config \
python3-dev \
software-properties-common \
zip \
&& sudo apt-get clean && sudo rm -rf /tmp/* /var/tmp/*
wait
cd ~ && \
mkdir -p dlib && \
git clone -b 'v19.9' --single-branch https://github.com/davisking/dlib.git dlib/ && \
cd  dlib/ && \
sudo python3 setup.py install --yes USE_AVX_INSTRUCTIONS
cd ~
wait
sudo pip3 install face_recognition
sudo pip3 install -U flask flask-uploads flask-dropzone

# python 3
sudo pip3 install install jupyter
sudo pip3 install -U setuptools numpy nose seaborn boto boto3 scikit-learn scipy h5py tqdm pyspark six pandas
sudo pip3 install -U tensorflow keras wget lxml Cython contextlib2 imutils
sudo pip3 install -U pip
sudo pip3 install -U kafka-python opencv-contrib-python Flask tornado absl-py
wait

# for jupyter
jupyter notebook --generate-config
echo "c.NotebookApp.ip = '*'" >> /home/ubuntu/.jupyter/jupyter_notebook_config.py
echo "c.NotebookApp.open_browser = False" >> /home/ubuntu/.jupyter/jupyter_notebook_config.py

# ---> make sure you replace ubuntu with your username <---
sudo chown -R ubuntu:ubuntu /home/ubuntu/.local

# python2.7
sudo pip install nose numpy pandas seaborn boto boto3 scikit-learn "ipython[notebook]==5.5.0"
sudo pip install -U pip
# get maven3 repository
sudo apt-get purge maven maven2 maven3
#sudo apt-add-repository -y ppa:andrei-pozolotin/maven3
sudo apt-get update
#sudo apt-get --yes --force-yes install maven3
wait
sudo update-java-alternatives -s java-1.8.0-openjdk-amd64
wait
sudo dpkg --configure -a
sudo apt-get -y --fix-missing install maven
sudo ipython kernel install
https://github.com/rrqq/eye_of_sauron.git

# Setup for object detection API = TOTALY OPTIONAL
echo export MODELSPATH=/home/ubuntu/models >> /home/ubuntu/.bashrc
echo export RESEARCHPATH=/home/ubuntu/models/research >> /home/ubuntu/.bashrc
echo export SLIMPATH=/home/ubuntu/models/research/slim >> /home/ubuntu/.bashrc
echo export ODPATH=/home/ubuntu/models/research/object_detection >> /home/ubuntu/.bashrc
echo export PYTHONPATH=$PYTHONPATH:/home/ubuntu/models/research:/home/ubuntu/models/research/slim >> /home/ubuntu/.bashrc

echo source /home/ubuntu/.bashrc > /home/ubuntu/od_setup.sh
echo git clone https://github.com/tensorflow/models >> /home/ubuntu/od_setup.sh
echo wget -O /home/ubuntu/models/research/protobuf.zip https://github.com/google/protobuf/releases/download/v3.0.0/protoc-3.0.0-linux-x86_64.zip >> /home/ubuntu/od_setup.sh
echo unzip /home/ubuntu/models/research/protobuf.zip -d /home/ubuntu/models/research >> /home/ubuntu/od_setup.sh
echo cd /home/ubuntu/models/research >> /home/ubuntu/od_setup.sh
echo ./bin/protoc object_detection/protos/*.proto --python_out=. >> /home/ubuntu/od_setup.sh
echo git clone https://github.com/lbeaucourt/Object-detection.git /home/ubuntu/Object-detection >> /home/ubuntu/od_setup.sh

if ! grep "export JAVA_HOME" ~/.profile; then
  echo -e "\nexport JAVA_HOME=/usr" | cat >> ~/.profile
  echo -e "export PATH=\$PATH:\$JAVA_HOME/bin" | cat >> ~/.profile
fi
