#!/bin/bash

sudo apt-get install python3-pip
sudo pip3 install virtualenv
virtualenv bdp-ass3
source bdp-ass3/bin/activate
pip install pika
