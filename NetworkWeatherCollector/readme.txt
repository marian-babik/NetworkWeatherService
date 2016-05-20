
to build Docker container:
sudo docker build -t perfsonar_collector .

to run all three collectors:
sudo docker run perfsonar_collector

or individually:
sudo docker run perfsonar_collector ./NetworkThroughputCollector.py
