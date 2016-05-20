
to build Docker container:
docker build -t esnet_collector .

to run it:
sudo docker run esnet_collector cl-analytics.mwt2.org:9200

at CERN:
sudo docker run esnet_collector es-atlas.cern.ch:9202 es-atlas password 600 
