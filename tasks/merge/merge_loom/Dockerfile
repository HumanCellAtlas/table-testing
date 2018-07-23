FROM ubuntu:18.04

RUN apt-get update \
 && apt-get install -y python3-pip python3-yaml \
 && pip3 install loompy

COPY merge_loom.py /scripts/merge_loom.py

ENTRYPOINT ["python3", "/scripts/merge_loom.py"]
