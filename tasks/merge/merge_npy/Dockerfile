FROM ubuntu:18.04

RUN apt-get update \
 && apt-get install -y python3-pip python3-yaml \
 && pip3 install numpy

COPY merge_npy.py /scripts/merge_npy.py

ENTRYPOINT ["python3", "/scripts/merge_npy.py"]
