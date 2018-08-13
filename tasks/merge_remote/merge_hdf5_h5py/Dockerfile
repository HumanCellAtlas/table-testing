FROM ubuntu:18.04

RUN apt-get update \
 && apt-get install -y python3-h5py python3-numpy python3-yaml

COPY merge_h5py.py /scripts/merge_h5py.py

ENTRYPOINT ["python3", "/scripts/merge_h5py.py"]
