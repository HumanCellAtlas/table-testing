FROM ubuntu:18.04

RUN apt-get update \
 && apt-get install -y python3-pip python3-yaml \
 && pip3 install h5sparse

COPY merge_sparse_hdf5.py /scripts/merge_sparse_hdf5.py

ENTRYPOINT ["python3", "/scripts/merge_sparse_hdf5.py"]
