FROM ubuntu:18.04

RUN apt-get update \
 && apt-get install -y python3-pip python3-yaml \
 && pip3 install dask s3fs toolz zarr

COPY merge_zarr.py /scripts/merge_zarr.py

ENTRYPOINT ["python3", "/scripts/merge_zarr.py"]
