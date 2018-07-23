FROM ubuntu:18.04

RUN apt-get update \
 && apt-get install -y python3-pip python3-yaml \
 && pip3 install pandas feather-format

COPY merge_feather.py /scripts/merge_feather.py

ENTRYPOINT ["python3", "/scripts/merge_feather.py"]
