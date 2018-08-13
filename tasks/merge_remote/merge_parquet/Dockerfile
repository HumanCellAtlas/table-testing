FROM ubuntu:18.04

RUN apt-get update \
 && apt-get install -y python3-pip python3-yaml \
 && pip3 install pandas pyarrow s3fs

COPY merge_parquet.py /scripts/merge_parquet.py

ENTRYPOINT ["python3", "/scripts/merge_parquet.py"]
