FROM ubuntu:18.04

RUN apt-get update \
 && apt-get install -y python3-pip python3-yaml \
 && pip3 install scipy

COPY merge_matrix_market.py /scripts/merge_matrix_market.py

ENTRYPOINT ["python3", "/scripts/merge_matrix_market.py"]
