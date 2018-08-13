FROM ubuntu:18.04

RUN apt-get update \
 && apt-get install -y python3-pip python3-yaml \
 && pip3 install scanpy

RUN sed -i -e "s/TkAgg/Agg/g" /usr/local/lib/python3.6/dist-packages/matplotlib/mpl-data/matplotlibrc

COPY merge_anndata.py /scripts/merge_anndata.py

ENTRYPOINT ["python3", "/scripts/merge_anndata.py"]
