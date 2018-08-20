FROM ubuntu:18.04

RUN apt-get update -y \
 && apt-get install -y python3-pip wget

RUN pip3 install -U pip \
 && pip install loompy==2.0.8 pyarrow==0.9.0 \
                zarr==2.2.0 pandas==0.22.0 \
                fastparquet==0.1.5 \
                feather-format==0.4.0 \
                h5sparse==0.0.4 \
                scanpy==1.2.2 \
                scipy==1.1.0 \
                h5py==2.8.0 \
                PyYAML

COPY convert.py /scripts/convert.py
COPY converters.py /scripts/converters.py

# Otherwise we fail in missing Tkinter
RUN sed -i -e "s/TkAgg/Agg/g" /usr/local/lib/python3.6/dist-packages/matplotlib/mpl-data/matplotlibrc

ENTRYPOINT ["python3", "/scripts/convert.py"]
