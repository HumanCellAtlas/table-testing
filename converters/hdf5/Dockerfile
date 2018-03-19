FROM ubuntu:xenial

RUN apt-get update -y \
 && apt-get install -y python3-h5py python3-scipy

ADD convert.py /convert.py

ENTRYPOINT ["python3", "/convert.py"]
CMD ["--help"]
