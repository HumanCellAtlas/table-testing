FROM ubuntu:artful

RUN apt-get update -y \
 && apt-get install -y python3-h5py python3-scipy python3-pip wget

RUN pip3 install -U pip \
 && pip install loompy==2.0.8 pyarrow==0.9.0 \
                zarr==2.2.0 pandas==0.22.0 \
                feather-format==0.4.0 \
                h5sparse==0.0.4 \
                anndata==0.5.9

# Install blosc
RUN wget https://github.com/Blosc/python-blosc/archive/v1.5.1.tar.gz \
 && tar xf v1.5.1.tar.gz \
 && cd python-blosc-1.5.1 \
 && python3 setup.py build_clib \
 && python3 setup.py build_ext --inplace \
 && mv blosc/ /usr/local/lib/python3.6/dist-packages/ \
 && cd .. \
 && rm -r python-blosc-1.5.1
