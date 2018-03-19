# Convert SmartSeq2 data from GEO into different file types

There's a dataset on GEO here:

ftp://ftp.ncbi.nlm.nih.gov/geo/series/GSE84nnn/GSE84465/suppl/GSE84465_GBM_All_data.csv.gz


That contains SmartSeq2 expression measurements from about 3500 cells. The
container defined here converts those into a number of different file types:

- HDF5 (with variation in chunking and compression)
- HDF5 but represented as a compressed sparse row or column matrix
- Loom
- AnnData from scanpy
- Binary numpy
- Feather
- Parquet
- Zarr

The container expects a `/data` directory to be mounted, and that's where it's
going to read the GEO file and write all the different files. You could run it
like this

```
docker run -d -v /path/to/geo/data:/data -it matrixtests/gse84465:v0.0.1
```
