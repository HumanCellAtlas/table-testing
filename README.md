# table-testing

This repository provides a collaborative space to specify requirements, examples, and tests for on-disk file formats used to store expression matrices arising from single-cell RNA sequencing analyses. There are currently a wide variety of formats used for these data, including generic formats (e.g. CSV) and those designed specifically for this domain (e.g. Loom). 

## goals

- Catalog the requirements that existing or future formats may or may not satisfy
- Provide example datasets and loading scripts (cross language) for these examples
- Provide test suites that evaluate performance of formats against requirements

## requirements

[fill in]

## formats

Here we list formats that have been used or proposed thus far in the community (please add!):
- `.csv` (also includes TSV)
- `.mat` (matlab)
- `.mtx` (matrix market)
- `.h5` (HDF5)
- `.h5ad` (a wrapped of HDF5 used by `scanpy`)
- `.loom` (a wrapper of `.h5`)
- `.npy` (serialized `numpy` matrices)
- `.arrow` (not currently used but potentially promising)
- `.Robj` (serialized R objects)

## test suite

[fill in]
