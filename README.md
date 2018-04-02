# table-testing

This repository provides a collaborative space to specify requirements, examples, and tests for on-disk file formats used to store expression matrices arising from single-cell RNA sequencing analyses. There are currently a wide variety of formats used for these data, including generic formats (e.g. CSV) and those designed specifically for this domain (e.g. Loom). 

Please open an issue or make a PR if you'd like to add ideas or start a discussion!

Associated documents that inform this repo can be found in this [Google Drive](https://drive.google.com/drive/u/0/folders/1D___JK5hnlc5A3Qhkp5LrZgiYycvsd_2) and are linked throughout the repository.

We meet from time to time, here are our [meeting notes](https://docs.google.com/document/d/1v0NSeML2F6JRuLrjXm7Kprh-BiamnpiODqHHpBnituY/edit), feel free to reach out and join!

## Goals

- Catalog the requirements that existing or future formats may or may not satisfy
- Provide example datasets and loading scripts (cross language) for these examples
- Provide test suites that evaluate performance of formats against requirements

## Requirements

In many discussions, requirements fall broadly into two categories, archival (long-term storage) and analysis (daily use with analytical software in e.g. R or Python). As written, some of these are explicit requirements (e.g. self-describing), whereas others are dimensions along which different formats vary (e.g. size and speed).

#### Archival
- Long-term abilily to read and parse the file (does it depend on APIs or language-features that are likely to change?)
- Self-describing (are the semantics of the file contained within it?)
- Size (especially after compression)

#### Analysis
- Partial IO (can portions of the file be read without loading the whole thing?)
  - Loading subsets of genes (e.g. fitting a regression model to each gene in parallel)
  - Loading subsets of cells (e.g. for differential expression)
  - Making arbitrary byte-range queries (e.g. for a web service)
- Speed of reading and writing data
- Storing additional metadata or features alongide primary table (e.g. derived features or auxillery measurements)
- Optomized for sparsity (affects both speed and size)
- Ability to handle large numbers of cells (e.g. out-of-memory, memory mapping, etc.)

## Formats

Here we list formats that have been used or proposed thus far in the community (please add!):
- `.csv` (also includes TSV)
- `.mat` (matlab)
- `.mtx` (matrix market)
- `.h5` (HDF5)
- `.h5ad` (a wrapped of HDF5 used by `scanpy`)
- `.loom` (a wrapper of `.h5`)
- `.npy` (serialized `numpy` matrices)
- `.arrow` (not currently used but potentially promising)
- `.Robj` (serialized R objects, e.g. from Seurat)
- `.zarr` [(chunked, compressed, N-dimensional arrays, python)](http://zarr.readthedocs.io/en/stable/index.html)

## Standard Matrices

We benchmark data formats and APIs on standard matrices. These matrices are found at the following S3 bucket.

[TODO add S3 bucket details]

[TODO describe matrices below]

## Test suite

Given [use cases](https://docs.google.com/document/d/1Vu0AJfV9N27hhcanRkrTcxP676ff-1qMuujrSs8jky8/edit#), we have collected we are creating a test suite to benchmark file formats contained in this project.

[TODO Add details on test suite]

[TODO Add details on extending test suite to new use cases]

[TODO Add detials on extending test suite to new file formats]

[TODO Add details on extending test suite to new data sets]

Fell free to contribute!
