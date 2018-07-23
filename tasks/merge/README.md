# Merge task

This task requires merging many small expression matrices into one large one. We
assume that each of the small matrices has been produced using an identical
processing pipeline with the same reference, so the number and order of genes in
each matrix is the same.

It addresses a particular problem for the HCA, where SmartSeq2 data is presently
stored so that an expression matrix file only contains data for a single cell.
But more generally, it involves performing relatively small reads from many
different files.

The relevant data source is `GSE84465_split`, which contains expression data from
3000 cells split into 3000 different files. The expected output matrix should
contain the expression values from all 3000 cells in any order.

# Results

The benchmark was run 10 times for each data source format. Note that the anndata
failed due to [this issue](https://github.com/theislab/anndata/issues/42).

The tests were run on an AWS r5d.xlarge with the matrices localized to the local
NVMe storage.

| Test            | Format                                | Mean time (s) |  Min  |  Max  | Variance |
|-----------------|---------------------------------------|---------------|-------|-------|----------|
| anndata         | anndata                               | Failed        | N/A   | N/A   | N/A      |
| feather         | feather                               | 16.69         | 16.22 | 17.56 | 0.21     |
| hdf5_h5py       | hdf5_1000_1000_chunks_3_compression   | 9.41          | 8.66  | 12.85 | 1.53     |
| hdf5_h5py       | hdf5_10000_10000_chunks_3_compression | 8.63          | 8.24  | 9.50  | 0.15     |
| hdf5_h5py       | hdf5_20000_20000_chunks_3_compression | 8.89          | 8.58  | 9.26  | 0.03     |
| loom            | loom                                  | 58.22         | 56.58 | 60.05 | 2.15     |
| matrix_market   | matrix_market                         | 27.07         | 26.29 | 27.77 | 0.25     |
| numpy           | numpy                                 | 7.55          | 7.28  | 8.23  | 0.08     |
| parquet         | parquet                               | 26.14         | 25.65 | 26.54 | 0.07     |
| sparse_hdf5     | sparse_hdf_csc                        | 7.35          | 6.84  | 8.41  | 0.35     |
| sparse_hdf5     | sparse_hdf_csr                        | 8.33          | 7.64  | 9.16  | 0.18     |
| zarr            | zarr_1000_1000                        | 14.54         | 14.39 | 14.77 | 0.01     |
| zarr            | zarr_10000_10000                      | 5.02          | 4.91  | 5.11  | 0.00     |
| zarr            | zarr_20000_20000                      | 4.60          | 4.49  | 4.69  | 0.00     |

