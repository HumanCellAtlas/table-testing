# Merge remote task

This task is similar to the [merge task](tasks/merge) except that files are not
localized before the task is run. So the task has to merge the matrix files
without first localizing them to disk.

There are a couple different approaches to do this. A few file formats have
associated libraries that handle s3 urls transparently, like parquet with
pandas. Generally, this is using something like the [s3fs python
library](https://s3fs.readthedocs.io/en/latest/) underneath. Other formats
don't have such support, especially HDF5 which wants to handle opening a file
within the C library. We can still use a [FUSE
approach](https://github.com/s3fs-fuse/s3fs-fuse) though.

Results appear below. Note that the HDF5 formats failed this test. They
produced errors that looked like this:
```
Traceback (most recent call last):
  File "/scripts/merge_h5py.py", line 68, in <module>
    main()
  File "/scripts/merge_h5py.py", line 63, in main
    merge_hdf5s(args.input_paths, args.output_path)
  File "/scripts/merge_h5py.py", line 12, in merge_hdf5s
    arrays_to_merge.append(h5py.File(hdf5_path, 'r')["data"])
  File "/usr/lib/python3/dist-packages/h5py/_hl/files.py", line 269, in
__init__
    fid = make_fid(name, mode, userblock_size, fapl, swmr=swmr)
  File "/usr/lib/python3/dist-packages/h5py/_hl/files.py", line 99, in make_fid
    fid = h5f.open(name, flags, fapl=fapl)
  File "h5py/_objects.pyx", line 54, in h5py._objects.with_phil.wrapper
(/build/h5py-qzs83i/h5py-2.7.1/h5py/_objects.c:2847)
  File "h5py/_objects.pyx", line 55, in h5py._objects.with_phil.wrapper
(/build/h5py-qzs83i/h5py-2.7.1/h5py/_objects.c:2805)
  File "h5py/h5f.pyx", line 78, in h5py.h5f.open
(/build/h5py-qzs83i/h5py-2.7.1/h5py/h5f.c:2129)
OSError: Unable to open file (unable to open file: name =
'/home/ec2-user/data/GSE84465_split/hdf5_20000_20000_chunks_3_compression/inputs/matrices/hdf5_20000_20000_chunks_3_compression/GSE84465_split/1016/hdf5_20000_20000_chunks_3_compression_GSE84465_split.h5',
errno = 5, error message = 'Input/output error', flags = 0, o_flags = 0)
```

Curiously, it was always file 1016 that failed across multiple test and data
sources. It seems to be a read error coming from FUSE, but the error didn't occur
using FUSE with the other formats.


# Results

The benchmark was run 10 times for each data source format. Note that the anndata
failed due to [this issue](https://github.com/theislab/anndata/issues/42).

The tests were run on an AWS r5d.xlarge with the matrices localized to the local
NVMe storage.

| Test            | Format                       | Mean time (s) |   Min   |   Max   | St. Dev. |
|-----------------|------------------------------|---------------|---------|---------|----------|
| feather         | feather                      | 426.65        | 395.67  | 460.64  | 22.24    |
| matrix_market   | matrix_market                | 319.77        | 297.50  | 360.58  | 20.01    |
| numpy           | numpy                        | 256.54        | 224.89  | 321.67  | 29.99    |
| parquet         | parquet                      | 414.56        | 358.89  | 473.98  | 37.55    |
| zarr            | zarr_1000_1000               | 4020.94       | 3923.30 | 4179.99 | 77.05    |
| zarr            | zarr_10000_10000             | 679.47        | 643.15  | 741.37  | 32.16    |
| zarr            | zarr_20000_20000             | 500.22        | 475.63  | 531.54  | 16.8     |
