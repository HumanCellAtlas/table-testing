"""Convert a csv from GEO into many different file types."""

import copy
import functools
import itertools
import multiprocessing.dummy
import os

import anndata
import h5py
import h5sparse
import loompy
import numpy
import pandas
import pyarrow
import scipy.sparse
import zarr

# The location of the geo data that's been mounted into the container
DATA_DIR = '/data'
in_data_dir = functools.partial(os.path.join, DATA_DIR)


def _create_path(library, cell_name, params, ext):
    """Create the path to a results file."""

    file_name_scheme = 'GSE84464_{cell_name}_{library}_{params}.{ext}'
    dir_name_scheme = "{library}_{params}"

    file_name = file_name_scheme.format(
        library=library,
        cell_name=cell_name,
        params=params,
        ext=ext)
    dir_name = dir_name_scheme.format(
        library=library,
        params=params
    )

    path = in_data_dir(os.path.join(dir_name, file_name))

    if not os.path.isdir(os.path.dirname(path)):
        os.mkdir(os.path.dirname(path))

    return path

def to_hdf5(df, cell_name):
    """Convert a dataframe of expression values to an hdf5 file."""

    # Select a range of chunking and compression
    chunk_sizes = [
        (df.shape[0], 1),
        (1, df.shape[1]),
        (min(df.shape[0], 25), min(df.shape[1], 25))
    ]

    compressions = [
        0,
        3,
        7,
        'lzf'
    ]

    for chunk_size, compression in itertools.product(chunk_sizes, compressions):
        creation_params = {"chunks": chunk_size, "compression": compression}

        # We probably don't need to do the experiment with single-measurement chunks
        if chunk_size == (1, 1):
            continue

        path = _create_path(
            "h5py",
            cell_name,
            "chunk_{}_{}_compression_{}".format(chunk_size[0], chunk_size[1], compression),
            "h5"
        )

        f = h5py.File(path, 'w')
        f.create_dataset("data", data=df.as_matrix(), **creation_params)
        dt = h5py.special_dtype(vlen=bytes)
        f.create_dataset("gene_names", data=df.index.values, dtype=dt)
        f.create_dataset("cell_names", data=df.columns.values, dtype=dt)

        f.attrs["hdf5_version"] = h5py.version.hdf5_version
        f.attrs["h5py_version"] = h5py.version.version

        f.close()

def to_sparse_hdf5(df, cell_name):
    """Convert a datafraom to a sparse represenation in an hdf5 file."""

    for major in ("csc", "csr"):
        path = _create_path("h5sparse", cell_name, major, "h5")
        matrix_class = getattr(scipy.sparse, major + "_matrix")
        sparse_matrix = matrix_class(df.as_matrix())
        f = h5sparse.File(path, 'w')
        f.create_dataset("data", data=sparse_matrix)

        f.h5f.attrs["hdf5_version"] = h5py.version.hdf5_version
        f.h5f.attrs["h5py_version"] = h5py.version.version

        f.h5f.close()

def to_loom(df, cell_name):
    """Convert a dataframe of expression values to a loom file."""

    path = _create_path("loompy", cell_name, "none", "loom")
    loompy.create(path, df.as_matrix(),
                  {"gene_names": df.index.values}, {"cell_names": df.columns.values})

def to_parquet(df, cell_name):
    """Convert a dataframe of expression values to a parquet file."""

    path = _create_path("parquet", cell_name, "pandas", "parquet")
    df.to_parquet(path, "pyarrow")

def to_feather(df, cell_name):
    """Convert a dataframe of expression values to a feather file."""

    path = _create_path("feather", cell_name, "pandas", "feather")
    df.reset_index().to_feather(path)

def to_anndata(df, cell_name):
    """Convert a dataframe of expression values to a scanpy anndata file."""

    adata = anndata.AnnData(
        df.as_matrix().T,
        {"cell_names": df.columns.values},
        {"gene_names": df.index.values}
    )

    path = _create_path("anndata", cell_name, "none", "h5ad")
    adata.write(path)

def to_npy(df, cell_name):
    """Convert a dataframe of expression values to a binary numpy file."""

    path = _create_path("numpy", cell_name, "none", "npy")
    numpy.save(path, df.as_matrix())


def to_zarr(df, cell_name):
    """Anything is possible with ZARR"""

    chunk_sizes = [
        (df.shape[0], 1),
        (1, df.shape[1]),
        (min(df.shape[0], 25), min(df.shape[1], 25))
    ]

    for chunk_size in chunk_sizes:

        if chunk_size == (1, 1):
            continue

        path = _create_path("zarr", cell_name, "{}_{}".format(*chunk_size), "zarr")
        zarr.save_array(path, df.as_matrix(), chunks=chunk_size)


# Load the data from the GEO CSV file
data = pandas.read_csv(
    in_data_dir("GSE84465_GBM_All_data.csv.gz"),
    header=0,
    index_col=0,
    delim_whitespace=True)
cell_names = data.columns.tolist()

fn_list = [
    to_zarr,
    to_hdf5,
    to_sparse_hdf5,
    to_loom,
    to_parquet,
    to_feather,
    to_anndata,
    to_npy
]

print("All")
for fn in fn_list:
    fn(data, "All")

counter = 0
for cell_name_ in cell_names:
    print(counter, cell_name_)
    counter += 1 

    cell_frame = data[cell_name_].to_frame()

    for fn in fn_list:
        fn(cell_frame, cell_name_)
