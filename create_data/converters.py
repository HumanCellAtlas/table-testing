import tempfile
import os

import anndata
import h5py
import h5sparse
import loompy
import numpy
import pandas
import scanpy.api as sc
import scipy.io
import scipy.sparse
import zarr

def convert_from_10xh5(path, genome):
    adata = sc.read_10x_h5(path, genome)
    adata.var_names_make_unique()
    df = pandas.DataFrame(adata.X.todense().T, adata.var_names, adata.obs_names)
    yield df

def convert_from_geocsv(path, split=False, num_to_keep=100):
    data = pandas.read_csv(
        path,
        header=0,
        index_col=0,
        delim_whitespace=True)

    if split:
        cell_names = data.columns.tolist()
        for cell_name in cell_names[:num_to_keep]:
            yield data[cell_name].to_frame()
    else:
        yield data


def _get_temp_path(suffix=None):
    temp_dir = tempfile.mkdtemp()
    temp_path = os.path.join(temp_dir, "matrix")
    if suffix:
        temp_path += suffix

    return temp_path

def convert_to_hdf5(df, chunks, compression):
    """Convert a dataframe of expression values to an hdf5 file."""
    path = _get_temp_path(".h5")
    f = h5py.File(path, 'w', libver='latest')

    adj_chunks = (min(df.shape[0], chunks[0]), min(df.shape[1], chunks[1]))
    f.create_dataset("data", data=df.as_matrix(), chunks=adj_chunks, compression=compression)
    dt = h5py.special_dtype(vlen=bytes)
    f.create_dataset("gene_names", data=df.index.values, dtype=dt)
    f.create_dataset("cell_names", data=df.columns.values, dtype=dt)

    f.attrs["hdf5_version"] = h5py.version.hdf5_version
    f.attrs["h5py_version"] = h5py.version.version

    f.close()

    return path

def convert_to_sparse_hdf5(df, major="csc"):
    """Convert a dataframe to a sparse represenation in an hdf5 file."""

    path = _get_temp_path(".h5")
    matrix_class = getattr(scipy.sparse, major + "_matrix")
    sparse_matrix = matrix_class(df.as_matrix())
    f = h5sparse.File(path, 'w', libver='latest')
    f.create_dataset("data", data=sparse_matrix)

    f.h5f.attrs["hdf5_version"] = h5py.version.hdf5_version
    f.h5f.attrs["h5py_version"] = h5py.version.version

    f.h5f.close()

    return path

def convert_to_loom(df):
    """Convert a dataframe of expression values to a loom file."""

    path = _get_temp_path(".loom")
    loompy.create(path, df.as_matrix(),
                  {"gene_names": df.index.values}, {"cell_names": df.columns.values})
    return path

def convert_to_parquet(df):
    """Convert a dataframe of expression values to a parquet file."""

    path = _get_temp_path(".parquet")
    df.to_parquet(path, "pyarrow")

    return path

def convert_to_feather(df):
    """Convert a dataframe of expression values to a feather file."""

    path = _get_temp_path(".feather")
    df.reset_index().to_feather(path)
    return path

def convert_to_anndata(df):
    """Convert a dataframe of expression values to a scanpy anndata file."""

    adata = anndata.AnnData(
        df.as_matrix().T,
        {"cell_names": df.columns.values},
        {"gene_names": df.index.values}
    )

    path = _get_temp_path(".h5ad")
    adata.write(path)
    return path

def convert_to_npy(df):
    """Convert a dataframe of expression values to a binary numpy file."""

    path = _get_temp_path(".npy")
    numpy.save(path, df.as_matrix())
    return path


def convert_to_zarr(df, store_type, chunks):
    """Anything is possible with ZARR"""

    path = _get_temp_path(".zarr")
    adj_chunks = (min(df.shape[0], chunks[0]), min(df.shape[1], chunks[1]))
    store = getattr(zarr, store_type)(path)
    zarr.array(df.as_matrix(), store=store, chunks=adj_chunks, dtype='f4')

    return path

def convert_to_csv(df):

    path = _get_temp_path(".csv.gz")
    df.to_csv(path, compression="gzip")
    return path

def convert_to_mtx(df):

    path = _get_temp_path(".mtx")
    sparse_mat = scipy.sparse.coo_matrix(df.values)
    scipy.io.mmwrite(path, sparse_mat)
    return path
