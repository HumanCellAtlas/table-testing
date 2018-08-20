import tempfile
import os

import anndata
import h5py
import h5sparse
import loompy
import numpy
import pandas
import pyarrow
import pyarrow.parquet
import scanpy.api as sc
import scipy.io
import scipy.sparse
import zarr

NUM_QC_VALUES = 100

def fake_qc_values(qc_val_count, index, seed=0):

    numpy.random.seed(int(seed))
    qc_data = numpy.array([numpy.random.normal(.5, .2, len(index)) for _ in range(qc_val_count)])
    qcs = pandas.DataFrame(
        data=qc_data.T,
        columns=["qc" + str(i) for i in range(100)],
        index=index
    )

    return qcs

def convert_from_10xh5(path, genome):
    adata = sc.read_10x_h5(path, genome)
    adata.var_names_make_unique()
    df = pandas.DataFrame(adata.X.todense(), index=adata.obs_names, columns=adata.var_names)
    yield df

def convert_from_geocsv(path, split=False, num_to_keep=100):
    data = pandas.read_csv(
        path,
        header=0,
        index_col=0,
        delim_whitespace=True).T

    if split:
        cell_names = data.index.tolist()
        for cell_name in cell_names[:num_to_keep]:
            yield data.loc(cell_name).to_frame().T
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
    f.create_dataset("gene_names", data=df.columns.values, dtype=dt)
    f.create_dataset("cell_names", data=df.index.values, dtype=dt)

    f.attrs["hdf5_version"] = h5py.version.hdf5_version
    f.attrs["h5py_version"] = h5py.version.version

    qcs = fake_qc_values(NUM_QC_VALUES, df.index, seed=df.values.sum())
    qc_chunks = (min(qcs.shape[0], chunks[0]), min(qcs.shape[1], chunks[1]))
    f.create_dataset("qc_values", data=qcs.as_matrix(), chunks=qc_chunks, compression=compression)
    f.create_dataset("qc_names", data=qcs.index.values, dtype=dt)

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

    qcs = fake_qc_values(NUM_QC_VALUES, df.index, seed=df.values.sum())
    dt = h5py.special_dtype(vlen=bytes)
    f.h5f.create_dataset("qc_values", data=qcs.as_matrix())
    f.h5f.create_dataset("qc_names", data=qcs.index.values, dtype=dt)

    f.h5f.close()

    return path

def convert_to_loom(df):
    """Convert a dataframe of expression values to a loom file."""

    path = _get_temp_path(".loom")
    qcs = fake_qc_values(NUM_QC_VALUES, df.index, seed=df.values.sum())
    row_attrs = qcs.to_dict(orient='list')
    row_attrs["cell_name"] = df.index.values

    loompy.create(path, df.as_matrix(), row_attrs,
                  {"gene_name": df.columns.values})
    return path

def convert_to_parquet(df, row_group_size, compression):
    """Convert a dataframe of expression values to a parquet file."""

    path = _get_temp_path(".parquet")
    qcs = fake_qc_values(NUM_QC_VALUES, df.index, seed=df.values.sum())
    full_df = pandas.concat([df, qcs], axis=1)
    table = pyarrow.Table.from_pandas(full_df)
    pyarrow.parquet.write_table(table, path, row_group_size=row_group_size,
                                compression=compression)

    return path

def convert_to_feather(df):
    """Convert a dataframe of expression values to a feather file."""

    path = _get_temp_path(".feather")
    qcs = fake_qc_values(NUM_QC_VALUES, df.index, seed=df.values.sum())
    full_df = pandas.concat([df, qcs], axis=1)
    full_df.reset_index().to_feather(path)
    return path

def convert_to_anndata(df):
    """Convert a dataframe of expression values to a scanpy anndata file."""

    qcs = fake_qc_values(NUM_QC_VALUES, df.index, seed=df.values.sum())
    cell_attrs = qcs.to_dict(orient='list')
    cell_attrs["cell_name"] = df.index.values

    adata = anndata.AnnData(
        df.as_matrix(),
        cell_attrs,
        {"gene_name": df.columns.values}
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
    root = zarr.group(store=store)

    root.create_dataset("data", data=df.as_matrix(), chunks=adj_chunks, dtype='f4')
    root.create_dataset("cell_name", data=df.index.tolist())
    root.create_dataset("gene_name", data=df.columns.tolist())

    qcs = fake_qc_values(NUM_QC_VALUES, df.index, seed=df.values.sum())
    qc_chunks = (min(qcs.shape[0], chunks[0]), min(qcs.shape[1], chunks[1]))
    root.create_dataset("qc_values", data=qcs, chunks=qc_chunks)
    root.create_dataset("qc_names", data=qcs.columns.tolist())

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
