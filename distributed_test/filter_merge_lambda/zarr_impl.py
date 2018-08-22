"""Implementation of zarr filter and merge."""

import json
import math
import os

import boto3
import numpy
import pandas
import s3fs
import zarr

import dynamo_utils

# Some constants for writing the output zarr archive
ZARR_OUTPUT_CONFIG = {
    "cells_per_chunk": 3000,
    "compressor": zarr.storage.default_compressor,
    "dtypes": {
        "data": "<f4",
        "cell_name": "<U64",
        "qc_values": "<f4"
    },
    "order": "C"
}

# A couple table names we expect cloudformation to set as environment
# variables.
ZARR_ENV_VARS = {
    "zarr_lock_table": os.environ.get("ZARR_LOCK_TABLE"),
    "zarr_output_table": os.environ.get("ZARR_OUTPUT_TABLE"),
    "result_bucket": os.environ.get("RESULT_BUCKET")
}

def open_zarr(s3_path, anon=False, cache=False):
    """Open a zarr archive and return its root."""

    s3 = s3fs.S3FileSystem(anon=anon)
    store = s3fs.S3Map(s3_path, s3=s3, check=False, create=False)
    if cache:
        lrucache = zarr.LRUStoreCache(store=store, max_size=1<<29)
        root = zarr.group(store=lrucache)
    else:
        root = zarr.group(store=store)
    return root

def get_output_row_boundaries(request_id, nrows):
    """Get the start and end rows in the output table that we should write a
    filtered result to.
    """

    return dynamo_utils.increment_field(
        table_name=ZARR_ENV_VARS["zarr_output_table"],
        key_dict={"RequestId": request_id},
        field_name="RowCount",
        increment_size=nrows)

def driver(request_id):
    """Initialize the output row count entry."""

    output_table = boto3.resource(
        "dynamodb", region_name="us-east-1").Table(ZARR_ENV_VARS["zarr_output_table"])
    output_table.put_item(
        Item={"RequestId": request_id,
              "RowCount": 0}
    )


def mapper(request_id, bucket, prefix):
    """Get instructions on how to process an input zarr store by chunk.

    For a zarr store at s3://bucket/prefix, return a list of row boundaries
    that correspond to chunks in the store. Assumes that the s3 path is
    readable by an anonymous client.
    """

    s3_path = f"{bucket}/{prefix}"
    print(f"Opening path {s3_path}")
    root = open_zarr(s3_path, anon=True)

    chunk_rows = root.data.chunks[0]
    nchunks = root.data.nchunks
    print(f"ChunkRows {chunk_rows} nchunks {nchunks}")
    return [{"bucket": bucket,
             "prefix": prefix,
             "start_row": n*chunk_rows,
             "num_rows": chunk_rows}
            for n in range(nchunks)]


def _filter_chunk(zarr_root, start_row, num_rows, filter_string):
    """Apply the filter defined in filter_string to the rows specified by start_row
    and num_rows.

    Args:
      zarr_root: A zarr.Group object that points to an input to be filtered
      start_row: Beginning of chunk to filter within the zarr store
      num_rows: Number of rows in the chunk
      filter_string: Pandas filter to apply to the matrix

    Returns:
      Tuple of two pandas.DataFrames:
        filtered_data: the filtered expression counts
        filtered_qcs: the filtered qc values
    """
    end_row = start_row + num_rows # it's okay if end_row reads off the end
    exp_df = pandas.DataFrame(data=zarr_root.data[start_row:end_row],
                              index=zarr_root.cell_name[start_row:end_row],
                              columns=zarr_root.gene_name)
    qc_df = pandas.DataFrame(data=zarr_root.qc_values[start_row:end_row],
                             index=zarr_root.cell_name[start_row:end_row],
                             columns=zarr_root.qc_names)
    # Notionally concatenate the qc and data matrices so we can apply the
    # filter to both at once.
    matrix = pandas.concat([exp_df, qc_df], axis=1, copy=False)
    print("Input matrix:", matrix.shape)

    # TODO: Maybe don't just eval whatever string is submitted by the user

    # Split up the matrices into data and qc parts so we can write them to
    # separate zarr datasets
    filtered_matrix = matrix[eval(filter_string)]
    filtered_data = filtered_matrix.iloc[:, :exp_df.shape[1]]
    filtered_qcs = filtered_matrix.iloc[:, exp_df.shape[1]:]

    return filtered_data, filtered_qcs

def work(request_id, filter_string, bucket, prefix, start_row, num_rows):
    """Apply the filter string to the chunk of the matrix defined by start_row
    and num_rows.
    """

    s3_path = f"{bucket}/{prefix}"
    root = open_zarr(s3_path, anon=True, cache=True)
    filtered_data, filtered_qcs = _filter_chunk(root, start_row, num_rows, filter_string)
    print("After filtering:", filtered_data.shape, filtered_qcs.shape)

    # Now write the output zarr files. We're rolling our own distributed zarr
    # writer for this demo, so we're going to have some problems...
    s3 = s3fs.S3FileSystem(anon=False)
    result_bucket = ZARR_ENV_VARS["result_bucket"]
    zarr_cells_per_chunk = ZARR_OUTPUT_CONFIG["cells_per_chunk"]

    # Figure out which rows of the output table this filtered chunk will be
    # assigned.
    result_start, result_end = get_output_row_boundaries(
        request_id, filtered_data.shape[0])
    print("In final output, we will write to", result_start, result_end)

    # Based on that, determine which zarr chunks we need to write to
    start_chunk_idx = math.floor(result_start/zarr_cells_per_chunk)
    end_chunk_idx = math.ceil(result_end/zarr_cells_per_chunk)
    print(f'This corresponds to chunks {start_chunk_idx}:{end_chunk_idx}')

    # Now iterate through each chunk we're supposed to write to, and write the
    # appropriate rows to each one.
    written_rows = 0
    for chunk_idx in range(start_chunk_idx, end_chunk_idx):

        # Get the start and end rows of this chunk in the whole dataset
        chunk_start_in_result = chunk_idx * zarr_cells_per_chunk
        chunk_end_in_result = chunk_start_in_result + zarr_cells_per_chunk
        print(f"Writing to chunk {chunk_idx}, corresponding to rows "
              f"{chunk_start_in_result} to {chunk_end_in_result}")

        # Get the start and end rows in the filtered matrix that correspond to
        # this chunk as well as the start and end rows in the chunk.
        start_in_values = int(written_rows)
        start_in_chunk = int(max(0, result_start - chunk_start_in_result))
        end_in_values = int(min(result_end - result_start,
                                start_in_values + zarr_cells_per_chunk - start_in_chunk))
        end_in_chunk = int(start_in_chunk + end_in_values - start_in_values)
        print(f"Writing {start_in_values}:{end_in_values} --> {start_in_chunk}:{end_in_chunk}")

        for dset in ["data", "qc_values", "cell_name"]:
            if dset == "data":
                vals_to_write = filtered_data.values
            elif dset == "qc_values":
                vals_to_write = filtered_qcs.values
            elif dset == "cell_name":
                vals_to_write = filtered_data.index.values

            full_dest_key = f"s3://{result_bucket}/{request_id}.zarr/{dset}/{chunk_idx}.0"
            print(f"Writing {dset} to {full_dest_key}")
            if vals_to_write.ndim == 2:
                chunk_shape = (zarr_cells_per_chunk, vals_to_write.shape[1])
            else:
                chunk_shape = (zarr_cells_per_chunk,)
            dtype = ZARR_OUTPUT_CONFIG["dtypes"][dset]

            # Reading and writing zarr chunks is pretty straightforward, you
            # just pass it through the compression and cast it to a numpy array
            with dynamo_utils.Lock(ZARR_ENV_VARS["zarr_lock_table"], full_dest_key):
                try:
                    arr = numpy.frombuffer(
                        ZARR_OUTPUT_CONFIG["compressor"].decode(
                            s3.open(full_dest_key, 'rb').read()),
                        dtype=dtype).reshape(chunk_shape, order=ZARR_OUTPUT_CONFIG["order"])
                    print("Read array from s3")
                except FileNotFoundError:
                    arr = numpy.zeros(shape=chunk_shape,
                                      dtype=dtype)
                    print("Created new array")
                arr.setflags(write=1)
                arr[start_in_chunk:end_in_chunk] = vals_to_write[start_in_values:end_in_values]
                s3.open(full_dest_key, 'wb').write(ZARR_OUTPUT_CONFIG["compressor"].encode(arr))

    # These names datasets are always the same, so just one worker needs to
    # write them and we don't really care about consistency. They're also
    # single-chunk and one dimensional.
    for dset in ["gene_name", "qc_names"]:

        full_dest_key = f"s3://{result_bucket}/{request_id}.zarr/{dset}/0.0"
        if not s3.exists(full_dest_key):
            with dynamo_utils.Lock(ZARR_ENV_VARS["zarr_lock_table"], full_dest_key):
                arr = numpy.array(getattr(root, dset))
                s3.open(full_dest_key, 'wb').write(ZARR_OUTPUT_CONFIG['compressor'].encode(arr))

            zarray_key = f"s3://{result_bucket}/{request_id}.zarr/{dset}/.zarray"
            zarray = {
                "chunks": [arr.shape[0]],
                "compressor": ZARR_OUTPUT_CONFIG["compressor"].get_config(),
                "dtype": str(arr.dtype),
                "fill_value": _fill_value(arr.dtype),
                "filters": None,
                "order": ZARR_OUTPUT_CONFIG["order"],
                "shape": [arr.shape[0]],
                "zarr_format": 2
            }
            with dynamo_utils.Lock(ZARR_ENV_VARS["zarr_lock_table"], zarray_key):
                s3.open(zarray_key, 'wb').write(json.dumps(zarray).encode())


def _fill_value(dtype):
    if dtype.kind == 'f':
        return float(0)
    elif dtype.kind == 'i':
        return 0
    elif dtype.kind == 'U':
        return ""

def reducer(request_id):
    """Write remaining metadata to the zarr store."""

    s3 = s3fs.S3FileSystem(anon=False)
    result_bucket = ZARR_ENV_VARS["result_bucket"]

    # Write the zgroup file, which is very simple
    zgroup_key = f"s3://{result_bucket}/{request_id}.zarr/.zgroup"
    s3.open(zgroup_key, 'wb').write(json.dumps({"zarr_format": 2}).encode())

    num_genes = json.loads(s3.open(
        f"s3://{result_bucket}/{request_id}.zarr/gene_name/.zarray", 'rb').read())["chunks"][0]
    num_qcs = json.loads(s3.open(
        f"s3://{result_bucket}/{request_id}.zarr/qc_names/.zarray", 'rb').read())["chunks"][0]
    ncols = {"data": int(num_genes), "qc_values": int(num_qcs), "cell_name": 0}
    num_rows, num_rows = get_output_row_boundaries(request_id, 0)

    for dset in ["data", "qc_values", "cell_name"]:
        zarray_key = f"s3://{result_bucket}/{request_id}.zarr/{dset}/.zarray"

        chunks = [ZARR_OUTPUT_CONFIG["cells_per_chunk"]]
        shape = [int(num_rows)]
        if ncols[dset]:
            chunks.append(ncols[dset])
            shape.append(ncols[dset])

        zarray = {
            "chunks": chunks,
            "compressor": ZARR_OUTPUT_CONFIG["compressor"].get_config(),
            "dtype": ZARR_OUTPUT_CONFIG["dtypes"][dset],
            "fill_value": _fill_value(numpy.dtype(ZARR_OUTPUT_CONFIG["dtypes"][dset])),
            "filters": None,
            "order": ZARR_OUTPUT_CONFIG["order"],
            "shape": shape,
            "zarr_format": 2
        }
        s3.open(zarray_key, 'wb').write(json.dumps(zarray).encode())
#
#def _direct_merge_reducer(request_id):
#    """Would like to avoid this."""
#    result_bucket = CFN_VARS["result_bucket"]
#    s3_path = f'{result_bucket}/{request_id}/intermediate'
#    fs = s3fs.S3FileSystem(anon=False)
#    shards = fs.ls(s3_path)
#
#    # Get info we need to initialize the output matrix
#    root = open_zarr(shards[0])
#    data_dtype = root.data.dtype
#    data_ncols = root.data.shape[1]
#    qcs_dtype = root.qc_values.dtype
#    qcs_ncols = root.qc_values.shape[1]
#    gene_name = root.gene_name
#    qc_names = root.qc_names
#
#    def get_rows(s):
#        store = s3fs.S3Map(s)
#        root = zarr.group(store=store)
#        return root.data.shape[0]
#
#    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as exe:
#        total_rows = sum(exe.map(get_rows, shards))
#
#    dest_s3_path = f'{result_bucket}/{request_id}/{request_id}.zarr'
#    dest_root = open_zarr(dest_s3_path, cache=False)
#    dest_root.create_dataset("gene_name", data=gene_name, chunks=gene_name.shape)
#    dest_root.create_dataset("qc_names", data=qc_names, chunks=qc_names.shape)
#    dest_root.create_dataset("cell_name", shape=(total_rows,), dtype="<U40")
#    dest_root.create_dataset("data", shape=(total_rows, data_ncols),
#                             dtype=data_dtype, chunks=(1000, data_ncols))
#    dest_root.create_dataset("qc_values", shape=(total_rows, qcs_ncols),
#                             dtype=qcs_dtype, chunks=(1000, qcs_ncols))
#
#    cur_row = 0
#
#    for shard in shards:
#        shard_root = open_zarr(shard, cache=True)
#        shard_nrows = shard_root.data.shape[0]
#        last_row = cur_row + shard_nrows
#        dest_root.data[cur_row:last_row, :] = shard_root.data
#        dest_root.qc_values[cur_row:last_row, :] = shard_root.qc_values
#        dest_root.cell_name[cur_row:last_row] = shard_root.cell_name
#        cur_row = last_row
