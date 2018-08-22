"""Implementation of parquet filter and merge."""
import os
import uuid

import fastparquet
import pyarrow
import pyarrow.parquet
import s3fs

PARQUET_ENV_VARS = {
    "result_bucket": os.environ.get("RESULT_BUCKET")
}

def driver(request_id):
    """Parquet doesn't need any special setup in the driver."""
    pass


def mapper(request_id, bucket, prefix):
    """Split work on parquet file across row groups."""

    fs = s3fs.S3FileSystem(anon=True)
    s3_url = f"s3://{bucket}/{prefix}"
    pq = pyarrow.parquet.ParquetFile(fs.open(s3_url))

    return [{"bucket": bucket, "prefix": prefix, "row_group": row_group}
            for row_group in range(pq.num_row_groups)]

def work(request_id, filter_string, bucket, prefix, row_group):
    """Filter a single row group."""
    fs = s3fs.S3FileSystem(anon=True)
    s3_url = f"s3://{bucket}/{prefix}"
    pq = pyarrow.parquet.ParquetFile(fs.open(s3_url))

    table = pq.read_row_group(row_group)
    matrix = table.to_pandas(zero_copy_only=True)

    filtered_matrix = matrix[eval(filter_string)]

    shard_id = str(uuid.uuid4())
    result_bucket = PARQUET_ENV_VARS["result_bucket"]
    dest_s3_url = f"s3://{result_bucket}/{request_id}/{shard_id}.parquet"
    fs = s3fs.S3FileSystem(anon=False)
    pyarrow.parquet.write_table(
        pyarrow.Table.from_pandas(filtered_matrix),
        fs.open(dest_s3_url, "wb"),
        compression="BROTLI"
    )


def reducer(request_id):
    """Merge parquet files produced by work into a single multi-file parquet
    dataset.
    """
    fs = s3fs.S3FileSystem()

    result_bucket = PARQUET_ENV_VARS["result_bucket"]
    s3_url = f's3://{result_bucket}/{request_id}'

    shards = fs.ls(s3_url)
    # This is the slowest step of the whole process...
    fastparquet.writer.merge(shards, open_with=fs.open)
