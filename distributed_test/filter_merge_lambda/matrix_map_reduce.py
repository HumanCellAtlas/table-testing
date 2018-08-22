"""Lambdas to demonstrate a distributed filter and merge of expression matrices
using different file formats.

By "filter and merge", I mean a query along the lines of "in this list of matrices,
find me all the cells that express CD4 and have a qc value greater than .6, and merge
them together".

This is an experiment implementing such a query on lambdas. Lambdas are very burstable
and horizontally scalable, but not all tasks can be subdivided into subtasks that can
run on small, short-lived workers.

The query is kicked off with a request the the following fields:
    - inputs: a list lf {"bucket": bucket, "prefix": prefix} objects that refer
      to the input matrices
    - format: the format of those matrices. Has to match something in
      FORMAT_HANDLERS below
    - filter_string: a pandas-like filter where "matrix" refers to the
      expression matrix. For the query above, it would be
      "matrix['CD4'] > 0 & matrix['qc'] > .6"

There is an attempt to separate the orchestration concerns from the file manipulation.
There are four lambda functions:
    - driver: accepts the request, sticks an id on it, kicks off other lambdas
      and returns. This is just to separate what a "user" of the test framework works
      with from the execution of the filter and merge.
    - mapper: Takes one input file and figures out how to distribute it in
      small enough chunks to worker lambdas that will perform the filtering
    - do_work: perform the filtering on a chunk of work created by mapper
    - reducer: merge all the work chunks together into a single matrix

For some formats, one or more of these steps may be trivial.

This means that a particular format needs to implement three functions that these lambdas
will call:

    - mapper(request_id, bucket, prefix) -> List of work_chunk_spec dicts
    - work(request_id, filter_string, **work_chunk_spec)
    - reducer(request_id)

(This is all slightly overfit to parquet at the moment. Also, it uses a dynamodb to
manage global state, which.....works?)
"""

import datetime
import json
import os
import uuid

import boto3
from aws_xray_sdk.core import xray_recorder
from aws_xray_sdk.core import patch
patch(["boto3"])

import dynamo_utils

# These are set in the cloudformation template
CFN_VARS = {
    "mapper_fn": os.environ.get("MAPPER_FN"),
    "work_fn": os.environ.get("WORK_FN"),
    "reducer_fn": os.environ.get("REDUCER_FN"),
    "state_table": os.environ.get("STATE_TABLE"),
    "timing_table": os.environ.get("TIMING_TABLE"),
    "result_bucket": os.environ.get("RESULT_BUCKET")
}

# A couple convenient interfaces to aws
LAMBDA_CLIENT = boto3.client("lambda", region_name="us-east-1")
STATE_TABLE = boto3.resource("dynamodb", region_name="us-east-1").Table(CFN_VARS["state_table"])
TIMING_TABLE = boto3.resource("dynamodb", region_name="us-east-1").Table(CFN_VARS["timing_table"])

# Record functions that handle different concerns for different formats here
FORMAT_HANDLERS = {}
try:
    import parquet_impl
    FORMAT_HANDLERS["parquet"] = {
        "driver": parquet_impl.driver,
        "mapper": parquet_impl.mapper,
        "work": parquet_impl.work,
        "reducer": parquet_impl.reducer
    }
except ImportError:
    pass

try:
    import zarr_impl
    FORMAT_HANDLERS["zarr"] = {
        "driver": zarr_impl.driver,
        "mapper": zarr_impl.mapper,
        "work": zarr_impl.work,
        "reducer": zarr_impl.reducer
    }
except ImportError:
    pass


def increment_state_field(request_id, field_name, increment_size):
    """Increment a field in the state table.

    This is used to keep track of how many lambda executions have completed and are
    expected to complete.
    """
    return dynamo_utils.increment_field(
        CFN_VARS["state_table"], {"RequestId": request_id}, field_name, increment_size)

def record_timing_event(request_id, event_name):
    """Record the time of an event."""
    timestamp = datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    TIMING_TABLE.update_item(
        Key={"RequestId": request_id},
        UpdateExpression=f"SET {event_name} = :t",
        ExpressionAttributeValues={":t": timestamp}
    )



def driver(event, context):
    """Initiate the matrix filter and merge. Return quickly after kicking off
    the work.

    Assume this function is invoked via an API Gateway lambda proxy
    integration.
    """

    body = event["body"]

    # Expect body to look like this
    # {
    #  "format": "parquet_simple",
    #  "inputs": [
    #    {"bucket": bucket_name, "prefix": prefix},
    #    ...
    #  ],
    #  "filter_string": "matrix['CD4'] > 0 & matrix['qc11'] < .25"
    # }
    # Do we have the keys we expect?
    missing_keys = [k for k in ["format", "inputs", "filter_string"] if k not in body]
    if missing_keys:
        return {
            "statusCode": "400",
            "body": json.dumps({
                "msg": "Missing required keys in body: {}".format(missing_keys),
            })
        }

    # Do we know what to do with this format?
    format_ = body["format"]
    if format_ not in FORMAT_HANDLERS:
        return {
            "statusCode": "400",
            "body": json.dumps({
                "msg": "Format {} not recognized".format(format_),
            })
        }

    # Okay, let's go for it
    request_id = str(uuid.uuid4())

    record_timing_event(request_id, "DriverStarted")
    # Record the request in the state table
    STATE_TABLE.put_item(
        Item={
            "RequestId": request_id,
            "ExpectedWorkExecutions": 0,
            "CompletedWorkExecutions": 0,
            "ExpectedMapperExecutions": len(body["inputs"]),
            "CompletedMapperExecutions": 0,
            "ExpectedReducerExecutions": 1,
            "CompletedReducerExecutions": 0
        }
    )

    # If we're working with zarr, initialize the output table for this request

    # Run mappers on each input
    for input_ in body["inputs"]:
        mapper_payload = {
            "request_id": request_id,
            "bucket": input_["bucket"],
            "prefix": input_["prefix"],
            "format": format_,
            "filter_string": event["body"]["filter_string"]
        }

        LAMBDA_CLIENT.invoke(
            FunctionName=CFN_VARS["mapper_fn"],
            InvocationType="Event",
            Payload=json.dumps(mapper_payload).encode()
        )

    format_driver_fn = FORMAT_HANDLERS[format_]["driver"]
    format_driver_fn(request_id)

    # And return the request id to the caller
    record_timing_event(request_id, "DriverComplete")
    return {
        "statusCode": "200",
        "body": json.dumps({"request_id": request_id})
    }


def mapper(event, context):
    """Distribute work from one (bucket, prefix) pair to worker lambdas."""

    format_mapper_fn = FORMAT_HANDLERS[event["format"]]["mapper"]
    print(f"Calling {format_mapper_fn.__name__}")
    work_chunk_specs = format_mapper_fn(event["request_id"], event["bucket"], event["prefix"])
    print(f"work_chunk_specs {work_chunk_specs}")

    increment_state_field(event["request_id"], "ExpectedWorkExecutions", len(work_chunk_specs))

    for work_chunk_spec in work_chunk_specs:
        work_payload = {
            "request_id": event["request_id"],
            "format": event["format"],
            "filter_string": event["filter_string"],
            "work_chunk_spec": work_chunk_spec
        }
        LAMBDA_CLIENT.invoke(
            FunctionName=CFN_VARS["work_fn"],
            InvocationType="Event",
            Payload=json.dumps(work_payload).encode()
        )

    increment_state_field(event["request_id"], "CompletedMapperExecutions", 1)

def work(event, context):
    """Filter one work chunk."""

    format_work_fn = FORMAT_HANDLERS[event["format"]]["work"]

    work_chunk_spec = event["work_chunk_spec"]

    format_work_fn(event["request_id"], event["filter_string"], **work_chunk_spec)

    increment_state_field(event["request_id"], "CompletedWorkExecutions", 1)

    # Are we all done? Then run the reducer
    item = STATE_TABLE.get_item(
        Key={"RequestId": event["request_id"]},
        ConsistentRead=True
    )

    done_mapping = (item["Item"]["ExpectedMapperExecutions"] ==
                    item["Item"]["CompletedMapperExecutions"])

    done_working = (item["Item"]["ExpectedWorkExecutions"] ==
                    item["Item"]["CompletedWorkExecutions"])

    if done_mapping and done_working:

        record_timing_event(event["request_id"], "WorkComplete")
        reducer_payload = {
            "request_id": event["request_id"],
            "format": event["format"]
        }
        LAMBDA_CLIENT.invoke(
            FunctionName=CFN_VARS["reducer_fn"],
            InvocationType="Event",
            Payload=json.dumps(reducer_payload).encode()
        )

def reducer(event, context):
    """Combine results from workers into a single result."""

    format_reducer_fn = FORMAT_HANDLERS[event["format"]]["reducer"]
    format_reducer_fn(event["request_id"])
    increment_state_field(event["request_id"], "CompletedReducerExecutions", 1)
    record_timing_event(event["request_id"], "ReduceComplete")
