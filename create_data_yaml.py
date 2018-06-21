import argparse
import collections
import yaml

import boto3
from botocore import UNSIGNED
from botocore.client import Config


def get_s3_files(bucket_name, prefix):
    """Get a list of files in a bucket that have a particular prefix."""

    s3 = boto3.resource('s3', config=Config(signature_version=UNSIGNED))
    bucket = s3.Bucket(bucket_name)
    all_objs = bucket.objects.filter(Prefix=prefix).all()
    all_keys = {o.key for o in all_objs}
    return all_keys

def main():

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--source-yaml",
        required=True,
        help="The yaml file used to generate the data."
    )
    parser.add_argument(
        "--s3-bucket",
        required=True,
        help="The S3 bucket with the test data."
    )
    args = parser.parse_args()

    source_data = yaml.load(open(args.source_yaml))
    sources = source_data["sources"].keys()
    outputs = source_data["outputs"].keys()


    output_dict = collections.defaultdict(dict)

    # Iterate over all the output format types
    for output in outputs:
        # And all the input data
        for source in sources:

            # We have to be a little careful about how we're finding S3 paths.
            # There are two cases that make it harder: first, the "split"
            # outputs can have many matrices associated with one output type,
            # second, some formats have many files for a single matrix, like
            # zarr
            candidate_s3_path = "/".join(["matrices", output, source]) + "/"
            candidate_s3_files = get_s3_files(args.s3_bucket, candidate_s3_path)

            file_indices = set()
            if source_data["sources"][source]["multiple_matrices"]:
                s3_tuples = set()
                for candidate_s3_file in candidate_s3_files:
                    file_parts = candidate_s3_file.split('/')[:5]
                    prefix = "s3://" + args.s3_bucket
                    format_and_source = '/'.join(file_parts[:3])
                    idx = file_parts[3]
                    file_ = file_parts[4]
                    s3_tuples.add(
                        (prefix, format_and_source, "$idx", file_)
                    )
                    file_indices.add(int(idx))
            else:
                s3_tuples = set()
                for candidate_s3_file in candidate_s3_files:
                    file_parts = candidate_s3_file.split('/')[:4]
                    prefix = "s3://" + args.s3_bucket
                    format_and_source = '/'.join(file_parts[:3])
                    file_ = file_parts[3]
                    s3_tuples.add(
                        (prefix, format_and_source, file_)
                    )

            s3_patterns = ['/'.join(s) for s in s3_tuples]

            if not s3_patterns:
                continue
            elif file_indices:
                output_dict[output][source] = {
                    "indices": sorted(file_indices),
                    "pattern": s3_patterns[0]
                }
            else:
                output_dict[output][source] = s3_patterns[0]

    output_yaml = yaml.dump(output_dict)
    print(output_yaml)


if __name__ == "__main__":
    main()
