import argparse
import numpy
import yaml

import pandas

def merge_feathers(feather_paths, output_path):

    dfs_to_merge = []
    for feather_path in feather_paths:
        print(feather_path)
        dfs_to_merge.append(pandas.read_feather(feather_path).drop("index", 1))
    merged_df = pandas.concat(dfs_to_merge, axis=1)
    merged_df.to_feather(output_path)

def verify_feathers(matrix_path, test_yaml_path):

    expected_values = yaml.load(open(test_yaml_path))['expected_output']

    output_matrix = pandas.read_feather(matrix_path)

    assert numpy.count_nonzero(output_matrix.as_matrix()) == expected_values["non_zero_count"]
    assert numpy.sum(output_matrix.as_matrix()) == expected_values["sum"]
    assert tuple(output_matrix.shape) == tuple(expected_values["shape"])

def main():

    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="subcommand")

    test_group = subparsers.add_parser("test", help="Run the test.")
    verify_group = subparsers.add_parser("verify", help="Verify the test output.")

    test_group.add_argument(
        "--input-paths",
        required=True,
        nargs="+",
        help="Paths to the input matrices."
    )
    test_group.add_argument(
        "--output-path",
        required=True,
        help="Where to put the result matrix file."
    )

    verify_group.add_argument(
        "--output-matrix",
        required=True,
        help="Path to the matrix produced by the test."
    )
    verify_group.add_argument(
        "--test-yaml",
        required=True,
        help="Path ."
    )
    args = parser.parse_args()

    if args.subcommand == "test":
        merge_feathers(args.input_paths, args.output_path)
    elif args.subcommand == "verify":
        verify_feathers(args.output_matrix, args.test_yaml)

if __name__ == "__main__":
    main()
