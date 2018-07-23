import argparse
import numpy
import yaml

import scipy.io
import scipy.sparse

def merge_matrix_markets(matrix_market_paths, output_path):

    arrays_to_merge = []
    for matrix_market_path in matrix_market_paths:
        arrays_to_merge.append(scipy.io.mmread(matrix_market_path))
    merged_array = scipy.sparse.hstack(arrays_to_merge)
    output_matrix_market = scipy.io.mmwrite(output_path, merged_array)

def verify_matrix_markets(matrix_path, test_yaml_path):

    expected_values = yaml.load(open(test_yaml_path))['expected_output']

    output_matrix = scipy.io.mmread(matrix_path).toarray()

    assert numpy.count_nonzero(output_matrix) == expected_values["non_zero_count"]
    assert numpy.sum(output_matrix) == expected_values["sum"]
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
        merge_matrix_markets(args.input_paths, args.output_path)
    elif args.subcommand == "verify":
        verify_matrix_markets(args.output_matrix, args.test_yaml)

if __name__ == "__main__":
    main()
