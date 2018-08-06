import argparse
import numpy
import yaml

import h5sparse
import scipy.sparse

def merge_hdf5s(hdf5_paths, output_path):

    arrays_to_merge = []
    for hdf5_path in hdf5_paths:
        arrays_to_merge.append(h5sparse.File(hdf5_path)["data"].value)
    merged_array = scipy.sparse.hstack(arrays_to_merge, format="coo")
    output_file = h5sparse.File(output_path, "w", libver="latest")
    output_file.create_dataset("data", data=merged_array.toarray())

def verify_hdf5(matrix_path, test_yaml_path):

    expected_values = yaml.load(open(test_yaml_path))['expected_output']

    output_matrix = h5sparse.File(matrix_path)["data"].value

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
        merge_hdf5s(args.input_paths, args.output_path)
    elif args.subcommand == "verify":
        verify_hdf5(args.output_matrix, args.test_yaml)

if __name__ == "__main__":
    main()
