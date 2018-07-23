import argparse
import numpy
import yaml

import zarr

def merge_zarrs(zarr_paths, output_path):

    arrays_to_merge = []
    for zarr_path in zarr_paths:
        arrays_to_merge.append(zarr.open(zarr_path))
    print("Opened", len(arrays_to_merge), "arrays")
    print(arrays_to_merge[0])
    print(arrays_to_merge[0].shape)
    merged_array = numpy.concatenate(arrays_to_merge, axis=1)
    print(merged_array.shape)
    output_zarr = zarr.save(output_path, merged_array)

def verify_zarrs(matrix_path, test_yaml_path):

    expected_values = yaml.load(open(test_yaml_path))['expected_output']

    output_matrix = zarr.open(matrix_path)

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
        merge_zarrs(args.input_paths, args.output_path)
    elif args.subcommand == "verify":
        verify_zarrs(args.output_matrix, args.test_yaml)

if __name__ == "__main__":
    main()
