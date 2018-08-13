import argparse
import numpy
import s3fs
import yaml

def merge_npys(npy_paths, output_path):

    s3 = s3fs.S3FileSystem(anon=True, client_kwargs=dict(region_name='us-east-1'))

    arrays_to_merge = []
    for npy_path in npy_paths:
        s3_path = npy_path[len("s3://"):]
        arrays_to_merge.append(numpy.load(s3.open(s3_path, 'rb')))
    merged_array = numpy.concatenate(arrays_to_merge, axis=1)
    numpy.save(output_path, merged_array)

def verify_npys(matrix_path, test_yaml_path):

    expected_values = yaml.load(open(test_yaml_path))['expected_output']

    output_matrix = numpy.load(matrix_path + ".npy")

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
        merge_npys(args.input_paths, args.output_path)
    elif args.subcommand == "verify":
        verify_npys(args.output_matrix, args.test_yaml)

if __name__ == "__main__":
    main()
