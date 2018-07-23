import argparse
import numpy
import yaml

import scanpy.api as sc

def merge_anndatas(anndata_paths, output_path):

    first_adata = sc.read_h5ad(anndata_paths[0])
    concat_adata = first_adata.concatenate(sc.read_h5ad(a) for a in anndata_paths[1:])
    concat_adata.write(output_path)


def verify_anndata(matrix_path, test_yaml_path):

    expected_values = yaml.load(open(test_yaml_path))['expected_output']

    output_matrix = sc.read_h5ad(matrix_path).X.T

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
        merge_anndatas(args.input_paths, args.output_path)
    elif args.subcommand == "verify":
        verify_anndata(args.output_matrix, args.test_yaml)

if __name__ == "__main__":
    main()
