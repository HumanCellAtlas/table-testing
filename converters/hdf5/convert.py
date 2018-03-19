#!/usr/bin/env python3
import argparse

import h5py
import scipy.io


def read_mtx(mtx_filename, rows_filename, cols_filename):
    """Read the mtx file into scipy matrix."""

    #TODO: Figure out how we want to handle row and column attibutes.

    mat = scipy.io.mmread(mtx_filename)

    return mat

def write_hdf5(mat, output_filename, chunks=None, compression=None):
    """Write a scipy sparse matrix to an hdf5 file."""

    kwargs = {}
    if chunks:
        # This is supposed to be a tuple, so just assume we'll get something
        # like '(10, 10)' and eval it
        kwargs["chunks"] = eval(chunks)
    if compression:
        kwargs["compression"] = compression

    h5_file = h5py.File(output_filename, "w")
    h5_file.create_dataset("data", data=mat.todense(), **kwargs)
    h5_file.close()

    return output_filename

def main():

    parser = argparse.ArgumentParser()
    parser.add_argument("--in-mtx", required=True)
    parser.add_argument("--in-rows", required=False)
    parser.add_argument("--in-cols", required=False)
    parser.add_argument("--out-filename", required=True)
    parser.add_argument("--h5-chunks", required=False)
    parser.add_argument("--h5-compression", required=False)

    args = parser.parse_args()

    mat = read_mtx(args.in_mtx, None, None)

    write_hdf5(mat, args.out_filename, chunks=args.h5_chunks, compression=args.h5_compression)

if __name__ == '__main__':
    main()
