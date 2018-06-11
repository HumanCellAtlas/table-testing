import argparse

import h5py

def merge_hdf5s(hdf5_paths, output_path):
    
    first_dset = h5py.File(hdf5_paths[0])["data"]
    output_hfile = h5py.File(output_path, "w")
    output_hfile.create_dataset(
        name="data",
        shape=(first_dset.shape[0], first_dset.shape[1]*len(hdf5_paths)),
        dtype=first_dset.dtype
    )

    for i, hdf5_path in enumerate(hdf5_paths):
        data = h5py.File(hdf5_path)["data"]
        output_hfile["data"][:, i:i+1] = data
    
    output_hfile.close()

def main():

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--input_paths",
        required=True,
        nargs="+",
        help="Paths to the input matrices."
    )
    parser.add_argument(
        "--output_path",
        required=True,
        help="Where to put the result matrix file."
    )

    args = parser.parse_args()

    merge_hdf5s(args.input_paths, args.output_path)

if __name__ == "__main__":
    main()
