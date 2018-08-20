import argparse
import numpy
import os
import pathlib
import shutil
import urllib.request
import yaml

import converters


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-yaml", help="YAML file describing data inputs and outputs.",
                        required=True)
    parser.add_argument("--data-path", help="Path to put files.", required=True)
    args = parser.parse_args()

    data_dict = yaml.load(open(args.data_yaml))

    sources = data_dict["sources"]
    outputs = data_dict["outputs"]

    for source in sources:

        # Get the remote file and put it in data/sources/<name>
        url = sources[source]["url"]

        source_path = os.path.join(
            args.data_path,
            "sources",
            source,
            os.path.basename(url)
        )

        pathlib.Path(os.path.dirname(source_path)).mkdir(parents=True)
        urllib.request.urlretrieve(url, filename=source_path)

        # Get the method to convert this file to dataframes
        convertfrom_method = getattr(converters, "convert_from_" + sources[source]["type"])

        # Iterate over converted dataframes
        df_counter = 0
        for dataframe in convertfrom_method(source_path, **sources[source].get("args", {})):

            # Iterate over expected output formats
            for output in outputs:
                convertto_method = getattr(converters, "convert_to_" + outputs[output]["format"])

                # The convertto_method returns a path to a matrix file or
                # directory, we'll want to move that to the data_path
                tmp_matrix_path = convertto_method(
                    dataframe,
                    **outputs[output].get("args", {}))
                ext = os.path.splitext(tmp_matrix_path)[1]

                matrix_path = os.path.join(
                    args.data_path,
                    "matrices",
                    output,
                    source,
                    str(df_counter) if sources[source]["multiple_matrices"] else "",
                    "{}_{}{}".format(output, source, ext)
                )

                pathlib.Path(os.path.dirname(matrix_path)).mkdir(parents=True)
                shutil.move(tmp_matrix_path, matrix_path)
            df_counter += 1

if __name__ == "__main__":
    main()
