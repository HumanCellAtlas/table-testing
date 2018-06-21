import argparse
import collections
import os
import pathlib
import subprocess
import tempfile
import threading
import time
import urllib.parse

import boto3
import botocore
import docker
import yaml


DOCKER_CLIENT = docker.from_env()

def drop_caches():
    """Clear the page cache so we're actually reading from disk.

    This unfortunately requires sudo access.
    """

    subprocess.run(
        "echo 3 | sudo tee /proc/sys/vm/drop_caches",
        shell=True)

def ensure_dir(path):
    pathlib.Path(os.path.dirname(path)).mkdir(parents=True, exist_ok=True)

class FileSizeMonitor(threading.Thread):
    """Monitor the size of a file in the background. Measure twice per second."""

    def __init__(self, file_path):

        threading.Thread.__init__(self)
        self.file_path = file_path
        self.file_sizes = []
        self._exit_event = threading.Event()
        self.start()

    def run(self):
        while not self._exit_event.isSet():
            try:
                self.file_sizes.append(os.path.getsize(self.file_path))
            except OSError:
                self.file_sizes.append(0)
            time.sleep(.1)

    def exit(self):
        self._exit_event.set()
        self.join()

def localize_inputs(inputs, staging_dir):

    s3 = boto3.resource('s3', config=botocore.client.Config(signature_version=botocore.UNSIGNED))

    def localize_input(input_s3_path, staging_dir_):
        parsed_path = urllib.parse.urlparse(input_s3_path)
        bucket = s3.Bucket(parsed_path.netloc)
        key = parsed_path.path[1:] # String the leading /

        for obj in bucket.objects.filter(Prefix=key).all():
            local_path = os.path.join(staging_dir_, obj.key)
            ensure_dir(local_path)
            bucket.download_file(obj.key, local_path)

        return os.path.join(staging_dir_, key)


    if isinstance(inputs, list):
        localized_inputs = []
        for input_ in inputs:
            localized_inputs.append(localize_input(input_, staging_dir))
        return localized_inputs

    return localize_input(inputs, staging_dir)

def run_test(test_dir, data_yaml_path, local_staging_dir=None):
    """Run a test. Get timing results for the specified matrix formats.

    Args:
      test_dir: path to the test directory. Should have a test.yaml and a Dockerfile
      data_yaml_path: path to the yaml file that describes the available data formats
        in s3
      local_staging_dir: where inputs and outputs should be staged

    Returns:
      output_file_sizes: dict of output produced over time for each of the
        source-format combinations
    """

    test_config = yaml.load(open(os.path.join(test_dir, "test.yaml")))
    data_config = yaml.load(open(data_yaml_path))

    test_staging_dir = local_staging_dir or tempfile.mkdtemp()
    if not os.path.isabs(test_staging_dir):
        raise RuntimeError("Staging path must be absolute: {}".format(test_staging_dir))

    output_file_sizes = collections.defaultdict(collections.defaultdict)

    # Iterate over the source and format combinations specified in the test's
    # config yaml
    for source in test_config["sources"]:
        for format_ in test_config["formats"]:

            sf_test_dir = os.path.join(test_staging_dir, source, format_)
            # Get the s3 paths to the inputs for this source/format combo
            inputs = data_config[format_][source]

            if isinstance(inputs, dict):
                inputs_ = [inputs["pattern"].replace("$idx", str(i)) for i in inputs["indices"]]
                inputs = inputs_

            # If the tests are supposed to run off of local files, localize the
            # remote s3 files first.
            if test_config["file_location"] == "local":
                inputs = localize_inputs(inputs, sf_test_dir)

            # Build the image that runs the test
            image, _ = DOCKER_CLIENT.images.build(path=test_dir, tag="matrix_benchmark")
            image_name = image.tags[0]

            drop_caches()

            # Set up the output path and the thread that monitors its size
            output_path = os.path.join(sf_test_dir, "output")
            ensure_dir(output_path)
            file_monitor = FileSizeMonitor(output_path)

            # Actually run the test
            cmd = ["--input_paths"]
            cmd.extend(inputs)
            cmd.append("--output_path")
            cmd.append(output_path)
            DOCKER_CLIENT.containers.run(
                image=image_name,
                command=' '.join(cmd),
                volumes={sf_test_dir: {"bind": sf_test_dir, "mode": "rw"}}
            )

            file_monitor.exit()
            print(file_monitor.file_sizes)
            output_file_sizes[source][format_] = file_monitor.file_sizes

    return output_file_sizes


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--test-dir",
        required=True,
        help="Path to the directory containing the test."
    )
    parser.add_argument(
        "--data-yaml",
        required=True,
        help="Path to yaml describing available data."
    )
    parser.add_argument(
        "--local-staging-dir",
        required=False,
        help="Where to stage files for local test. Defaults to mkdtemp."
    )
    args = parser.parse_args()

    output_file_sizes = run_test(args.test_dir, args.data_yaml, args.local_staging_dir)
    print(output_file_sizes)

if __name__ == "__main__":
    main()
