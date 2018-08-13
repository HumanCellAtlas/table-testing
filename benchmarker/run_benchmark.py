import argparse
import collections
import concurrent.futures
import os
import pathlib
import shutil
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
        shell=True,
        stdout=subprocess.PIPE)

def ensure_dir(path):
    """Test if directory at path exists, and if not, create it."""
    pathlib.Path(os.path.dirname(path)).mkdir(parents=True, exist_ok=True)

class FileSizeMonitor(threading.Thread):
    """Monitor the size of a file in the background. Measure every second."""

    def __init__(self, file_path):

        threading.Thread.__init__(self)
        self.file_path = file_path
        self.file_sizes = []
        self._exit_event = threading.Event()
        self.start()

    def _get_size(self, path):
        if os.path.isfile(path):
            return os.path.getsize(self.file_path)
        else:
            return sum(
                os.path.getsize(os.path.join(dirpath, filename))
                for dirpath, dirnames, filenames
                in os.walk(path)
                for filename in filenames
            )

    def run(self):
        while not self._exit_event.isSet():
            try:
                self.file_sizes.append(self._get_size(self.file_path))
            except OSError:
                self.file_sizes.append(0)
            time.sleep(1)

    def exit(self):
        self._exit_event.set()
        self.join()

def localize_inputs(inputs, staging_dir):
    """Copy inputs from s3 into the staging dir.

    This is done prior to test execution for tests that expect data to be present in
    a local filesystem.
    """

    s3 = boto3.resource('s3', config=botocore.client.Config(signature_version=botocore.UNSIGNED))

    def localize_input(input_s3_path, staging_dir_):
        """Download the input_s3_path into the staging_dir_."""
        parsed_path = urllib.parse.urlparse(input_s3_path)
        bucket = s3.Bucket(parsed_path.netloc)
        key = parsed_path.path[1:] # String the leading /

        futures = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
            for obj in bucket.objects.filter(Prefix=key).all():
                local_path = os.path.join(staging_dir_, obj.key)
                if os.path.exists(local_path):
                    continue
                ensure_dir(local_path)
                futures.append(executor.submit(bucket.download_file, obj.key, local_path))
                #bucket.download_file(obj.key, local_path)
        _ = [f.result() for f in futures]

        return os.path.join(staging_dir_, key)


    if isinstance(inputs, list):
        localized_inputs = []
        for input_ in inputs:
            localized_inputs.append(localize_input(input_, staging_dir))
        return localized_inputs

    return localize_input(inputs, staging_dir)

def s3fs_mount_inputs(inputs, staging_dir):
    """Use s3fs to mount the bucket with the inputs and treat them like local files."""

    # check that we have s3fs and say something helpful if we don't
    try:
        subprocess.run(["s3fs", "-h"], check=True, stdout=subprocess.PIPE)
    except FileNotFoundError:
        raise RuntimeError("s3fs isn't installed, so we can't try s3-fuse. "
                           "Try apt-get install s3fs")

    # Just run s3fs for the whole bucket and them update the paths
    if isinstance(inputs, list):
        input_s3_path = inputs[0]
    else:
        input_s3_path = inputs
    parsed_path = urllib.parse.urlparse(input_s3_path)
    bucket_name = parsed_path.netloc
    staging_input_dir = os.path.join(staging_dir, "inputs/")
    ensure_dir(staging_input_dir)
    s3fs_cmd = ["s3fs", bucket_name, staging_input_dir, "-o", "public_bucket=1",
                "-o", "allow_other", "-o", "max_stat_cache_size=5000"]
    print(" ".join(s3fs_cmd))
    subprocess.run(s3fs_cmd, check=True)

    def get_local_input_path(input_):
        """Assuming s3fs has been run, return the locally-mounted path for the input_."""
        parsed_path = urllib.parse.urlparse(input_)
        key = parsed_path.path[1:]
        local_path = os.path.join(staging_input_dir, key)
        return local_path

    if isinstance(inputs, list):
        localized_inputs = []
        for input_ in inputs:
            localized_inputs.append(get_local_input_path(input_))
        return localized_inputs

    return get_local_input_path(inputs)


def run_test_repetition(docker_image_name, test_dir, input_paths, test_yaml_path, repetition):
    """Execute one repetition of a test. Return the time it took to complete."""

    # Try to clear the pagecache.
    drop_caches()

    output_path = os.path.join(test_dir, "output_{}".format(repetition))
    ensure_dir(output_path)
    file_monitor = FileSizeMonitor(output_path)

    # Run and time the test repetition
    test_cmd = ["test", "--input-paths"]
    test_cmd.extend(input_paths)
    test_cmd.append("--output-path")
    test_cmd.append(output_path)

    start_time = time.perf_counter()
    DOCKER_CLIENT.containers.run(
        image=docker_image_name,
        command=' '.join(test_cmd),
        volumes={test_dir: {"bind": test_dir, "mode": "rw"}}
    )
    end_time = time.perf_counter()
    file_monitor.exit()

    test_time = end_time - start_time

    # Write the timing results to a file
    results_log_path = os.path.join(test_dir, "timing_results_{}.log".format(repetition))
    with open(results_log_path, "w") as results_log:
        results_log.write(str(test_time) + "\n")
        for size in file_monitor.file_sizes:
            results_log.write(str(size) + "\n")

    # And finally, verify the output
    shutil.copy(test_yaml_path, os.path.join(test_dir, "test.yaml"))

    verify_cmd = ["verify", "--output-matrix"]
    verify_cmd.append(output_path)
    verify_cmd.append("--test-yaml")
    verify_cmd.append(os.path.join(test_dir, "test.yaml"))
    DOCKER_CLIENT.containers.run(
        image=docker_image_name,
        command=' '.join(verify_cmd),
        volumes={test_dir: {"bind": test_dir, "mode": "rw"}}
    )
    print(test_time)
    return test_time

def run_test(test_path, data_yaml_path, repetitions=10, local_staging_dir=None):
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

    test_config = yaml.load(open(os.path.join(test_path, "test.yaml")))
    data_config = yaml.load(open(data_yaml_path))

    test_staging_dir = local_staging_dir or tempfile.mkdtemp()
    if not os.path.isabs(test_staging_dir):
        raise RuntimeError("Staging path must be absolute: {}".format(test_staging_dir))

    test_running_times = collections.defaultdict(collections.defaultdict)

    # Iterate over the source and format combinations specified in the test's
    # config yaml
    for source in test_config["sources"]:
        for format_ in test_config["formats"]:
            test_instance_dir = os.path.join(test_staging_dir, source, format_)

            # Get the s3 paths to the inputs for this source/format combo
            inputs = data_config[format_][source]

            if isinstance(inputs, dict):
                inputs_ = [inputs["pattern"].replace("$idx", str(i)) for i in inputs["indices"]]
                inputs = inputs_

            # If the tests are supposed to run off of local files, localize the
            # remote s3 files first.
            if test_config["file_location"] == "local":
                inputs = localize_inputs(inputs, test_instance_dir)
            elif test_config["file_location"] == "s3fs":
                inputs = s3fs_mount_inputs(inputs, test_instance_dir)
            print("Done localizing to", test_instance_dir)

            # Build the image that runs the test
            image, _ = DOCKER_CLIENT.images.build(
                path=test_path, tag="matrix_benchmark_{}_{}".format(source, format_).lower())
            image_name = image.tags[0]
            print("Built", image_name)

            running_times = [
                run_test_repetition(image_name, test_instance_dir, inputs,
                                    os.path.join(test_path, "test.yaml"), r)
                for r in range(repetitions)]
            test_running_times[source][format_] = running_times

    return test_running_times

def run_tests(test_dir, data_yaml_path, repetitions=10, local_staging_dir=None):
    """Discover tests by recursing through test_dir. Run each test repetitions
    times and report running times.
    """

    all_running_times = {}
    for candidate_test_yaml in pathlib.Path(test_dir).glob("**/test.yaml"):
        candidate_test_path = candidate_test_yaml.parent
        print(candidate_test_yaml, candidate_test_path)
        if candidate_test_path.joinpath("Dockerfile").exists():
            running_times = run_test(str(candidate_test_path), data_yaml_path,
                                     repetitions, local_staging_dir)
            all_running_times[candidate_test_path] = running_times
    print(all_running_times)
    return all_running_times

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--test-root",
        required=True,
        help=("Path to the directory containing the test(s). Will walk "
              "subdirectories looking for valid tests.")
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
    parser.add_argument(
        "--repetitions",
        default=10,
        type=int,
        help="Number of times to repeat each test."
    )
    args = parser.parse_args()

    output_file_sizes = run_tests(
        args.test_root, args.data_yaml, args.repetitions, args.local_staging_dir)
    print(output_file_sizes)

if __name__ == "__main__":
    main()
