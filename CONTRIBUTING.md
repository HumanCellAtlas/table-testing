## How to contribute

Thanks for taking the time to contribute to `table-testing`! We appreciate it.

This repository contains tests and benchmarks for different expression matrix file formats. The goal
is to help the community identify what formats or features of formats are desirable to support
scalable querying and analysis of gene expression data.

There are three main ways to contribute to this effort:
- Adding new file formats or data sources for consideration
- Writing new tests for different formats or use cases
- Improving the test execution code

### Adding new formats and sources

The [create_data/data.yaml](create_data/data.yaml) file describes the set of
expression matrices that will be created and available for testing. There are
two parts to that file, `sources` and `formats`. The `sources` describe how to
retrieve raw expression data from somewhere on the internet and annotate the
format in which they are stored. So for example, this entry

```yaml
immune_cell_census_cord_blood:
  url: https://s3.amazonaws.com/preview-ica-expression-data/ica_cord_blood_h5.h5
  type: 10xh5
  multiple_matrices: false
  args:
    genome: GRCh38
```

says there is an expression matrix at that url of type `10xh5`. The "type"
means that there is a function in
[create_data/converters.py](create_data/converters.py) called
`convert_from_10xh5` that accepts the downloaded matrix file as input and
returns a pandas dataframe. The `args` entry means that that function should
receive an additional keyword argument `genome="GRCh38"`.

So to add a data source, create a pull request that
1. Adds an entry to `sources` in
   [create_data/data.yaml](create_data/data.yaml).
2. If a conversion function for the data format does not already exist, add one
   in [create_data/converters.py](create_data/converters.py) with the name
   `convert_from_xxxxx` that matches the `type` in the data.yaml file.

Adding a new output format is similar. There is an `outputs` entry in data.yaml
with entries like this:

```yaml
sparse_hdf5_csc:
  format: sparse_hdf5
  args:
    major: csc
```

This will create a matrix file from all of the data sources using a function
called `convert_to_sparse_hdf5` in
[create_data/converters.py](create_data/converters.py) with the additional
argument `major="csc"` passed to it. So adding a new output format requires a
pull request that 

1. Adds an entry to `outputs` in
   [create_data/data.yaml](create_data/data.yaml).
2. Adds a function to [create_data/converters.py](create_data/converters.py) 
   named `convert_to_xxxx` that matches the `format` in the data.yaml file.

### Adding tests

Tests are defined in subdirectories of [tasks](tasks). They perform certain
"tasks" that address different expression matrix use cases using one or more
of the matrix data sources and formats.  

Tests have two components: they have a docker container that exposes a
particular interface, and they have a yaml file that provides some information
about test inputs and expected outputs. So to create a new test, create or
find a directory in [tasks](tasks) for the particular use case you want to
test. Then, create a subdirectory there that will define the docker image that
runs the test. That subdirectory has three components:

1. test.yaml - A yaml file that defines the data sources and formats on which
   the test should run. There are four required entries:
   - file_location: either "local" or "remote". "local" means the files should
     be localized before the test is run.
   - sources: The matrix data sources on which the test should be run
   - formats: The matrix formats on which the test should run. The test will
     run on each (source, format) combination
   - expected_output: A few expected properties of the output matrix: its
     shape, sum, and number of nonzero elements.
2. Dockerfile - The dockerfile is reponsible for creating the environment where
   the test can run, so it installs dependencies and defines and entrypoint.
3. Additional image files (optional) - Files to be included in the test's
   docker image, including, for example, scripts to run the test.

The docker image should define an entrypoint with two subcommands: `test` and
`verify`. The  `test` command has the following required arguments: 

- `--input-paths`: One or more paths to the input matrix files
- `--output-path`: Path where the test should write the output matrix

The `verify` command has the following required arguments:

- `--output-matrix`: Path to matrix to be verified
- `--test-yaml`: Path to the test.yaml file

Note that these paths refer to locations within the docker container, so they
need to match the paths to mounted volumes.

### Improving test execution

The code for actually running the tests is in
[benchmarker/run_benchmark.py](benchmarker/run_benchmark.py). The general goal
is to run the tests multiple times and record the running times. Suggestions
and improvements are welcome!
