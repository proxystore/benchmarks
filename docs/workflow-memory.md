# Workflow Memory Usage

This benchmark executes a simulated workflow and records simulated memory
usage. There are three data management modes: "default" where task parameters
and results are left to the workflow executor to manage, "default-proxy"
which transfer task parameters and results via basic proxies, and
"owned-proxy" which uses owned proxies and references to manage parameters and
results.

## Setup

1. Create a new virtual environment.
   ```bash
   $ python -m venv venv
   $ . venv/bin/activate
   ```
2. Install the `psbench` package (must be done from root of repository).
   ```bash
   $ pip install .
   ```

## Benchmark

The benchmark can be configured using CLI parameters. The full list of options
can be found using `--help`.

```bash
$ python -m psbench.run.workflow_memory \
    --executor dask --dask-workers 4 \
    --ps-connector file --ps-file-dir runs/cache \
    --data-management none default-proxy owned-proxy \
    --stage-task-counts 1 3 1 1 \
    --stage-bytes-sizes 1000 10000 10000 10000 1000 \
    --task-sleep 1.0 \
    --repeat 5
```

This configuration uses a Dask local process cluster with four workers and ProxyStore configured to use a local `FileConnector`.
Each run will execute a workflow consisting of four stages with 1, 3, 1, and 1 tasks in each stage, respectively.
The `stage-bytes-sizes` parameter controls the input and output sizes for the tasks in each stage.
Each task in the workflow will resolve it's input data, sleep for 1.0 seconds, and generate result data.
Each workflow will be repeated for each data management method, and each of those unique configurations will be repeated five times.
In total, there will be `3 * 5 = 15` runs, each taking around 4 seconds (each workflow stage should take around one second).

Two CSV files will be saved at the end to `--run-dir` (defaults to `runs/`):
a results log with one line for each workflow run with the start and end timestamps of the run,
and a memory log with the system memory usage recorded every `--memory-profile-interval` seconds.
The start and end timestamps of each workflow can be used to extract the memory profile from the memory log.

### Executors

The task executor can be changed with CLI options. Some examples include:

* Globus Compute:
  ```bash
  --executor globus --globus-compute-endpoint <UUID>
  ```
* Parsl `ThreadPoolExecutor`:
  ```bash
  --executor parsl --parsl-executor thread --parsl-max-workers 2
  ```
* Parsl Local `HighThroughputExecutor`:
  ```bash
  --executor parsl --parsl-executor htex-local --parsl-max-workers 4
  ```

### ProxyStore

The ProxyStore connector can be configured using the `--ps-connector`
option. Some choices for `--ps-connector` will mark additional CLI options
as required.
