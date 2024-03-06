# Task Pipelining

This benchmark executes a workflow consisting of a sequence of sleep tasks
with data dependencies in a sequential and pipelined fashion. In the sequental
mode, task `n+1` is not submitted until task `n` has completed.
In the pipelined mode, task `n+1` is submitted before task `n` completes and
the output of task `n` is communicated to task `n+1` with a proxy future.

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
$ python -m psbench.run.task_pipelining \
    --executor dask --dask-workers 2 \
    --ps-connector file --ps-file-dir runs/cache \
    --submission-method sequential pipelined \
    --task-chain-length 5 \
    --task-data-bytes 1000 10000 10000 \
    --task-overhead-fractions 0.01 0.1 0.2 0.5 \
    --task-sleep 1.0 \
    --repeat 5
```

This configuration uses a Dask local process cluster with four workers and a ProxyStore configured to use a local `FileConnector`.
Run data will be saved to a CSV file that is printed at the end of the run.
Each "workflow" will consist of a chain of five tasks where each task will sleep for a total of 1.0 seconds.
The first sleep simulates overhead and will be `task-overhead-fraction` of the total sleep time.
After, the input will be resolved, and then the task will sleep again to finish the simulated computation.
The second sleep lasts for `1 - task-overhead-fraction` of the total sleep.
Each workflow will be executed twice, once sequential and once pipelined, and this processes will be repeated for each data size (here, 1kB, 10kB, 100kB, and 1MB).
Every configuration will be repeated five times as set by `--repeat`.
In total, there will be `2 * 3 * 4 * 5 = 120` runs, each taking around 5 seconds (chain of five, one second long tasks).

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
