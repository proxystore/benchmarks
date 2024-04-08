# Simple Globus Compute + ProxyStore Test

**Note:** Globus Compute was formerly called funcX. Read about the change
[here](https://globus-compute.readthedocs.io/en/latest/funcx_upgrade.html).

This benchmark executes and times a series of simple Globus Compute tasks
that take arbitrarily sized byte array inputs and return arbitrarily sized
byte arrays. The input/output sizes and ProxyStore backend can be
specified.

## Setup

1. Create a new virtual environment.
   ```
   $ virtualenv venv
   $ . venv/bin/activate
   ```
2. Install the `psbench` package (must be done from root of repository).
   ```
   $ pip install .
   ```
3. Create a Globus Compute endpoint.
   ```
   $ globus-compute-endpoint configure psbench
   $ globus-compute-endpoint start psbench
   ```
   The returned endpoint UUID will be needed in the next step.

## Benchmark

The benchmark can be configured using CLI parameters.
Here's an example of a minimal working example that uses the ProxyStore
file backend.

```
$ python -m psbench.run.task_rtt \
    --executor globus
    --globus-compute-endpoint {ENDPOINT_UUID} \
    --input-sizes 100 1000 10000 \
    --output-sizes 100 1000 10000 \
    --ps-connector file --ps-file-dir /tmp/proxystore-dump
```

Omitting `--ps-connector` will result in data being passed directly via
Globus Compute. `--input-sizes` and `--output-sizes` take a list of options and
will result in a matrix of tasks being run. Individual task configurations can
be repeated *n* times with the `--repeat` parameter. A sleep can be added
to tasks with `--task-sleep`. Task timing stats are saved to a CSV file
if the run directory.

The full list of options can be found using `--help`.
