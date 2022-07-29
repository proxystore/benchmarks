# Simple FuncX + ProxyStore Test

This benchmark executes and times a series of simple FuncX tasks
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
3. Install the dependencies.
   ```
   $ pip install -r requirements.txt
   ```
4. Create a FuncX endpoint.
   ```
   $ funcx-endpoint configure psbench
   $ funcx-endpoint start psbench
   ```
   The returned endpoint UUID will be needed in the next step.

## Benchmark

The benchmark can be configure using CLI parameters.
Here's an example of a minimal working example that uses the ProxyStore
file backend.

```
$ python benchmark.py \
    --funcx-endpoint {UUID} \  # UUID returned by funcx-endpoint start
    --input-sizes 100 1000 10000 \
    --output-sizes 100 1000 10000 \
    --ps-backend FILE --ps-file-dir /tmp/proxystore-dump
```

Omitting `--ps-backend` will result in data being passed directly via
FuncX. `--input-sizes` and `--output-sizes` take a list of options and will
result in a matrix of tasks being run. Individual task configurations can
be repeated *n* times with the `--task-repeat` parameter. A sleep can be added
to tasks with `--task-sleep`. Task timing stats can be saved to a CSV file
with `--csv-file PATH` (this will append to existing files as well).

The full list of options can be found using `$ python test.py --help`.
