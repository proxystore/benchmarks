# Colmena Roundtrip Task Time Test

This benchmark measures roundtrip time with Colmena.

## Setup

1. Create a new virtual environment.
   ```bash
   $ virtualenv venv
   $ . venv/bin/activate
   ```
2. Install the `psbench` package (must be done from root of repository).
   ```bash
   $ pip install .
   ```
3. Colmena can be used with Parsl or Globus Compute.
   With Globus Compute, an endpoint needs to be created.
   ```bash
   $ globus-compute-endpoint configure psbench
   $ globus-compute-endpoint start psbench
   ```
   The returned endpoint UUID will be needed in the next step.

## Benchmark

The benchmark can be configured using CLI parameters.
The full list of options can be found using `--help`.

**Globus Compute**
```bash
$ python -m psbench.benchmarks.colmena_rtt \
    --globus-compute --endpoint b8aba48a-386d-4977-b5c9-9bcbbaebd0bf \
    --input-sizes 100 1000 10000 \
    --output-sizes 100 1000 10000 \
    --task-repeat 5
```

By default, Colmena's `PipeQueues` are used which only work when the thinker and workers are on the same host.
For distributed compute, the Colmena `RedisQueues` need to be enabled by 1) starting a Redis Server (e.g., `redis-server --save "" --appendonly no`) and passing `--redis-host $HOSTNAME --redis-port 6379` to the benchmark.

**Parsl**
```bash
$ python -m psbench.benchmarks.colmena_rtt \
    --parsl \
    --input-sizes 100 1000 10000 \
    --output-sizes 100 1000 10000 \
    --task-repeat 5
```
The Parsl config can be modified in `psbench/benchmarks/colmena_rtt/config.py`.
