# ProxyStore Endpoint QPS Test

This benchmark measures queries per second of a ProxyStore endpoint.

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
3. Create a ProxyStore endpoint.
   ```
   $ proxystore-endpoint configure psbench
   $ proxystore-endpoint start psbench
   ```
   The returned endpoint UUID will be needed in the next step.
   The endpoint runs in the terminal process so it is recommended to either
   background the process or run in a separate terminal.

## Benchmark

The benchmark can be configured using CLI parameters.

```
$ python -m psbench.run.endpoint_qps \
    b8aba48a-386d-4977-b5c9-9bcbbaebd0bf \
    --route SET \
    --queries 1000 \
    --workers 4 \
    --payload-size 1000
```

The full list of options can be found using `--help`.
