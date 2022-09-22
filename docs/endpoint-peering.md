# ProxyStore Endpoint Peering Test

This benchmark measures latency and bandwidth from a local endpoint to a remote.

## Setup

1. On the local and remote systems, create a new virtual environment.
   ```
   $ virtualenv venv
   $ . venv/bin/activate
   ```
2. Install the `psbench` package (must be done from root of repository).
   ```
   $ pip install .
   ```
3. On the remote system, create a ProxyStore endpoint.
   ```
   $ proxystore-endpoint configure psbench --server {signaling-server-address}
   $ proxystore-endpoint start psbench
   ```
   The returned endpoint UUID will be needed in the next step.

## Benchmark

The benchmark can be configured using CLI parameters.

```
$ python -m psbench.benchmarks.endpoint_peering \
    --remote b8aba48a-386d-4977-b5c9-9bcbbaebd0bf \
    --ops GET SET \
    --payload-sizes 1 1000 1000000 \
    --repeat 5 \
    --server {signaling-server-address}
```

The full list of options can be found using `--help`.
