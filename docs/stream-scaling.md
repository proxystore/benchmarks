# Stream Scaling

This benchmark simulates stream processing workflow composed of one
data generator, one data dispatcher, and a set of data compute workers.
The data generator publishes data to the stream which is then consumed
by the dispatcher. The dispatcher launches compute tasks (simulated with
sleeps) for each data item across the workers.

There a two streaming methods: "default" uses a Redis pub/sub or Kafka
broker to stream data directly from the generator to the dispatcher and
"proxy" which streams data via proxies. In the "proxy" case, only metadata
is sent via the Redis pub/sub or Kafka broker and bulk data is transmitted
via the specified ProxyStore connector. This enables the dispatcher to
process the stream without resolving data; data communication occurs directly
between the generator and the worker computing on the data.

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
3. Start a Redis server (or Kafka if preferred).
   ```
   $ redis-server --save "" --appendonly no
   ```

## Benchmark

The benchmark can be configured using CLI parameters. The full list of options
can be found using `--help`.

```bash
python -m psbench.run.stream_scaling \
    --executor parsl --parsl-executor htex-local --parsl-max-workers 4 \
    --stream-method default proxy \
    --data-size-bytes 100000 1000000 10000000 \
    --task-count 16 \
    --task-sleep 1.0 \
    --ps-connector redis --ps-host localhost --ps-port 6379 \
    --stream redis --stream-servers localhost:6379
```

This configurations uses a local Redis server for both message streaming and
bulk data storage with ProxyStore, and tasks are executed using a Parsl
HighThroughputExecutor with 4 workers.

Six experiments will be performed: three for each of the specified data sizes
and then twice for each stream method ("default" or "proxy"). In the
experiment, the data generator will generate random bytes with size
`data_size_bytes` with an interval of `task-sleep / (workers - 1)`.
Note there are `workers - 1` compute workers because one worker is designated
the generator. The dispatcher will consume data and dispatch compute tasks
as quickly as possible, with each compute task resolving the input data and
then sleeping for `task-sleep` seconds.

**Note:** Redis pub/sub places a limit on the maximum data rate of clients
so large data sizes or fast data generation rates may crash Redis or raise
"Serialized object exceeds buffer threshold of 1048576 bytes, this could cause
overflows" warning. You may need to adjust the Redis server configuration
according to your benchmark parameters.

### Executors

The task executor can be changed with CLI options. Some examples include:

* Globus Compute:
  ```bash
  --executor globus --globus-compute-endpoint <UUID>
  ```
* Parsl `ThreadPoolExecutor`:
  ```bash
  --executor parsl --parsl-executor thread --parsl-max-workers 4
  ```
* Parsl Local `HighThroughputExecutor`:
  ```bash
  --executor parsl --parsl-executor htex-local --parsl-max-workers 4
  ```

### ProxyStore

The ProxyStore connector can be configured using the `--ps-connector`
option. Some choices for `--ps-connector` will mark additional CLI options
as required.
