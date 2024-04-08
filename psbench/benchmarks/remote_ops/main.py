"""Remote Operation Performance Test.

Provides comparisons between remote operations with endpoints
and Redis servers.
"""

from __future__ import annotations

import asyncio
import logging
import socket
import statistics
import sys
import uuid
from typing import Any

if sys.version_info >= (3, 11):  # pragma: >=3.11 cover
    pass
else:  # pragma: <3.11 cover
    pass

import redis
from proxystore.endpoint.endpoint import Endpoint
from proxystore.p2p.manager import PeerManager
from proxystore.p2p.relay.client import RelayClient

import psbench.benchmarks.remote_ops.endpoint_ops as endpoint_ops
import psbench.benchmarks.remote_ops.redis_ops as redis_ops
from psbench.benchmarks.protocol import ContextManagerAddIn
from psbench.benchmarks.remote_ops.config import OP_TYPE
from psbench.benchmarks.remote_ops.config import RunConfig
from psbench.benchmarks.remote_ops.config import RunResult
from psbench.logging import BENCH_LOG_LEVEL
from psbench.logging import TEST_LOG_LEVEL

logger = logging.getLogger('remote-ops')


async def run_endpoint(
    endpoint: Endpoint,
    remote_endpoint: uuid.UUID | None,
    op: OP_TYPE,
    payload_size: int = 0,
    repeat: int = 3,
) -> RunResult:
    """Run test for single operation and measure performance.

    Args:
        endpoint (Endpoint): local endpoint.
        remote_endpoint (UUID): UUID of remote endpoint to peer with.
        op (str): endpoint operation to test.
        payload_size (int): bytes to send/receive for GET/SET operations.
        repeat (int): number of times to repeat operation. If repeat is greater
            than or equal to three, the slowest and fastest times will be
            dropped to account for the first op being slower while establishing
            a connection.

    Returns:
        RunResult with summary of test run.
    """
    logger.log(TEST_LOG_LEVEL, f'starting endpoint peering test for {op}')

    if op == 'evict':
        times_ms = await endpoint_ops.test_evict(
            endpoint,
            remote_endpoint,
            repeat,
        )
    elif op == 'exists':
        times_ms = await endpoint_ops.test_exists(
            endpoint,
            remote_endpoint,
            repeat,
        )
    elif op == 'get':
        times_ms = await endpoint_ops.test_get(
            endpoint,
            remote_endpoint,
            payload_size,
            repeat,
        )
    elif op == 'set':
        times_ms = await endpoint_ops.test_set(
            endpoint,
            remote_endpoint,
            payload_size,
            repeat,
        )
    else:
        raise AssertionError(f'Unsupported operation {op}')

    if len(times_ms) >= 3:
        times_ms = times_ms[1:-1]

    avg_time_s = sum(times_ms) / 1000 / len(times_ms)
    payload_mb = payload_size / 1e6
    avg_bandwidth_mbps = (
        payload_mb / avg_time_s if op in ('get', 'set') else None
    )

    return RunResult(
        backend='endpoint',
        op=op,
        payload_size_bytes=payload_size if op in ('get', 'set') else None,
        repeat=repeat,
        total_time_ms=sum(times_ms),
        avg_time_ms=sum(times_ms) / len(times_ms),
        min_time_ms=min(times_ms),
        max_time_ms=max(times_ms),
        stdev_time_ms=(
            statistics.stdev(times_ms) if len(times_ms) > 1 else 0.0
        ),
        avg_bandwidth_mbps=avg_bandwidth_mbps,
    )


def run_redis(
    client: redis.StrictRedis[Any],
    op: OP_TYPE,
    payload_size: int = 0,
    repeat: int = 3,
) -> RunResult:
    """Run test for single operation and measure performance.

    Args:
        client (StrictRedis): Redis client connected to remote server.
        op (str): endpoint operation to test.
        payload_size (int): bytes to send/receive for GET/SET operations.
        repeat (int): number of times to repeat operation. If repeat is greater
            than or equal to three, the slowest and fastest times will be
            dropped to account for the first op being slower while establishing
            a connection.

    Returns:
        RunResult with summary of test run.
    """
    logger.log(TEST_LOG_LEVEL, f'starting remote redis test for {op}')

    if op == 'evict':
        times_ms = redis_ops.test_evict(client, repeat)
    elif op == 'exists':
        times_ms = redis_ops.test_exists(client, repeat)
    elif op == 'get':
        times_ms = redis_ops.test_get(client, payload_size, repeat)
    elif op == 'set':
        times_ms = redis_ops.test_set(client, payload_size, repeat)
    else:
        raise AssertionError(f'Unsupported operation {op}')

    if len(times_ms) >= 3:
        times_ms = times_ms[1:-1]

    avg_time_s = sum(times_ms) / 1000 / len(times_ms)
    payload_mb = payload_size / 1e6
    avg_bandwidth_mbps = (
        payload_mb / avg_time_s if op in ('get', 'set') else None
    )

    return RunResult(
        backend='redis',
        op=op,
        payload_size_bytes=payload_size if op in ('get', 'set') else None,
        repeat=repeat,
        total_time_ms=sum(times_ms),
        avg_time_ms=sum(times_ms) / len(times_ms),
        min_time_ms=min(times_ms),
        max_time_ms=max(times_ms),
        stdev_time_ms=(
            statistics.stdev(times_ms) if len(times_ms) > 1 else 0.0
        ),
        avg_bandwidth_mbps=avg_bandwidth_mbps,
    )


async def runner_endpoint(
    remote_endpoint: uuid.UUID | None,
    ops: list[OP_TYPE],
    *,
    payload_sizes: list[int],
    repeat: int,
    relay_server: str | None = None,
) -> list[RunResult]:
    """Run matrix of test test configurations with an Endpoint.

    Args:
        remote_endpoint (UUID): remote endpoint UUID to peer with.
        ops (str): endpoint operations to test.
        payload_sizes (int): bytes to send/receive for GET/SET operations.
        repeat (int): number of times to repeat operations.
        relay_server (str): relay server address
    """
    results: list[RunResult] = []
    manager = (
        PeerManager(RelayClient(relay_server))
        if relay_server is not None
        else None
    )
    async with Endpoint(
        name=socket.gethostname(),
        uuid=uuid.uuid4(),
        peer_manager=manager,
    ) as endpoint:
        for op in ops:
            for i, payload_size in enumerate(payload_sizes):
                # Only need to repeat for payload_size for GET/SET
                if i == 0 or op in ['get', 'set']:
                    result = await run_endpoint(
                        endpoint,
                        remote_endpoint=remote_endpoint,
                        op=op,
                        payload_size=payload_size,
                        repeat=repeat,
                    )
                    logger.log(TEST_LOG_LEVEL, results)
                    results.append(result)
    return results


def runner_redis(
    host: str,
    port: int,
    ops: list[OP_TYPE],
    *,
    payload_sizes: list[int],
    repeat: int,
) -> list[RunResult]:
    """Run matrix of test test configurations with a Redis server.

    Args:
        host (str): remote Redis server hostname/IP.
        port (int): remote Redis server port.
        ops (str): endpoint operations to test.
        payload_sizes (int): bytes to send/receive for GET/SET operations.
        repeat (int): number of times to repeat operations.
    """
    client = redis.StrictRedis(host=host, port=port)
    results: list[RunResult] = []
    for op in ops:
        for i, payload_size in enumerate(payload_sizes):
            # Only need to repeat for payload_size for GET/SET
            if i == 0 or op in ['get', 'set']:
                result = run_redis(
                    client,
                    op=op,
                    payload_size=payload_size,
                    repeat=repeat,
                )
                logger.log(TEST_LOG_LEVEL, result)
                results.append(result)
    return results


class Benchmark(ContextManagerAddIn):
    name = 'Remote Ops'
    config_type = RunConfig
    result_type = RunResult

    def __init__(
        self,
        endpoint: str | None = None,
        relay_server: str | None = None,
        redis_host: str | None = None,
        redis_port: int | None = None,
        use_uvloop: bool = False,
    ) -> None:
        if use_uvloop:
            try:
                import uvloop

                uvloop.install()
                logger.log(
                    BENCH_LOG_LEVEL,
                    'uvloop available... using as event loop',
                )
            except ImportError:  # pragma: no cover
                logger.log(
                    BENCH_LOG_LEVEL,
                    'uvloop unavailable... using asyncio event loop',
                )
        else:
            logger.log(
                BENCH_LOG_LEVEL,
                'uvloop override... using asyncio event loop',
            )

        self.endpoint = endpoint
        self.relay_server = relay_server
        self.redis_host = redis_host
        self.redis_port = redis_port
        self.use_uvloop = use_uvloop
        super().__init__()

    def config(self) -> dict[str, Any]:
        return {
            'endpoint': self.endpoint,
            'relay_server': self.relay_server,
            'redis_host': self.redis_host,
            'redis_port': self.redis_port,
            'use_uvloop': self.use_uvloop,
        }

    def run(self, config: RunConfig) -> list[RunResult]:
        if config.backend == 'endpoint':
            endpoint = (
                self.endpoint
                if self.endpoint is None
                else uuid.UUID(self.endpoint)
            )
            results = asyncio.run(
                runner_endpoint(
                    endpoint,
                    config.ops,
                    payload_sizes=config.payload_sizes,
                    repeat=config.repeat,
                    relay_server=self.relay_server,
                ),
            )
        elif config.backend == 'redis':
            assert self.redis_host is not None
            assert self.redis_port is not None
            results = runner_redis(
                self.redis_host,
                self.redis_port,
                config.ops,
                payload_sizes=config.payload_sizes,
                repeat=config.repeat,
            )
        else:
            raise AssertionError('Unreachable.')

        return results
