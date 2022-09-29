"""Remote Operation Performance Test.

Provides comparisons between remote operations with endpoints
and Redis servers.
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import socket
import sys
import uuid
from typing import NamedTuple
from typing import Sequence

if sys.version_info >= (3, 8):  # pragma: >3.7 cover
    from typing import Literal
else:  # pragma: <3.8 cover
    from typing_extensions import Literal

from proxystore.endpoint.endpoint import Endpoint

from psbench.argparse import add_logging_options
from psbench.benchmarks.remote_ops.ops import test_evict
from psbench.benchmarks.remote_ops.ops import test_exists
from psbench.benchmarks.remote_ops.ops import test_get
from psbench.benchmarks.remote_ops.ops import test_set
from psbench.csv import CSVLogger
from psbench.logging import init_logging
from psbench.logging import TESTING_LOG_LEVEL

OP_TYPE = Literal['EVICT', 'EXISTS', 'GET', 'SET']

logger = logging.getLogger('endpoint-peering')


class RunStats(NamedTuple):
    """Stats for a given run configuration."""

    op: OP_TYPE
    payload_size_bytes: int | None
    repeat: int
    local_endpoint_uuid: str
    remote_endpoint_uuid: str
    total_time_ms: float
    avg_time_ms: float
    min_time_ms: float
    max_time_ms: float
    avg_bandwidth_mbps: float | None


async def run(
    endpoint: Endpoint,
    remote_endpoint: uuid.UUID | None,
    op: OP_TYPE,
    payload_size: int = 0,
    repeat: int = 3,
) -> RunStats:
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
        RunStats with summary of test run.
    """
    logger.log(TESTING_LOG_LEVEL, f'starting endpoint peering test for {op}')

    if op == 'EVICT':
        times_ms = await test_evict(endpoint, remote_endpoint, repeat)
    elif op == 'EXISTS':
        times_ms = await test_exists(endpoint, remote_endpoint, repeat)
    elif op == 'GET':
        times_ms = await test_get(
            endpoint,
            remote_endpoint,
            payload_size,
            repeat,
        )
    elif op == 'SET':
        times_ms = await test_set(
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
        payload_mb / avg_time_s if op in ('GET', 'SET') else None
    )

    return RunStats(
        op=op,
        payload_size_bytes=payload_size if op in ('GET', 'SET') else None,
        repeat=repeat,
        local_endpoint_uuid=str(endpoint.uuid),
        remote_endpoint_uuid=str(remote_endpoint),
        total_time_ms=sum(times_ms),
        avg_time_ms=sum(times_ms) / len(times_ms),
        min_time_ms=min(times_ms),
        max_time_ms=max(times_ms),
        avg_bandwidth_mbps=avg_bandwidth_mbps,
    )


async def runner(
    remote_endpoint: uuid.UUID | None,
    ops: list[OP_TYPE],
    *,
    payload_sizes: list[int],
    repeat: int,
    server: str | None = None,
    csv_file: str | None = None,
) -> None:
    """Run matrix of test test configurations.

    Args:
        remote_endpoint (UUID): remote endpoint UUID to peer with.
        ops (str): endpoint operations to test.
        payload_sizes (int): bytes to send/receive for GET/SET operations.
        repeat (int): number of times to repeat operations.
        server (str): signaling server address
        csv_file (str): optional csv filepath to log results to.
    """
    if csv_file is not None:
        csv_logger = CSVLogger(csv_file, RunStats)

    async with Endpoint(
        name=socket.gethostname(),
        uuid=uuid.uuid4(),
        signaling_server=server,
    ) as endpoint:
        for op in ops:
            for payload_size in payload_sizes:
                run_stats = await run(
                    endpoint,
                    remote_endpoint=remote_endpoint,
                    op=op,
                    payload_size=payload_size,
                    repeat=repeat,
                )

                logger.log(TESTING_LOG_LEVEL, run_stats)
                if csv_file is not None:
                    csv_logger.log(run_stats)

    if csv_file is not None:
        csv_logger.close()
        logger.log(TESTING_LOG_LEVEL, f'results logged to {csv_file}')


def main(argv: Sequence[str] | None = None) -> int:
    """Remote ops test entrypoint."""
    argv = argv if argv is not None else sys.argv[1:]

    parser = argparse.ArgumentParser(
        description='Remote ops performance test.',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        '--remote',
        required=True,
        help='Remote Endpoint UUID',
    )
    parser.add_argument(
        '--ops',
        choices=['GET', 'SET', 'EXISTS', 'EVICT'],
        nargs='+',
        required=True,
        help='Endpoint operations to measure',
    )
    parser.add_argument(
        '--payload-sizes',
        type=int,
        nargs='+',
        default=0,
        help='Payload sizes for GET/SET operations',
    )
    parser.add_argument(
        '--server',
        required=True,
        help='Signaling server address for connecting to the remote endpoint',
    )
    parser.add_argument(
        '--repeat',
        type=int,
        default=10,
        help='Number of times to repeat operations',
    )
    parser.add_argument(
        '--no-uvloop',
        action='store_true',
        help='Override using uvloop if available',
    )
    add_logging_options(parser)
    args = parser.parse_args(argv)

    init_logging(args.log_file, args.log_level, force=True)

    if not args.no_uvloop:
        try:
            import uvloop

            uvloop.install()
            logger.info('uvloop available... using as event loop')
        except ImportError:  # pragma: no cover
            logger.info('uvloop unavailable... using asyncio event loop')
    else:
        logger.info('uvloop override... using asyncio event loop')

    asyncio.run(
        runner(
            uuid.UUID(args.remote),
            args.ops,
            payload_sizes=args.payload_sizes,
            repeat=args.repeat,
            server=args.server,
            csv_file=args.csv_file,
        ),
    )

    return 0
