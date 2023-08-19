from __future__ import annotations

from typing import NamedTuple


class ProxyStats(NamedTuple):
    """Proxy stats from within task."""

    input_get_ms: float | None = None
    input_resolve_ms: float | None = None
    output_put_ms: float | None = None
    output_proxy_ms: float | None = None


def pong(data: bytes, *, result_size: int = 0, sleep: float = 0) -> bytes:
    """Task that takes data and returns more data.

    Args:
        data (bytes): input data.
        result_size (int): size of results byte array (default: 0).
        sleep (float): seconds to sleep for to simulate work (default: 0).

    Returns:
        bytes
    """
    import time

    from psbench.utils import randbytes

    assert isinstance(data, bytes)
    time.sleep(sleep)

    return randbytes(result_size)


def pong_ipfs(
    cid: str,
    ipfs_dir: str,
    *,
    result_size: int = 0,
    sleep: float = 0,
) -> str | None:
    """Task that takes data as IPFS CIDs and returns data via IPFS.

    Args:
        cid (str): content ID of input data.
        ipfs_dir (str): directory to write output data to.
        result_size (int): size of results byte array (default: 0).
        sleep (float): seconds to sleep for to simulate work (default: 0).

    Returns:
        String content ID of return data or None.
    """
    import os
    import time
    import uuid

    from psbench import ipfs
    from psbench.utils import randbytes

    data = ipfs.get_data(cid)
    assert isinstance(data, bytes)
    time.sleep(sleep)

    if result_size > 0:
        os.makedirs(ipfs_dir, exist_ok=True)
        filepath = os.path.join(ipfs_dir, str(uuid.uuid4()))
        return_data = randbytes(result_size)
        return ipfs.add_data(return_data, filepath)
    else:
        return None


def pong_dspaces(
    path: str,
    data_size: int,
    rank: int,
    size: int,
    *,
    version: int = 1,
    result_size: int = 0,
    sleep: float = 0,
) -> str | None:
    """Task that takes a DataSpace path and returns data via DataSpaces.

    Args:
        client (ds.DSpaces):DataSpaces client
        path (str): filename of the DataSpaces stored data.
                data_size (int) : the size of the DataSpaces object.
                rank (int) : MPI rank.
                size (int): MPI communication size.
                version (int): The version of the data to access (default: 1).
        result_size (int): size of results byte array (default: 0).
        sleep (float): seconds to sleep for to simulate work (default: 0).

    Returns:
        Filename of return data or None.
    """
    import os
    import time
    import uuid
    import numpy as np

    import dspaces as ds

    from psbench.utils import randbytes

    client = ds.dspaces.DSClient()
    data = client.Get(
        path,
        version=version,
        lb=((data_size * rank),),
        ub=((data_size * rank + data_size - 1),),
        dtype=bytes,
        timeout=-1,
    ).tobytes()

    assert isinstance(data, bytes)
    time.sleep(sleep)

    if result_size > 0:
        filepath = str(uuid.uuid4())
        return_data = bytearray(randbytes(result_size))
        client.Put(
            np.array(return_data), filepath, version=version, offset=((result_size * rank),)
        )
        return (filepath, result_size)
    else:
        return None


def pong_proxy(
    data: bytes,
    *,
    evict_result: bool = True,
    result_size: int = 0,
    sleep: float = 0,
) -> tuple[bytes, ProxyStats | None]:
    """Task that takes a proxy of data and return a proxy of data.

    Args:
        data (bytes): input data.
        evict_result (bool): Set evict flag in returned proxy (default: True).
        result_size (int): size of results byte array.
        sleep (float): seconds to sleep for to simulate work (default: 0). If
            the sleep is non-zero, the input proxy will be asynchronously
            resolved during the sleep.

    Returns:
        Tuple of bytes and ProxyStats is stat tracking on the store is enabled.

    Raises:
        RuntimeError:
            if the ProxyStore backend cannot be extracted from the input
            Proxy.
    """
    import time

    from proxystore.proxy import is_resolved
    from proxystore.proxy import Proxy
    from proxystore.store import get_store
    from proxystore.store.utils import resolve_async

    from psbench.tasks.pong import ProxyStats
    from psbench.utils import randbytes

    assert isinstance(data, Proxy)
    assert not is_resolved(data)

    if sleep > 0.0:
        data.resolve_async()
        time.sleep(sleep)

    assert isinstance(data, bytes)
    assert isinstance(data, Proxy)

    result_data = randbytes(result_size)
    store = get_store(data)
    if store is None:  # pragma: no cover
        # init_store does not return None in ProxyStore <= 0.3.3
        raise RuntimeError('Cannot find ProxyStore backend to use.')
    result: Proxy[bytes] = store.proxy(result_data, evict=evict_result)

    stats: ProxyStats | None = None
    if store.metrics is not None:
        input_metrics = store.metrics.get_metrics(data)
        output_metrics = store.metrics.get_metrics(result)
        stats = ProxyStats(
            input_get_ms=input_metrics.times['store.get'].avg_time_ms,
            input_resolve_ms=input_metrics.times[
                'factory.resolve'
            ].avg_time_ms,
            output_put_ms=output_metrics.times['store.put'].avg_time_ms,
            output_proxy_ms=output_metrics.times['store.proxy'].avg_time_ms,
        )

    return (result, stats)
