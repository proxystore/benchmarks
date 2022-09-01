from __future__ import annotations

import os
import random
import sys
import time


def randbytes(size: int) -> bytes:
    """Get random byte string of specified size.

    Uses `random.randbytes()` in Python 3.9 or newer and
    `os.urandom()` in Python 3.8 and older.

    Args:
        size (int): size of byte string to return.

    Returns:
        random byte string.
    """
    if sys.version_info >= (3, 9) and size < 1e9:  # pragma: >=3.9 cover
        return random.randbytes(size)
    else:  # pragma: <3.9 cover
        return os.urandom(size)


def make_parent_dirs(filepath: str) -> None:
    """Make parent directories of a filepath."""
    parent_dir = os.path.dirname(filepath)
    if len(parent_dir) > 0 and not os.path.isdir(parent_dir):
        os.makedirs(parent_dir, exist_ok=True)


def wait_until(timestamp: float) -> None:
    """Sleep until UNIX timestamp."""
    current = time.time()
    if timestamp <= current:
        return

    time.sleep(timestamp - current)
