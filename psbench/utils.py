from __future__ import annotations

import os
import random
import time


def randbytes(size: int) -> bytes:
    """Get random byte string of specified size.

    This method previously existed for Python 3.8 and older compatibility
    but now remains because it's used in many places throughout the codebase.

    Args:
        size (int): size of byte string to return.

    Returns:
        random byte string.
    """
    return random.randbytes(size)


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
