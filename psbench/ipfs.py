"""Inter Planetary File System utilities."""

from __future__ import annotations

import os
import pathlib
import subprocess
import tempfile


def add_data(data: bytes, filepath: str | pathlib.Path) -> str:
    """Writes data to file and adds to IPFS.

    Args:
        data (bytes): data to add to IPFS.
        filepath (str): path to write data to.

    Returns:
        The string content ID of the IPFS file.
    """
    filepath = pathlib.Path(filepath)
    filepath.parent.mkdir(parents=True, exist_ok=True)

    with open(filepath, 'wb') as f:
        f.write(data)

    output = subprocess.check_output(['ipfs', 'add', filepath]).decode('utf-8')
    # Output looks like:
    #   $ ipfs add meow.txt
    #   added QmabZ1pL9npKXJg8JGdMwQMJo2NCVy9yDVYjhiHK4LTJQH meow.txt
    cid = output.split(' ')[1]

    return cid


def get_data(cid: str) -> bytes:
    """Read binary data from an IPFS file.

    Args:
        cid (str): content ID of the IPFS file.

    Returns:
        bytes read from the file.
    """
    with tempfile.TemporaryDirectory() as tempdir:
        filepath = os.path.join(tempdir, 'data')
        subprocess.run(['ipfs', 'get', cid, '-o', filepath], check=True)
        with open(filepath, 'rb') as f:
            return f.read()
