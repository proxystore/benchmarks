from __future__ import annotations

import argparse
import re
import sys
from typing import Any
from typing import Optional
from typing import Sequence

if sys.version_info >= (3, 11):  # pragma: >=3.11 cover
    from typing import Self
else:  # pragma: <3.11 cover
    from typing_extensions import Self

from pydantic import BaseModel


class IPFSConfig(BaseModel):
    use_ipfs: bool
    local_dir: Optional[str]  # noqa: UP007
    remote_dir: Optional[str]  # noqa: UP007

    @staticmethod
    def add_parser_group(
        parser: argparse.ArgumentParser,
        required: bool = True,
        argv: Sequence[str] | None = None,
    ) -> None:
        group = parser.add_argument_group(title='IPFS Configuration')

        args_str = ' '.join(argv) if argv is not None else ''
        group.add_argument(
            '--ipfs',
            action='store_true',
            default=False,
            help='Use IPFS for data transfer.',
        )
        group.add_argument(
            '--ipfs-local-dir',
            required=bool(re.search(r'--ipfs($|\s)', args_str)),
            help='Local directory to write IPFS files to.',
        )
        group.add_argument(
            '--ipfs-remote-dir',
            required=bool(re.search(r'--ipfs($|\s)', args_str)),
            help='Local directory to write IPFS files to.',
        )

    @classmethod
    def from_args(cls, **kwargs: Any) -> Self:
        return cls(
            use_ipfs=kwargs.get('ipfs', False),
            local_dir=kwargs.get('ipfs_local_dir', None),
            remote_dir=kwargs.get('ipfs_remote_dir', None),
        )
