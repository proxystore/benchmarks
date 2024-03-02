from __future__ import annotations

import atexit
import sys
from concurrent.futures import Future
from types import TracebackType
from typing import Any
from typing import Callable
from typing import TypeVar

if sys.version_info >= (3, 10):  # pragma: >=3.10 cover
    from typing import ParamSpec
else:  # pragma: <3.10 cover
    from typing_extensions import ParamSpec

if sys.version_info >= (3, 11):  # pragma: >=3.11 cover
    from typing import Self
else:  # pragma: <3.11 cover
    from typing_extensions import Self

import parsl
from parsl.app.python import PythonApp
from parsl.config import Config
from parsl.executors import ThreadPoolExecutor

P = ParamSpec('P')
T = TypeVar('T')


class ParslExecutor:
    """Parsl task execution engine."""

    def __init__(
        self,
        config: Config,
        app_options: dict[str, Any] | None = None,
        max_workers: int | None = None,
    ) -> None:
        if len(config.executors) > 1:
            raise ValueError('Multiple Parsl executors is not supported.')

        self._config = config
        self._app_options = {} if app_options is None else app_options
        # Mapping of function to function wrapped in Parsl App
        self._parsl_apps: dict[Callable[[Any], Any], PythonApp] = {}

        self.max_workers: int | None = None

        (executor,) = config.executors
        if isinstance(executor, ThreadPoolExecutor):
            self.max_workers = executor.max_threads

    def __enter__(self) -> Self:
        self.start()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        exc_traceback: TracebackType | None,
    ) -> None:
        self.close()

    def start(self) -> None:
        parsl.load(self._config)

    def close(self) -> None:
        dfk = parsl.dfk()
        dfk.wait_for_current_tasks()
        dfk.cleanup()
        atexit.unregister(dfk.atexit_cleanup)
        parsl.clear()

    def submit(
        self,
        function: Callable[P, T],
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> Future[T]:
        if function not in self._parsl_apps:
            app = PythonApp(function, **self._app_options)
            self._parsl_apps[function] = app

        app = self._parsl_apps[function]

        future = app(*args, **kwargs)

        return future
