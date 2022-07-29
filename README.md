# ProxyStore Benchmark Suite

[![pre-commit.ci status](https://results.pre-commit.ci/badge/github/gpauloski/proxystore-benchmarks/main.svg)](https://results.pre-commit.ci/latest/github/gpauloski/proxystore-benchmarks/main)
[![Tests](https://github.com/gpauloski/proxystore-benchmarks/actions/workflows/tests.yml/badge.svg)](https://github.com/gpauloski/proxystore-benchmarks/actions)

[ProxyStore](https://github.com/gpauloski/proxystore) benchmark repository.
Check out the [benchmarks](benchmarks) to get started.

## Development Installation

[Tox](https://tox.wiki/en/3.0.0/index.html)'s `--devenv` is the recommended
way to configure a development environment.
```
$ tox --devenv venv -e py 310
$ . venv/bin/activate
$ pre-commit install
```

Alternatively, a development environment can be manually configured.
```
$ virtualenv ven
$ . venv/bin/activate
$ pip install -e .
$ pip install -r requirements-dev.txt
```
