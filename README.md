# ProxyStore Benchmark Suite

[![pre-commit.ci status](https://results.pre-commit.ci/badge/github/gpauloski/proxystore-benchmarks/main.svg)](https://results.pre-commit.ci/latest/github/gpauloski/proxystore-benchmarks/main)
[![Tests](https://github.com/gpauloski/proxystore-benchmarks/actions/workflows/tests.yml/badge.svg)](https://github.com/gpauloski/proxystore-benchmarks/actions)

[ProxyStore](https://github.com/gpauloski/proxystore) benchmark repository.
Check out the [benchmark instructions](docs/) to get started.

# Installation

```
$ virtualenv venv
$ . venv/bin/activate
$ pip install -e .
```
The `psbench` package can also be installed into a Conda environment if that
is your jam.

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
$ virtualenv venv
$ . venv/bin/activate
$ pip install -e .
$ pip install -r requirements-dev.txt
$ pre-commit install
```

The test suite can be run with `tox` or for a specific Python version with
`tox -e py39`. Linting/type-checking/etc. can be run using pre-commit:
`pre-commit run --all-files`.
