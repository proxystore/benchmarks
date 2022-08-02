# Benchmark Instructions

This directory contains an instruction markdown file for each benchmark in
the `psbench` package.

Benchmarks are written as executable submodules of `psbench.benchmarks`.
E.g., to see what options a benchmark supports, execute:
```
python -m psbench.benchmarks.{benchmark_name} --help
```

### Why use submodules?

We prefer not writing benchmark scripts as standalone Python scripts
(e.g., `python benchmarks/myscript.py`) because Python will prepend the
the directory of the script to the path rather than prepending the current
working directory.

This can cause unexpected errors when trying to import files in the current
working directory and the common workaround (modifying `PYTHONPATH` or using
relative imports) are fragile and prone to causing more problems.

Using an executable submodule will always prepend the current working directory
to the path and ensure that all code is packages within `psbench` and
therefore accessible via absolute imports. As a bonus, the benchmark can be
executed anywhere as long as `psbench` is installed in the environment.
