This is a small local python package wrapping some [sigma-rust](https://github.com/ergoplatform/sigma-rust) functionality using [PyO3](https://github.com/PyO3/PyO3).

It is used by the watcher's testbench to generate mock-up data.

## Installation
Start by  installing the [maturin](https://github.com/PyO3/maturin) build system:

```
pip install maturin
```

Then build and install the package. From within this directory, run:

```
maturin build
pip install ./target/wheels/sigpy-<system-specific-stuff>.whl
```
