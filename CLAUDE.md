# Repository conventions for AI agents

## Docstrings: markdown only

Write all docstrings in **markdown**. Do **not** use reStructuredText (RST) or
mkdocstrings cross-reference syntax — the docs are built with mkdocs +
mkdocstrings-python and RST/autoref constructs break the build.

- Keep numpy-style section headers (`Parameters`, `Returns`, `Examples` with
  `----------` underlines). These are required by ruff's `pydocstyle` numpy
  convention and match the existing code (see `src/cmip_data_manager/operations.py`).
- Use markdown for prose and inline formatting: `**bold**`, fenced code blocks,
  and markdown links like `[text](https://example.com)`.
- **Never** use:
  - mkdocstrings autorefs such as `[Name][full.dotted.path]`
  - RST roles such as `:func:`, `:class:`, `:mod:`
  - RST directives such as `.. note::`

## Tooling

- Run everything through `uv` (e.g. `uv run --group tests pytest`, `make test`,
  `make checks`, `make ruff-fixes`).
- Code must pass `mypy --strict`, `ruff`, and keep test coverage >= 90%.
- Tests run with `--doctest-modules`, so docstring `>>>` examples must not perform
  network or filesystem I/O.
