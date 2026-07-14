"""
Re-useable fixtures etc. for tests

See https://docs.pytest.org/en/7.1.x/reference/fixtures.html#conftest-py-sharing-fixtures-across-multiple-files
"""

import sys
from pathlib import Path

import pandas as pd
import pytest

# The header-timeout tests fan reads out to ``spawn``ed child processes, which
# unpickle module-level reader fakes defined in ``tests.unit.test_headers``.
# Under pytest's ``importlib`` import mode the repo root is not on ``sys.path``,
# so a fresh child cannot import the ``tests`` package.  ``spawn`` propagates the
# parent's ``sys.path`` to children, so putting the root here makes those readers
# importable in the child.
_ROOTDIR = str(Path(__file__).resolve().parent.parent)
if _ROOTDIR not in sys.path:
    sys.path.insert(0, _ROOTDIR)


@pytest.fixture(scope="session", autouse=True)
def pandas_terminal_width():
    # Set pandas terminal width so that doctests don't depend on terminal width.

    # We set the display width to 120 because examples should be short,
    # anything more than this is too wide to read in the source.
    pd.set_option("display.width", 120)

    # Display as many columns as you want (i.e. let the display width do the
    # truncation)
    pd.set_option("display.max_columns", 1000)
