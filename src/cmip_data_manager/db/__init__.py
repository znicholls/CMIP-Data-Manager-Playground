"""
Local database layer for cached ESGF search results

Schemas are defined with SQLModel (pydantic + SQLAlchemy).  The engine is created
from a URL or path so that, although we ship SQLite, a different backend (e.g. a
local PostgreSQL server) can be swapped in later without touching the models.
"""

from __future__ import annotations

from cmip_data_manager.db.engine import create_db_engine, init_db
from cmip_data_manager.db.repository import Repository, RunResult
from cmip_data_manager.db.schema import (
    Dataset,
    DatasetChange,
    File,
    QueryRun,
    RunMembership,
)

__all__ = [
    "Dataset",
    "DatasetChange",
    "File",
    "QueryRun",
    "Repository",
    "RunMembership",
    "RunResult",
    "create_db_engine",
    "init_db",
]
