"""
Database engine creation

The user chooses where results are stored.  A bare path or filename is treated as
a SQLite database; anything containing `://` is treated as a full SQLAlchemy URL,
so a PostgreSQL server (`postgresql://.../cmip`) can be dropped in later without
changing the models or the repository.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from sqlalchemy import Engine, event
from sqlmodel import SQLModel, create_engine

# Importing the schema module registers every table on ``SQLModel.metadata`` so
# that ``init_db`` creates them all.
from cmip_data_manager.db import schema  # noqa: F401


def _to_url(target: str | Path) -> str:
    """
    Turn a path or URL into a SQLAlchemy connection URL

    Parameters
    ----------
    target
        Either a SQLAlchemy URL (containing `://`) or a filesystem path to a
        SQLite database file.

    Returns
    -------
    :
        A SQLAlchemy URL string.
    """
    text = str(target)
    if "://" in text:
        return text
    return f"sqlite:///{Path(text).expanduser()}"


def create_db_engine(target: str | Path, *, echo: bool = False) -> Engine:
    """
    Create a database engine for the given target

    Parameters
    ----------
    target
        SQLite path/filename or a full SQLAlchemy URL.

    echo
        If `True`, log all SQL (useful when debugging).

    Returns
    -------
    :
        A configured SQLAlchemy engine.  For SQLite, foreign-key enforcement is
        enabled so the `Dataset`/`File` relationship behaves correctly.
    """
    url = _to_url(target)
    engine = create_engine(url, echo=echo)
    if engine.dialect.name == "sqlite":
        _enable_sqlite_foreign_keys(engine)
    return engine


def _enable_sqlite_foreign_keys(engine: Engine) -> None:
    """Turn on `PRAGMA foreign_keys` for every SQLite connection."""

    @event.listens_for(engine, "connect")
    def _set_pragma(dbapi_connection: Any, _connection_record: object) -> None:
        cursor = dbapi_connection.cursor()
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.close()


def init_db(engine: Engine) -> None:
    """
    Create any missing tables

    Parameters
    ----------
    engine
        Engine whose database should be initialised.
    """
    SQLModel.metadata.create_all(engine)
