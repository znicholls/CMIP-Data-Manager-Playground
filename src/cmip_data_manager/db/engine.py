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

from sqlalchemy import Engine, event, inspect, text
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
        _configure_sqlite(engine)
    return engine


def _configure_sqlite(engine: Engine) -> None:
    """Set per-connection SQLite pragmas.

    Enables foreign-key enforcement (so the `Dataset`/`File` relationships behave)
    and write-ahead logging, so Step 2's save-as-you-go commits do not block a
    reader inspecting the database while a run is in progress.
    """

    @event.listens_for(engine, "connect")
    def _set_pragma(dbapi_connection: Any, _connection_record: object) -> None:
        cursor = dbapi_connection.cursor()
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.execute("PRAGMA journal_mode=WAL")
        cursor.close()


def init_db(engine: Engine) -> None:
    """
    Create any missing tables, then add any missing (nullable) columns

    `create_all` creates absent tables but never alters an existing one, so a model
    that gained a column since the database was first created would otherwise have
    nowhere to write it.  `_add_missing_columns` closes that gap with a lightweight,
    additive-only migration so an older cache keeps working after a schema addition.

    Parameters
    ----------
    engine
        Engine whose database should be initialised.
    """
    SQLModel.metadata.create_all(engine)
    _add_missing_columns(engine)


def _add_missing_columns(engine: Engine) -> None:
    """
    Add columns present on the models but missing from an existing table

    A minimal forward-only migration: for every already-existing table it issues an
    `ALTER TABLE ... ADD COLUMN` for each *nullable* model column the table lacks
    (only additions — it never drops, renames or retypes a column, and skips
    not-null columns, which SQLite cannot add to a populated table).
    """
    inspector = inspect(engine)
    existing_tables = set(inspector.get_table_names())
    with engine.begin() as connection:
        for table in SQLModel.metadata.sorted_tables:
            if table.name not in existing_tables:
                continue
            present = {column["name"] for column in inspector.get_columns(table.name)}
            for column in table.columns:
                if column.name in present or not column.nullable:
                    continue
                type_sql = column.type.compile(engine.dialect)
                connection.execute(
                    text(
                        f"ALTER TABLE {table.name} ADD COLUMN {column.name} {type_sql}"
                    )
                )
