"""Tests for database engine creation."""

from __future__ import annotations

from sqlalchemy import inspect, text
from sqlmodel import Session, select

from cmip_data_manager.db.engine import create_db_engine, init_db
from cmip_data_manager.db.schema import Dataset


def test_sqlite_path_creates_working_db(tmp_path):
    engine = create_db_engine(tmp_path / "x.sqlite")
    assert engine.dialect.name == "sqlite"
    init_db(engine)
    with Session(engine) as session:
        session.add(Dataset(instance_id="d1"))
        session.commit()
        assert session.exec(select(Dataset)).one().instance_id == "d1"


def test_full_url_is_passed_through(tmp_path):
    url = f"sqlite:///{tmp_path / 'y.sqlite'}"
    engine = create_db_engine(url)
    assert str(engine.url) == url


def test_sqlite_foreign_keys_are_enforced(tmp_path):
    engine = create_db_engine(tmp_path / "fk.sqlite")
    with engine.connect() as connection:
        result = connection.exec_driver_sql("PRAGMA foreign_keys").scalar()
    assert result == 1


def test_init_db_adds_a_missing_column_to_an_existing_table(tmp_path):
    engine = create_db_engine(tmp_path / "old.sqlite")
    # Simulate an older database: a headerreadattempt table lacking the newer
    # `detail` column (a minimal stand-in with the not-null key columns).
    with engine.begin() as connection:
        connection.execute(
            text(
                "CREATE TABLE headerreadattempt ("
                "id INTEGER PRIMARY KEY, created_at DATETIME, "
                "source_id VARCHAR, experiment_id VARCHAR, variant_label VARCHAR, "
                "outcome VARCHAR)"
            )
        )
    assert "detail" not in {
        c["name"] for c in inspect(engine).get_columns("headerreadattempt")
    }

    init_db(engine)  # additive migration should add the missing column

    columns = {c["name"] for c in inspect(engine).get_columns("headerreadattempt")}
    assert "detail" in columns
    init_db(engine)  # idempotent: a second run does not error
