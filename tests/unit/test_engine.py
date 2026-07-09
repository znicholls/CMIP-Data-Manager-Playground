"""Tests for database engine creation."""

from __future__ import annotations

from sqlmodel import Session, select

from cmip_data_manager.db.engine import create_db_engine, init_db
from cmip_data_manager.db.schema import Dataset


def test_sqlite_path_creates_working_db(tmp_path):
    engine = create_db_engine(tmp_path / "x.sqlite")
    assert engine.dialect.name == "sqlite"
    init_db(engine)
    with Session(engine) as session:
        session.add(Dataset(id="d1", raw_json="{}"))
        session.commit()
        assert session.exec(select(Dataset)).one().id == "d1"


def test_full_url_is_passed_through(tmp_path):
    url = f"sqlite:///{tmp_path / 'y.sqlite'}"
    engine = create_db_engine(url)
    assert str(engine.url) == url


def test_sqlite_foreign_keys_are_enforced(tmp_path):
    engine = create_db_engine(tmp_path / "fk.sqlite")
    with engine.connect() as connection:
        result = connection.exec_driver_sql("PRAGMA foreign_keys").scalar()
    assert result == 1
