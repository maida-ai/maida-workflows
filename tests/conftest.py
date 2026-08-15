from __future__ import annotations

import os
from collections.abc import Iterator
from pathlib import Path
from uuid import uuid4

import psycopg
import pytest
from psycopg import sql
from psycopg.conninfo import make_conninfo

from maida_workflows.artifacts import ArtifactStore, ValueCodec
from maida_workflows.persistence import PostgresStore


@pytest.fixture
def postgres_store(tmp_path: Path) -> Iterator[PostgresStore]:
    base_dsn = os.environ.get("MAIDA_WORKFLOWS_TEST_DSN")
    if not base_dsn:
        pytest.skip("MAIDA_WORKFLOWS_TEST_DSN is not configured")
    schema = f"test_{uuid4().hex}"
    with psycopg.connect(base_dsn, autocommit=True) as connection:
        connection.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
    dsn = make_conninfo(base_dsn, options=f"-csearch_path={schema}")
    store = PostgresStore(dsn, ValueCodec(ArtifactStore(tmp_path / "artifacts"), inline_limit=32))
    store.upgrade()
    try:
        yield store
    finally:
        with psycopg.connect(base_dsn, autocommit=True) as connection:
            connection.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema)))
