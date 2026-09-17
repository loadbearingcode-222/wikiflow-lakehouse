"""Integration tests run real Spark on serverless compute, from wherever pytest runs.

    uv run --group connect pytest tests/integration

They use the DEFAULT CLI profile and write only to the sandbox catalog.
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any

import pytest

SANDBOX = "wikiflow_sbx"


@pytest.fixture(scope="session")
def spark() -> Iterator[Any]:
    connect = pytest.importorskip("databricks.connect")
    session = connect.DatabricksSession.builder.profile("DEFAULT").serverless(True).getOrCreate()
    yield session
