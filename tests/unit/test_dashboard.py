"""The attention dashboard file stays deployable to every target from one definition."""

from __future__ import annotations

import json
import re
from pathlib import Path

DASHBOARD = Path(__file__).parents[2] / "src" / "dashboards" / "attention.lvdash.json"
definition = json.loads(DASHBOARD.read_text(encoding="utf-8"))
DATASETS = {d["name"]: "".join(d["queryLines"]) for d in definition["datasets"]}


def test_no_query_names_a_catalog_so_dataset_catalog_decides_per_target() -> None:
    for name, sql in DATASETS.items():
        assert not re.search(r"\bwikiflow(_sbx|_dev)?\.", sql), f"{name} names a catalog"
        assert re.search(r"\b(gold|ops|silver)\.\w+", sql), f"{name} reads no published table"


def test_marts_and_ops_only_never_bronze_or_raw() -> None:
    """The tiles are cross-checked against bronze by hand, so the dashboard must not read it."""
    for name, sql in DATASETS.items():
        assert not re.search(r"\b(bronze|raw)\.", sql), f"{name} reads bronze or raw"


def test_every_widget_query_uses_a_declared_dataset() -> None:
    used = {
        query["query"]["datasetName"]
        for page in definition["pages"]
        for placed in page["layout"]
        for query in placed["widget"].get("queries", [])
    }
    assert used == set(DATASETS)
