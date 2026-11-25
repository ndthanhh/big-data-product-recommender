import os
import sys

import pytest
from pyspark.sql import SparkSession

from recommender.ranking.fpgrowth import fit_rules


@pytest.fixture(scope="module")
def spark():
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    session = (
        SparkSession.builder.master("local[2]")
        .appName("fpgrowth-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def test_support_uses_only_multi_item_baskets(spark, tmp_path):
    baskets = spark.createDataFrame(
        [
            ("run1", ["fixture:a", "fixture:b", "fixture:b"]),
            ("run1", ["fixture:a", "fixture:b"]),
            ("run1", ["fixture:a", "fixture:c"]),
            ("run1", ["fixture:d"]),
        ],
        "run_id string, product_ids array<string>",
    )
    rules = fit_rules(baskets, min_support=0.1, min_confidence=0.1)
    rows = rules.collect()
    row = next(
        row
        for row in rows
        if row.antecedent_ids == ["fixture:a"] and row.consequent_id == "fixture:b"
    )
    assert row.support == pytest.approx(2 / 3)
    assert row.confidence == pytest.approx(2 / 3)
    assert row.lift == pytest.approx(1.0)


def test_empty_and_single_item_baskets(spark):
    for data in ([], [("run1", ["fixture:a"])]):
        frame = spark.createDataFrame(data, "run_id string, product_ids array<string>")
        assert fit_rules(frame, min_support=0.1, min_confidence=0.1).count() == 0
