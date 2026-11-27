"""Publication cache identity must cover the complete requested learning run."""

from datetime import UTC, datetime

import pytest

from recommender.pipeline.lake import LocalLakeStore
from recommender.pipeline.publish_snapshot import build_snapshot, make_run_id

START = datetime(2026, 10, 1, tzinfo=UTC)
END = datetime(2026, 11, 8, tzinfo=UTC)
SELECTION = {
    "als": {"rank": 4},
    "mmr_lambda": 0.7,
    "fpgrowth": {"min_support": 0.1, "min_confidence": 0.2},
    "personalized_method": "als_mmr",
}


def test_identity_includes_training_start_and_evaluation_grid():
    def identity(start=START, grid=None):
        return make_run_id(
            "run",
            "ds-fixture",
            SELECTION,
            42,
            END,
            training_start=start,
            evaluated=grid is not None,
            evaluation_config=grid,
        )

    assert identity() != identity(datetime(2026, 10, 2, tzinfo=UTC))
    one = {
        "grids": {"mmr_lambda": [0.7]},
        "created_at": "one",
        "environment": {"os": "one"},
    }
    two = {"grids": {"mmr_lambda": [0.7, 1.0]}, "created_at": "one"}
    assert identity(grid=one) != identity(grid=two)
    assert identity(grid=one) == identity(
        grid={**one, "created_at": "later", "environment": {"os": "two"}}
    )


@pytest.mark.parametrize(
    "changed", ["training_start", "dataset_version", "selection", "evaluation"]
)
def test_existing_run_cannot_hide_a_different_request(tmp_path, changed):
    from types import SimpleNamespace

    store = LocalLakeStore(tmp_path)
    manifest = {
        "run_id": "published",
        "dataset_version": "ds-fixture",
        "seed": 42,
        "training_start": "2026-10-01T00:00:00Z",
        "training_end": "2026-11-08T00:00:00Z",
        "evaluation_window": None,
        "parameters": {"selection": SELECTION},
    }
    store.write_json("gold", "published", "manifest.json", value=manifest)
    args = {
        "dataset_version": "ds-fixture",
        "selection": SELECTION,
        "seed": 42,
        "training_start": START,
        "training_end": END,
        "run_id": "published",
        "log": lambda _: None,
    }
    if changed == "training_start":
        args[changed] = datetime(2026, 10, 2, tzinfo=UTC)
    elif changed == "dataset_version":
        args[changed] = "ds-other"
    elif changed == "selection":
        args[changed] = {**SELECTION, "mmr_lambda": 1.0}
    else:
        args["evaluation"] = SimpleNamespace(
            config={"split": {}, "grids": {"mmr_lambda": [0.7]}}
        )
    with pytest.raises(ValueError, match="different"):
        build_snapshot(None, store, None, None, **args)
