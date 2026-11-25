import json
from concurrent.futures import ThreadPoolExecutor

import pytest
from gold_fixture import make_gold_fixture, rewrite_table

from recommender.api.db import connect
from recommender.pipeline.importer import activate_run, import_snapshot
from recommender.pipeline.snapshot import validate_snapshot


@pytest.mark.parametrize(
    "defect",
    [
        "missing",
        "checksum",
        "escape",
        "duplicate",
        "run",
        "rank",
        "antecedent",
        "cutoff",
    ],
)
def test_invalid_snapshots_rejected(tmp_path, defect):
    root = make_gold_fixture(tmp_path)
    path = root / "manifest.json"
    manifest = json.loads(path.read_text("utf-8"))
    if defect == "missing":
        manifest["files"] = manifest["files"][:-1]
    elif defect == "checksum":
        manifest["files"][0]["sha256"] = "0" * 64
    elif defect == "escape":
        manifest["files"][0]["path"] = "../outside.parquet"
    else:
        field, value = {
            "duplicate": ("product_id", "fixture:a"),
            "run": ("run_id", "other"),
            "rank": ("rank", 9),
            "antecedent": ("antecedent_ids", ["fixture:missing"]),
            "cutoff": (
                "collected_at",
                __import__("datetime").datetime(
                    2027, 1, 1, tzinfo=__import__("datetime").UTC
                ),
            ),
        }[defect]
        name = (
            "products"
            if defect in {"duplicate", "run", "cutoff"}
            else "association_rules"
            if defect == "antecedent"
            else "recommendation_items"
        )
        rewrite_table(
            root,
            name,
            lambda rows: rows[1 if defect == "duplicate" else 0].update({field: value}),
        )
        manifest = json.loads(path.read_text("utf-8"))
    path.write_text(json.dumps(manifest), encoding="utf-8")
    with pytest.raises(ValueError):
        validate_snapshot(root)


def test_import_retry_rollback_and_conflict(tmp_path, db_url):
    first = make_gold_fixture(tmp_path, "run1")
    second = make_gold_fixture(tmp_path, "run2")
    assert import_snapshot(first, db_url) == "run1"
    import_snapshot(second, db_url)
    import_snapshot(first, db_url)
    with connect(db_url) as c:
        assert (
            c.execute("SELECT active_run_id FROM serving_state").fetchone()[0] == "run2"
        )
    activate_run("run1", db_url)
    rewrite_table(first, "products", lambda rows: rows[0].update(name="Changed"))
    with pytest.raises(ValueError, match="fingerprint"):
        import_snapshot(first, db_url)
    with connect(db_url) as c:
        assert (
            c.execute(
                "SELECT name FROM products WHERE run_id='run1' ORDER BY product_id LIMIT 1"
            ).fetchone()[0]
            == "Product a"
        )


def test_failed_import_leaves_active_catalog(tmp_path, db_url, monkeypatch):
    from recommender.pipeline import importer

    import_snapshot(make_gold_fixture(tmp_path, "run1"), db_url)
    original = importer._insert_table

    def broken(conn, name, rows):
        original(conn, name, rows)
        if name == "products":
            raise RuntimeError("injected interruption")

    monkeypatch.setattr(importer, "_insert_table", broken)
    with pytest.raises(RuntimeError):
        import_snapshot(make_gold_fixture(tmp_path, "run2"), db_url)
    with connect(db_url) as c:
        assert (
            c.execute("SELECT active_run_id FROM serving_state").fetchone()[0] == "run1"
        )
        assert (
            c.execute("SELECT count(*) FROM products WHERE run_id='run2'").fetchone()[0]
            == 0
        )
        assert (
            c.execute("SELECT status FROM batch_runs WHERE run_id='run2'").fetchone()[0]
            == "failed"
        )


def test_concurrent_same_snapshot_is_idempotent(tmp_path, db_url):
    root = make_gold_fixture(tmp_path)
    with ThreadPoolExecutor(2) as pool:
        assert (
            list(pool.map(lambda _: import_snapshot(root, db_url), range(2)))
            == ["fixture_run"] * 2
        )
    with connect(db_url) as c:
        assert c.execute("SELECT count(*) FROM products").fetchone()[0] == 5


def test_empty_rules_with_schema_are_valid(tmp_path, db_url):
    root = make_gold_fixture(tmp_path)
    rewrite_table(root, "association_rules", lambda rows: rows.clear())
    assert validate_snapshot(root).tables["association_rules"] == []
    import_snapshot(root, db_url)


def test_competing_import_does_not_overwrite_new_active(tmp_path, db_url, monkeypatch):
    from recommender.pipeline import importer

    first = make_gold_fixture(tmp_path, "first")
    second = make_gold_fixture(tmp_path, "second")
    original = importer.validate_snapshot

    def delayed(path):
        result = original(path)
        if path == first:
            import_snapshot(second, db_url)
        return result

    monkeypatch.setattr(importer, "validate_snapshot", delayed)
    with pytest.raises(ValueError, match="changed"):
        import_snapshot(first, db_url)
    with connect(db_url) as c:
        assert (
            c.execute("SELECT active_run_id FROM serving_state").fetchone()[0]
            == "second"
        )


def test_parquet_parse_uses_the_bytes_that_passed_checksum(tmp_path, monkeypatch):
    import pyarrow as pa
    import pyarrow.parquet as pq

    from recommender.pipeline import snapshot

    root = make_gold_fixture(tmp_path)
    products = root / "products.parquet"
    original = pq.read_table(products)
    changed = original.set_column(
        original.schema.get_field_index("name"),
        "name",
        pa.array(["Unverified replacement"] * 5),
    )
    read = pq.read_table

    def replace_before_parse(source, *args, **kwargs):
        pq.write_table(changed, products)
        return read(source, *args, **kwargs)

    monkeypatch.setattr(snapshot.pq, "read_table", replace_before_parse)
    validated = snapshot.validate_snapshot(root)
    assert validated.tables["products"][0]["name"] == "Product a"
