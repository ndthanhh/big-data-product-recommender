"""Kiểm tra các lô chia sẻ đầy đủ và bất biến."""

import hashlib
import io
import json
from pathlib import Path

import pytest
from minio.error import S3Error

from recommender.ingestion import sync_minio


class Response(io.BytesIO):
    def release_conn(self):
        pass


class Store:
    def __init__(self):
        self.objects = {}
        self.fail_data = False

    def bucket_exists(self, bucket):
        return True

    def fput_object(self, bucket, key, path, **kwargs):
        if self.fail_data and not key.endswith("manifest.json"):
            raise OSError("upload interrupted")
        self.objects[key] = Path(path).read_bytes()

    def get_object(self, bucket, key):
        if key not in self.objects:
            raise S3Error(
                response=None,
                code="NoSuchKey",
                message="missing",
                resource=key,
                request_id="",
                host_id="",
            )
        return Response(self.objects[key])


def batch(root):
    folder = root / "products"
    folder.mkdir(parents=True)
    data = b'{"product_id":"fixture:p1"}\n'
    (folder / "products.jsonl").write_bytes(data)
    (folder / "manifest.json").write_text(
        json.dumps(
            {
                "schema_version": 1,
                "dataset_version": "v1",
                "files": [
                    {
                        "path": "products.jsonl",
                        "bytes": len(data),
                        "sha256": hashlib.sha256(data).hexdigest(),
                        "format": "jsonl",
                        "rows": 1,
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    return folder


def test_share_round_trip_and_cache(tmp_path):
    source = batch(tmp_path / "bronze")
    store = Store()
    prefixes = sync_minio.sync_bronze_to_minio(tmp_path / "bronze", client=store)
    assert len(prefixes) == 1
    target = tmp_path / "download"
    sync_minio.download_batch(prefixes[0], target, client=store)
    assert (target / "products.jsonl").read_bytes() == (
        source / "products.jsonl"
    ).read_bytes()
    sync_minio.download_batch(prefixes[0], target, client=store)


def test_incomplete_upload_has_no_published_manifest(tmp_path):
    batch(tmp_path / "bronze")
    store = Store()
    store.fail_data = True
    with pytest.raises(OSError, match="interrupted"):
        sync_minio.sync_bronze_to_minio(tmp_path / "bronze", client=store)
    assert not any(key.endswith("manifest.json") for key in store.objects)


def test_corrupt_file_not_activated(tmp_path):
    batch(tmp_path / "bronze")
    store = Store()
    prefixes = sync_minio.sync_bronze_to_minio(tmp_path / "bronze", client=store)
    store.objects[prefixes[0] + "/products.jsonl"] = b"corrupt"
    with pytest.raises(ValueError, match="checksum|size"):
        sync_minio.download_batch(prefixes[0], tmp_path / "download", client=store)
    assert not (tmp_path / "download").exists()


def test_manifest_cannot_escape_batch_directory(tmp_path):
    folder = batch(tmp_path / "bronze")
    manifest_path = folder / "manifest.json"
    manifest = json.loads(manifest_path.read_text("utf-8"))
    manifest["files"][0]["path"] = "../escape"
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    with pytest.raises(ValueError, match="path"):
        sync_minio.sync_bronze_to_minio(tmp_path / "bronze", client=Store())


def test_staging_directory_is_not_published(tmp_path):
    batch(tmp_path / "bronze" / ".export-staging")
    store = Store()
    assert sync_minio.sync_bronze_to_minio(tmp_path / "bronze", client=store) == []
    assert store.objects == {}


def test_publication_uses_verified_copy_and_never_rewrites_ready_batch(
    tmp_path, monkeypatch
):
    source = batch(tmp_path / "bronze")
    original = (source / "products.jsonl").read_bytes()
    validate = sync_minio.validate_manifest

    def mutate_after_validation(folder):
        result = validate(folder)
        (source / "products.jsonl").write_bytes(b"changed after validation\n")
        return result

    monkeypatch.setattr(sync_minio, "validate_manifest", mutate_after_validation)
    store = Store()
    prefix = sync_minio.sync_bronze_to_minio(tmp_path / "bronze", client=store)[0]
    assert store.objects[prefix + "/products.jsonl"] == original
    (source / "products.jsonl").write_bytes(original)

    def must_not_upload(*args, **kwargs):
        raise AssertionError("Published batches must never be rewritten")

    store.fput_object = must_not_upload
    sync_minio.sync_bronze_to_minio(tmp_path / "bronze", client=store)
