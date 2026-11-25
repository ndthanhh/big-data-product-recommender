"""Sync local Bronze files to MinIO S3 Object Storage."""

from minio import Minio

from recommender.config import get_settings


def sync_bronze_to_minio() -> None:
    settings = get_settings()
    client = Minio(
        settings.minio_endpoint,
        access_key=settings.minio_access_key,
        secret_key=settings.minio_secret_key,
        secure=settings.minio_secure,
    )

    # Ensure buckets exist
    for bucket in ("bronze", "silver", "gold"):
        if not client.bucket_exists(bucket):
            client.make_bucket(bucket)
            print(f"[MinIO] Created bucket: {bucket}")

    bronze_dir = settings.data_lake_dir / "bronze"
    if not bronze_dir.exists():
        print(f"[MinIO] Local bronze dir not found: {bronze_dir}")
        return

    files_to_sync = [
        ("products/products.jsonl", "products/products.jsonl"),
        ("products/manifest.json", "products/manifest.json"),
        ("users/users.jsonl", "users/users.jsonl"),
        ("users/manifest.json", "users/manifest.json"),
        ("events/events.jsonl", "events/events.jsonl"),
        ("events/manifest.json", "events/manifest.json"),
    ]

    for local_rel, s3_key in files_to_sync:
        local_path = bronze_dir / local_rel
        if local_path.exists():
            size_mb = local_path.stat().st_size / (1024 * 1024)
            print(f"[MinIO] Uploading s3a://bronze/{s3_key} ({size_mb:.2f} MB)...")
            client.fput_object("bronze", s3_key, str(local_path))
            print(f"[MinIO] Uploaded successfully: s3a://bronze/{s3_key}")
        else:
            print(f"[MinIO] Skipped (not found): {local_rel}")

    print("\n[MinIO] Sync complete! Listing objects in bronze bucket:")
    objects = client.list_objects("bronze", recursive=True)
    for obj in objects:
        print(f"  - s3a://bronze/{obj.object_name} ({obj.size} bytes)")


if __name__ == "__main__":
    sync_bronze_to_minio()
