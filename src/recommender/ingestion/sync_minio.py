"""Công bố các lô Bronze bất biến lên MinIO và tải bản sao đã kiểm tra về máy."""

import argparse
import hashlib
import json
import shutil
import tempfile
from pathlib import Path, PurePosixPath
from typing import Any

from minio.error import S3Error
from pydantic import TypeAdapter

from recommender.config import get_settings
from recommender.schemas import PathId
from recommender.streaming.consumer import create_minio_client


def _manifest_files(folder: Path, manifest: dict) -> list[tuple[dict, Path]]:
    """Xác định đường dẫn nằm trong thư mục cho phép trước khi sao chép hoặc đọc dữ liệu."""
    if manifest.get("schema_version") != 1 or not manifest.get("files"):
        raise ValueError("Invalid manifest")
    TypeAdapter(PathId).validate_python(manifest["dataset_version"])
    seen = set()
    result = []
    for entry in manifest["files"]:
        relative = PurePosixPath(entry["path"])
        if (
            relative.is_absolute()
            or ".." in relative.parts
            or "\\" in entry["path"]
            or ":" in entry["path"]
            or entry["path"] in seen
            or entry["path"] == "manifest.json"
        ):
            raise ValueError("Invalid manifest file path")
        seen.add(entry["path"])
        path = folder / relative
        if not path.resolve().is_relative_to(folder.resolve()):
            raise ValueError("File path escapes batch")
        result.append((entry, path))
    return result


def validate_manifest(folder: Path) -> dict:
    """Kiểm tra phạm vi đường dẫn, số byte và mã băm trước khi công bố hoặc kích hoạt."""
    manifest = json.loads((folder / "manifest.json").read_bytes())
    for entry, path in _manifest_files(folder, manifest):
        relative = entry["path"]
        data = path.read_bytes()
        if len(data) != entry["bytes"]:
            raise ValueError(f"Invalid file size: {relative}")
        if hashlib.sha256(data).hexdigest() != entry["sha256"]:
            raise ValueError(f"Invalid checksum: {relative}")
        if entry["format"] == "jsonl" and len(data.splitlines()) != entry["rows"]:
            raise ValueError(f"Invalid row count: {relative}")
    return manifest


def _read_object(client: Any, key: str) -> bytes | None:
    try:
        response = client.get_object("bronze", key)
    except S3Error as exc:
        if exc.code == "NoSuchKey":
            return None
        raise
    try:
        return response.read()
    finally:
        response.close()
        response.release_conn()


def sync_bronze_to_minio(
    source_dir: Path | None = None,
    *,
    client: Any = None,
) -> list[str]:
    """Công bố các lô có phiên bản và trả về tiền tố đường dẫn dùng để tải xuống."""
    source = source_dir or get_settings().data_lake_dir / "bronze"
    client = client or create_minio_client()
    for bucket in ("bronze", "silver", "gold"):
        if not client.bucket_exists(bucket):
            client.make_bucket(bucket)
    published = []
    for manifest_path in sorted(source.rglob("manifest.json")):
        if any(
            part.startswith(".") for part in manifest_path.relative_to(source).parts
        ):
            continue
        folder = manifest_path.parent
        content = manifest_path.read_bytes()
        manifest = json.loads(content)
        # Mã băm manifest còn phân biệt các lô consumer mà chỉ riêng dataset ID
        # có thể cùng mô tả một nội dung sự kiện.
        digest = hashlib.sha256(content).hexdigest()[:16]
        prefix = f"datasets/{manifest['dataset_version']}/{digest}"
        existing = _read_object(client, f"{prefix}/manifest.json")
        if existing is not None and existing != content:
            raise ValueError("Immutable remote manifest conflict")
        if existing is None:
            # Tạo bản sao riêng, tính mã băm rồi chỉ tải chính bản sao đó lên:
            # thay đổi file nguồn lúc công bố không làm đổi các byte đã kiểm tra.
            with tempfile.TemporaryDirectory(prefix="recommender-publish-") as name:
                staged = Path(name)
                for entry, path in _manifest_files(folder, manifest):
                    target = staged / entry["path"]
                    target.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copyfile(path, target)
                (staged / "manifest.json").write_bytes(content)
                validate_manifest(staged)
                for entry in manifest["files"]:
                    client.fput_object(
                        "bronze",
                        f"{prefix}/{entry['path']}",
                        str(staged / entry["path"]),
                    )
                client.fput_object(
                    "bronze", f"{prefix}/manifest.json", str(staged / "manifest.json")
                )
        # Nếu manifest đã công bố giống hệt thì bỏ qua. Không mở lại các đối tượng
        # dữ liệu để ghi, kể cả khi nguồn cục bộ đã thay đổi.
        published.append(prefix)
        print(prefix)
    return published


def download_batch(prefix: str, output_dir: Path, *, client: Any = None) -> Path:
    """Tải cả lô vào thư mục tạm cùng cấp, sau đó kích hoạt bằng thao tác nguyên tử."""
    client = client or create_minio_client()
    content = _read_object(client, prefix.rstrip("/") + "/manifest.json")
    if content is None:
        raise FileNotFoundError("Remote batch manifest is missing")
    if output_dir.exists():
        validate_manifest(output_dir)
        if (output_dir / "manifest.json").read_bytes() != content:
            raise ValueError("Output already contains a different dataset")
        return output_dir
    output_dir.parent.mkdir(parents=True, exist_ok=True)
    temporary = Path(tempfile.mkdtemp(prefix=".download-", dir=output_dir.parent))
    try:
        (temporary / "manifest.json").write_bytes(content)
        manifest = json.loads(content)
        for entry in manifest["files"]:
            relative = PurePosixPath(entry["path"])
            if (
                relative.is_absolute()
                or ".." in relative.parts
                or "\\" in entry["path"]
                or ":" in entry["path"]
            ):
                raise ValueError("Invalid manifest path")
            if relative.as_posix() == "manifest.json":
                raise ValueError("Invalid manifest path")
            data = _read_object(client, prefix.rstrip("/") + "/" + relative.as_posix())
            if data is None:
                raise FileNotFoundError(f"Missing remote file: {relative}")
            target = temporary / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(data)
        validate_manifest(temporary)
        temporary.rename(output_dir)
    finally:
        if temporary.exists():
            shutil.rmtree(temporary)
    return output_dir


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-dir", type=Path)
    parser.add_argument("--download-prefix")
    parser.add_argument("--output-dir", type=Path)
    args = parser.parse_args()
    if args.download_prefix:
        if args.output_dir is None:
            parser.error("--output-dir is required for download")
        print(download_batch(args.download_prefix, args.output_dir))
    else:
        sync_bronze_to_minio(args.source_dir)


if __name__ == "__main__":
    main()
