"""Validate/import an existing Gold snapshot, then serve the demo API."""

import argparse
from pathlib import Path

import uvicorn

from recommender.api.main import create_app
from recommender.api.migrate import migrate
from recommender.config import get_settings
from recommender.pipeline.importer import import_snapshot


def prepare(snapshot_dir: Path, database_url: str) -> str:
    migrate(database_url)
    return import_snapshot(snapshot_dir, database_url)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--snapshot-dir", type=Path, required=True)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument("--prepare-only", action="store_true")
    args = parser.parse_args()
    url = get_settings().database_url
    run = prepare(args.snapshot_dir, url)
    print(f"Ready snapshot: {run}")
    if not args.prepare_only:
        print(f"Liveness: http://{args.host}:{args.port}/health")
        uvicorn.run(create_app(url), host=args.host, port=args.port)


if __name__ == "__main__":
    main()
