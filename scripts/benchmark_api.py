"""Đo hiệu năng phục vụ HTTP riêng với huấn luyện, dùng cơ sở dữ liệu đo thử riêng."""

import argparse
import json
import os
import platform
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

import httpx


def percentile(values, p):
    ordered = sorted(values)
    at = (len(ordered) - 1) * p
    low = int(at)
    return ordered[low] + (ordered[min(low + 1, len(ordered) - 1)] - ordered[low]) * (
        at - low
    )


def benchmark(client, *, requests: int, concurrency: int, warmup: int = 3):
    if requests < 1 or concurrency < 1 or warmup < 0:
        raise ValueError("requests/concurrency must be positive; warmup nonnegative")
    catalog = client.get("/api/v1/products?limit=1")
    catalog.raise_for_status()
    if not catalog.json()["items"]:
        raise ValueError("Benchmark requires a nonempty snapshot")
    product = catalog.json()["items"][0]["product_id"]
    user = "benchmark-" + uuid4().hex
    context = client.get(
        "/api/v1/recommendations/personalized", params={"user_id": user}
    )
    context.raise_for_status()
    run = context.json()["run_id"]

    def call(name):
        if name == "personalized":
            return client.get(
                "/api/v1/recommendations/personalized", params={"user_id": user}
            )
        if name == "substitutes":
            return client.get(
                "/api/v1/recommendations/substitutes", params={"product_id": product}
            )
        if name == "complements":
            return client.post(
                "/api/v1/recommendations/complements", json={"product_ids": [product]}
            )
        return client.post(
            "/api/v1/events",
            json={
                "event_id": str(uuid4()),
                "user_id": user,
                "session_id": user,
                "product_id": product,
                "event_type": "click",
                "event_at": datetime.now(UTC).isoformat(),
                "source": "demo",
                "cart_id": None,
            },
        )

    result = {
        "created_at": datetime.now(UTC).isoformat(),
        "run_id": run,
        "requests_per_endpoint": requests,
        "concurrency": concurrency,
        "warmup": warmup,
        "measurement": "HTTP roundtrip incl. connection/serialization; excludes training",
        "environment": {
            "python": platform.python_version(),
            "os": platform.platform(),
            "cpu": platform.processor(),
            "logical_cpus": os.cpu_count(),
        },
        "endpoints": {},
    }
    for name in ("personalized", "substitutes", "complements", "events"):
        for _ in range(warmup):
            call(name).raise_for_status()

        def measure(_, name=name):
            begin = time.perf_counter()
            try:
                response = call(name)
                valid = response.status_code in ({201} if name == "events" else {200})
                if name != "events" and valid:
                    valid = response.json().get("run_id") == run
                return (
                    (time.perf_counter() - begin) * 1000,
                    response.status_code,
                    not valid,
                )
            except (httpx.HTTPError, ValueError):
                return (time.perf_counter() - begin) * 1000, 0, True

        begin = time.perf_counter()
        with ThreadPoolExecutor(concurrency) as pool:
            rows = list(pool.map(measure, range(requests)))
        elapsed = time.perf_counter() - begin
        durations = [row[0] for row in rows]
        errors = sum(row[2] for row in rows)
        result["endpoints"][name] = {
            "requests": requests,
            "errors": errors,
            "error_rate": errors / requests,
            "status_counts": dict(Counter(str(row[1]) for row in rows)),
            "p50_ms": percentile(durations, 0.5),
            "p95_ms": percentile(durations, 0.95),
            "requests_per_second": requests / elapsed,
            "samples": [
                {"latency_ms": row[0], "status": row[1], "error": row[2]}
                for row in rows
            ],
        }
    return result


def write_result(path: Path, result):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(result, ensure_ascii=False, indent=2, allow_nan=False) + "\n",
        encoding="utf-8",
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", default="http://127.0.0.1:8000")
    parser.add_argument("--requests", type=int, default=100)
    parser.add_argument("--concurrency", type=int, default=4)
    parser.add_argument(
        "--output", type=Path, default=Path("docs/results/api-benchmark.json")
    )
    args = parser.parse_args()
    with httpx.Client(base_url=args.base_url, timeout=15) as client:
        result = benchmark(client, requests=args.requests, concurrency=args.concurrency)
    write_result(args.output, result)
    print(json.dumps(result["endpoints"], indent=2))
    if any(value["errors"] for value in result["endpoints"].values()):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
