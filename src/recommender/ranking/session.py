"""Xếp hạng theo phiên có tính xác định và giới hạn xử lý; không huấn luyện trong yêu cầu."""

from collections import defaultdict
from collections.abc import Sequence
from datetime import datetime, timedelta

from recommender.schemas import EVENT_WEIGHTS, Event, Recommendation, RecommendationItem


def normalize(scores: dict[str, float]) -> dict[str, float]:
    if not scores:
        return {}
    low, high = min(scores.values()), max(scores.values())
    return {
        key: ((value - low) / (high - low) if high != low else float(high != 0))
        for key, value in scores.items()
    }


def _window(events, request_time):
    return [
        event
        for event in events
        if request_time - timedelta(minutes=30) <= event.event_at <= request_time
    ]


def session_scores(
    events: Sequence[Event],
    similarities: Sequence[RecommendationItem],
    request_time: datetime,
) -> dict[str, float]:
    recent = sorted(
        _window(events, request_time), key=lambda e: e.event_at, reverse=True
    )[:20]
    by_source = defaultdict(list)
    for row in similarities:
        if row.kind == "substitute" and row.score > 0:
            by_source[row.context_id].append(row)
    result = defaultdict(float)
    for event in recent:
        age = (request_time - event.event_at).total_seconds() / 60
        for row in by_source[event.product_id]:
            result[row.product_id] += (
                EVENT_WEIGHTS[event.event_type] * 2 ** (-age / 10) * row.score
            )
    return dict(result)


def rerank_session(
    batch_items: Sequence[RecommendationItem],
    popularity_items: Sequence[RecommendationItem],
    session_events: Sequence[Event],
    similarities: Sequence[RecommendationItem],
    seen_ids: set[str],
    catalog_ids: set[str],
    request_time: datetime,
    limit: int,
) -> list[Recommendation]:
    blocked = seen_ids | {
        event.product_id for event in _window(session_events, request_time)
    }

    def allowed(row):
        return row.product_id in catalog_ids and row.product_id not in blocked

    def append(result, rows):
        used = {row.product_id for row in result}
        for row in rows:
            if len(result) >= limit:
                break
            if allowed(row) and row.product_id not in used:
                result.append(
                    Recommendation(
                        product_id=row.product_id,
                        rank=len(result) + 1,
                        score=row.score,
                        method=row.method,
                    )
                )
                used.add(row.product_id)
        return result

    signal = {
        key: value
        for key, value in session_scores(
            session_events, similarities, request_time
        ).items()
        if key in catalog_ids and key not in blocked and value > 0
    }
    if not signal:
        return append([], list(batch_items) + list(popularity_items))
    primary = [
        row for row in batch_items if row.method != "popularity" and allowed(row)
    ]
    source = (
        primary
        if any(row.method != "popularity" for row in batch_items)
        else [row for row in popularity_items if allowed(row)]
    )
    batch = normalize({row.product_id: row.score for row in source})
    session = normalize(signal)
    combined = {
        key: 0.7 * batch.get(key, 0) + 0.3 * session.get(key, 0)
        for key in batch.keys() | session.keys()
    }
    ordered = sorted(combined, key=lambda key: (-combined[key], key))[:200]
    result = [
        Recommendation(product_id=key, rank=n, score=combined[key], method="session")
        for n, key in enumerate(ordered[:limit], 1)
    ]
    return append(result, popularity_items)
