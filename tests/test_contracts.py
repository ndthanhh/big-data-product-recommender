"""Offline acceptance tests for the shared v1 data contracts (T02)."""

import json
from collections import Counter, defaultdict
from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from uuid import UUID

import pytest
from pydantic import ValidationError

from recommender.config import (
    DEFAULT_API_BASE_URL,
    DEFAULT_DATABASE_URL,
    PROJECT_ROOT,
    get_settings,
)
from recommender.schemas import (
    EVENT_WEIGHTS,
    AssociationRule,
    Basket,
    ComplementsRequest,
    ContractModel,
    ErrorDetail,
    ErrorField,
    ErrorResponse,
    Event,
    EventCreate,
    EventResponse,
    GoldProduct,
    HealthResponse,
    Interaction,
    PersonalizedQuery,
    Product,
    ProductsQuery,
    ProductsResponse,
    Recommendation,
    RecommendationItem,
    RecommendationResponse,
    SeenProduct,
    SubstitutesQuery,
    validate_recommendation_items,
)

FIXTURES = Path(__file__).parent / "fixtures"
RUN_ID = "run_20261108T000000Z_als_42"
UTC_TIME = "2026-10-20T04:00:00Z"
EVENT_ID = "ed8f2fa8-9b81-43b6-8bd4-9b8115640866"
PRODUCT = {
    "product_id": "fixture:p001",
    "name": "USB-C keyboard",
    "category_id": "keyboard",
    "source": "fixture",
    "collected_at": "2026-10-01T00:00:00Z",
}
EVENT = {
    "event_id": EVENT_ID,
    "user_id": "u_fixture_001",
    "session_id": "s_fixture_001",
    "product_id": "fixture:p001",
    "event_type": "view",
    "event_at": UTC_TIME,
    "source": "demo",
}
INTERACTION = {
    "run_id": RUN_ID,
    "user_id": "u_fixture_001",
    "product_id": "fixture:p001",
    "interaction_weight": 7.0,
    "last_event_at": UTC_TIME,
}
BASKET = {
    "run_id": RUN_ID,
    "cart_id": "c_fixture_001",
    "user_id": "u_fixture_001",
    "session_id": "s_fixture_001",
    "product_ids": ["fixture:p001", "fixture:p016"],
    "first_event_at": UTC_TIME,
    "last_event_at": "2026-10-20T04:04:00Z",
}
RECOMMENDATION = {
    "product_id": "fixture:p002",
    "rank": 1,
    "score": 0.82,
    "method": "tfidf",
}
GOLD_RECOMMENDATION = {
    **RECOMMENDATION,
    "run_id": RUN_ID,
    "kind": "substitute",
    "context_id": "fixture:p001",
}
RULE = {
    "run_id": RUN_ID,
    "antecedent_ids": ["fixture:p001"],
    "consequent_id": "fixture:p016",
    "support": 0.12,
    "confidence": 0.6,
    "lift": 1.5,
}


def read_jsonl(filename: str, model: type[ContractModel]) -> list[ContractModel]:
    """Parse every fixture line through the same JSON entry point as ingestion."""
    data = (FIXTURES / filename).read_bytes()
    assert b"\r" not in data
    assert not data.startswith(b"\xef\xbb\xbf")
    assert data.endswith(b"\n")
    lines = data.decode("utf-8").splitlines()
    assert all(line.strip() for line in lines)
    return [model.model_validate_json(line) for line in lines]


def test_offline_fixtures_and_identity_integrity() -> None:
    products = read_jsonl("products.jsonl", Product)
    events = read_jsonl("events.jsonl", Event)
    assert len(products) == 20
    assert len(events) == 50
    assert len({product.category_id for product in products}) == 4
    catalog = {product.product_id for product in products}
    assert len(catalog) == len(products)
    assert len({event.event_id for event in events}) == len(events)
    assert {event.product_id for event in events} <= catalog
    assert {event.event_type for event in events} == set(EVENT_WEIGHTS)
    assert {event.source for event in events} == {"synthetic", "demo"}
    assert all(UUID(event.event_id).version == 4 for event in events)

    sessions: dict[str, set[str]] = defaultdict(set)
    owners: dict[str, set[tuple[str, str]]] = defaultdict(set)
    baskets: dict[str, set[str]] = defaultdict(set)
    for event in events:
        sessions[event.session_id].add(event.user_id)
        if event.cart_id is not None:
            owners[event.cart_id].add((event.user_id, event.session_id))
        if event.event_type == "add_to_cart":
            assert event.cart_id is not None
            baskets[event.cart_id].add(event.product_id)
    assert all(len(users) == 1 for users in sessions.values())
    assert all(len(identities) == 1 for identities in owners.values())
    assert len(baskets) == 10
    assert all(len(items) >= 2 for items in baskets.values())
    # The first fixture journey provides a known aggregate for downstream ETL.
    counts = Counter(
        event.event_type
        for event in events
        if event.user_id == "u_fixture_001" and event.product_id == "fixture:p001"
    )
    assert sum(counts[kind] * weight for kind, weight in EVENT_WEIGHTS.items()) == 7
    assert EVENT_WEIGHTS == {"view": 1, "click": 2, "add_to_cart": 4}
    times = [event.event_at for event in events]
    assert min(times) < datetime(2026, 10, 25, tzinfo=UTC) < max(times)
    assert max(times) >= datetime(2026, 11, 1, tzinfo=UTC)


@pytest.mark.parametrize(
    "filename,model", [("products.jsonl", Product), ("events.jsonl", Event)]
)
def test_fixture_json_round_trips(filename: str, model: type[ContractModel]) -> None:
    for record in read_jsonl(filename, model):
        encoded = record.model_dump_json()
        assert model.model_validate_json(encoded) == record
        assert all(
            value.endswith("Z")
            for key, value in json.loads(encoded).items()
            if key.endswith("_at")
        )


def test_product_defaults_and_optional_normalization() -> None:
    product = Product(**PRODUCT, description=" \t ", brand="\n")
    assert product.model_dump(mode="json") == {
        **PRODUCT,
        "description": None,
        "brand": None,
        "image_url": None,
        "product_url": None,
        "attributes": {},
    }
    product.attributes["connector"] = "usb-c"
    assert Product(**PRODUCT).attributes == {}
    assert Product(**PRODUCT, image_url="https://example.com/a.png").image_url
    assert Product(**PRODUCT, product_url="http://example.com/p001").product_url


@pytest.mark.parametrize(
    "changes",
    [
        {"product_id": "p001"},
        {"product_id": ":p001"},
        {"product_id": "fixture:"},
        {"product_id": "fixture:p 001"},
        {"product_id": "fixture:p\t001"},
        {"product_id": "other:p001"},
        {"product_id": "fixture:" + "p" * 121},
        {"source": ""},
        {"source": "fixture:x"},
        {"source": " fixture"},
        {"source": "x" * 65},
        {"name": " \t"},
        {"name": "x" * 513},
        {"name": None},
        {"category_id": "Keyboard"},
        {"category_id": ""},
        {"category_id": "x" * 129},
        {"description": "x" * 20001},
        {"brand": "x" * 257},
        {"attributes": None},
        {"attributes": {"": "value"}},
        {"attributes": {"connector": None}},
        {"attributes": {"nested": {"connector": "usb-c"}}},
        {"attributes": {"nested": ["usb-c"]}},
        {"attributes": {"length": 1}},
        {"attributes": {1: "usb-c"}},
        {"image_url": "/relative.png"},
        {"image_url": "ftp://example.com/a.png"},
        {"image_url": "https://"},
        {"image_url": "https:example.com"},
        {"product_url": "http:/example.com"},
        {"image_url": "https://example.com/a b.png"},
        {"product_url": "https://example.com/" + "x" * 2048},
    ],
)
def test_invalid_product_fields(changes: dict[str, Any]) -> None:
    with pytest.raises(ValidationError):
        Product.model_validate({**PRODUCT, **changes})


@pytest.mark.parametrize(
    "value",
    ["", " ", " u1", "u1 ", "u\n1", "u\x001", "u\x7f1", "x" * 129, 1, True, None],
)
def test_business_ids_are_not_coerced_or_trimmed(value: object) -> None:
    with pytest.raises(ValidationError):
        Event.model_validate({**EVENT, "user_id": value})


def test_unicode_ids_are_case_sensitive() -> None:
    first = Event(**{**EVENT, "user_id": "người_dùng_A"})
    second = Event(**{**EVENT, "user_id": "người_dùng_a"})
    assert first.user_id != second.user_id


@pytest.mark.parametrize(
    "value", [EVENT_ID.upper(), EVENT_ID.replace("-", ""), UUID(EVENT_ID)]
)
def test_uuid_is_normalized(value: object) -> None:
    event = Event.model_validate({**EVENT, "event_id": value})
    assert event.event_id == EVENT_ID
    assert EventResponse(event_id=value).model_dump() == {"event_id": EVENT_ID}


def test_contract_v1_accepts_valid_non_v4_uuid() -> None:
    value = "6ba7b810-9dad-11d1-80b4-00c04fd430c8"
    assert Event(**{**EVENT, "event_id": value}).event_id == value


@pytest.mark.parametrize("value", ["", "not-a-uuid", "ed8f2fa8", None, 42, True])
def test_invalid_uuid(value: object) -> None:
    with pytest.raises(ValidationError):
        Event.model_validate({**EVENT, "event_id": value})


@pytest.mark.parametrize(
    "value",
    [
        "2026-10-20T04:00:00",
        "2026-10-20",
        "2026-10-20T11:00:00+07:00",
        "2026-10-20T04:00:00+00:30",
        "2026-10-20T04:00:00-00:00",
        "2026-10-20 04:00:00Z",
        "2026-02-30T04:00:00Z",
        "2026-10-20T04:00:00.1234567Z",
        datetime(2026, 10, 20, 4),
        datetime(2026, 10, 20, 11, tzinfo=timezone(timedelta(hours=7))),
        1792468800,
        True,
        None,
    ],
)
def test_non_utc_or_invalid_timestamps_are_rejected(value: object) -> None:
    with pytest.raises(ValidationError):
        Event.model_validate({**EVENT, "event_at": value})


@pytest.mark.parametrize(
    "model,payload,field",
    [
        (Product, PRODUCT, "collected_at"),
        (Event, EVENT, "event_at"),
        (GoldProduct, {**PRODUCT, "run_id": RUN_ID}, "collected_at"),
        (Interaction, INTERACTION, "last_event_at"),
        (Basket, BASKET, "first_event_at"),
        (Basket, BASKET, "last_event_at"),
    ],
)
def test_all_timestamp_fields_validate_and_serialize_utc(
    model: type[ContractModel], payload: dict[str, Any], field: str
) -> None:
    for value in [
        UTC_TIME,
        UTC_TIME.replace("Z", "+00:00"),
        datetime(2026, 10, 20, 4, tzinfo=UTC),
    ]:
        record = model.model_validate({**payload, field: value})
        assert record.model_dump(mode="json")[field] == UTC_TIME
        assert getattr(record, field).tzinfo == UTC
    with pytest.raises(ValidationError):
        model.model_validate({**payload, field: "2026-10-20T04:00:00"})


def test_utc_equivalence_and_microsecond_precision() -> None:
    first = Event(**EVENT)
    second = Event(
        **{**EVENT, "event_at": UTC_TIME.replace("Z", "+00:00"), "cart_id": None}
    )
    assert first == second
    precise = Event(**{**EVENT, "event_at": "2026-10-20T04:00:00.123456+00:00"})
    assert precise.model_dump(mode="json")["event_at"] == "2026-10-20T04:00:00.123456Z"


@pytest.mark.parametrize("value", [None, "", " ", " c1", "c1\n", 123])
def test_add_to_cart_requires_valid_cart_id(value: object) -> None:
    with pytest.raises(ValidationError):
        Event.model_validate({**EVENT, "event_type": "add_to_cart", "cart_id": value})


def test_add_to_cart_does_not_invent_cart_id() -> None:
    with pytest.raises(ValidationError, match="cart_id"):
        Event(**{**EVENT, "event_type": "add_to_cart"})
    assert (
        Event(**{**EVENT, "event_type": "add_to_cart", "cart_id": "c1"}).cart_id == "c1"
    )


@pytest.mark.parametrize("event_type", ["view", "click"])
def test_non_cart_events_allow_missing_null_or_valid_cart(event_type: str) -> None:
    assert Event(**{**EVENT, "event_type": event_type}).cart_id is None
    assert Event(**{**EVENT, "event_type": event_type, "cart_id": None}).cart_id is None
    assert Event(**{**EVENT, "event_type": event_type, "cart_id": "c1"}).cart_id == "c1"
    with pytest.raises(ValidationError):
        Event(**{**EVENT, "event_type": event_type, "cart_id": ""})


@pytest.mark.parametrize(
    "changes",
    [
        {"event_type": "purchase"},
        {"event_type": None},
        {"source": "fixture"},
        {"source": None},
        {"session_id": ""},
    ],
)
def test_event_enums_and_session_id(changes: dict[str, Any]) -> None:
    with pytest.raises(ValidationError):
        Event.model_validate({**EVENT, **changes})


@pytest.mark.parametrize("field", list(PRODUCT))
def test_product_requires_core_fields(field: str) -> None:
    with pytest.raises(ValidationError):
        Product.model_validate(
            {key: value for key, value in PRODUCT.items() if key != field}
        )


@pytest.mark.parametrize("field", list(EVENT))
def test_event_requires_core_fields(field: str) -> None:
    with pytest.raises(ValidationError):
        Event.model_validate(
            {key: value for key, value in EVENT.items() if key != field}
        )


def test_gold_aggregate_records() -> None:
    assert GoldProduct(**PRODUCT, run_id=RUN_ID).run_id == RUN_ID
    assert Interaction(**INTERACTION).interaction_weight == 7
    assert Basket(**BASKET).product_ids == ["fixture:p001", "fixture:p016"]
    assert (
        SeenProduct(run_id=RUN_ID, user_id="u1", product_id="fixture:p001").user_id
        == "u1"
    )


@pytest.mark.parametrize(
    "value", [0, -1, float("nan"), float("inf"), -float("inf"), True, "7", None]
)
def test_interaction_weight_is_finite_and_positive(value: object) -> None:
    with pytest.raises(ValidationError):
        Interaction.model_validate({**INTERACTION, "interaction_weight": value})


@pytest.mark.parametrize(
    "value",
    ["", "../run", "/run", "run/name", "run\\name", ".run", "run id", "x" * 129, None],
)
def test_run_ids_are_safe_path_components(value: object) -> None:
    with pytest.raises(ValidationError):
        Interaction.model_validate({**INTERACTION, "run_id": value})


@pytest.mark.parametrize(
    "field,payload", [("product_ids", BASKET), ("antecedent_ids", RULE)]
)
@pytest.mark.parametrize(
    "value",
    [
        [],
        None,
        [None],
        ["fixture:p002", "fixture:p001"],
        ["fixture:p001", "fixture:p001"],
        ["p001"],
    ],
)
def test_gold_product_sets_must_be_nonempty_sorted_and_unique(
    field: str, payload: dict[str, Any], value: object
) -> None:
    model = Basket if field == "product_ids" else AssociationRule
    with pytest.raises(ValidationError):
        model.model_validate({**payload, field: value})


def test_basket_time_range() -> None:
    assert (
        Basket(**{**BASKET, "last_event_at": UTC_TIME}).first_event_at
        == Basket(**BASKET).first_event_at
    )
    with pytest.raises(ValidationError):
        Basket(**{**BASKET, "last_event_at": "2026-10-20T03:59:59Z"})


@pytest.mark.parametrize(
    "method,score",
    [
        ("tfidf", 0),
        ("tfidf", 1),
        ("popularity", 7),
        ("als", -3.4),
        ("als_mmr", -0.5),
        ("session", 0.3),
        ("fpgrowth", 1),
    ],
)
def test_recommendation_score_domains_allow_valid_values(
    method: str, score: float
) -> None:
    assert (
        Recommendation(**{**RECOMMENDATION, "method": method, "score": score}).score
        == score
    )


@pytest.mark.parametrize(
    "method,score",
    [
        ("tfidf", -0.01),
        ("tfidf", 1.01),
        ("popularity", 0),
        ("popularity", -1),
        ("session", 1.01),
        ("fpgrowth", 0),
        ("fpgrowth", 1.01),
    ],
)
def test_invalid_method_score_domains(method: str, score: float) -> None:
    with pytest.raises(ValidationError):
        Recommendation(**{**RECOMMENDATION, "method": method, "score": score})


@pytest.mark.parametrize(
    "value", [float("nan"), float("inf"), -float("inf"), None, True, "0.82"]
)
def test_recommendation_score_must_be_a_finite_number(value: object) -> None:
    with pytest.raises(ValidationError):
        Recommendation(**{**RECOMMENDATION, "score": value})


@pytest.mark.parametrize("value", [0, -1, 1.5, True, "1", None, 2147483648])
def test_rank_is_a_positive_32_bit_integer(value: object) -> None:
    with pytest.raises(ValidationError):
        Recommendation(**{**RECOMMENDATION, "rank": value})


@pytest.mark.parametrize(
    "changes",
    [
        {"method": "session"},
        {"method": "fpgrowth"},
        {"method": "tfidf_substitute"},
        {"kind": "substitutes"},
        {"method": "als"},
        {"context_id": "u1"},
        {"context_id": "fixture:p002"},
        {"kind": "popular", "context_id": "global"},
        {"kind": "popular", "method": "popularity"},
    ],
)
def test_gold_kind_context_and_method_constraints(changes: dict[str, Any]) -> None:
    with pytest.raises(ValidationError):
        RecommendationItem.model_validate({**GOLD_RECOMMENDATION, **changes})


def test_gold_recommendation_kinds() -> None:
    assert RecommendationItem(**GOLD_RECOMMENDATION).method == "tfidf"
    for method in ["als", "als_mmr", "tfidf", "popularity"]:
        assert RecommendationItem(
            **{
                **GOLD_RECOMMENDATION,
                "kind": "personalized",
                "context_id": "u1",
                "method": method,
            }
        )
    assert RecommendationItem(
        **{
            **GOLD_RECOMMENDATION,
            "kind": "popular",
            "context_id": "global",
            "method": "popularity",
        }
    )


def test_gold_recommendation_table_keys_ranks_and_groups() -> None:
    first = RecommendationItem(**GOLD_RECOMMENDATION)
    second = RecommendationItem(
        **{**GOLD_RECOMMENDATION, "product_id": "fixture:p003", "rank": 2}
    )
    another = RecommendationItem(
        **{**GOLD_RECOMMENDATION, "context_id": "fixture:p004"}
    )
    validate_recommendation_items([])
    validate_recommendation_items([second, another, first])
    for items in [
        [first, first],
        [second],
        [first, second.model_copy(update={"rank": 1})],
        [first, second.model_copy(update={"rank": 3})],
    ]:
        with pytest.raises(ValueError):
            validate_recommendation_items(items)
    too_many = [
        RecommendationItem(
            **{**GOLD_RECOMMENDATION, "product_id": f"fixture:x{rank}", "rank": rank}
        )
        for rank in range(1, 202)
    ]
    with pytest.raises(ValueError, match="200"):
        validate_recommendation_items(too_many)


def test_association_rule_metrics_and_consequent() -> None:
    assert AssociationRule(**RULE).confidence == 0.6
    assert AssociationRule(**{**RULE, "support": 1, "confidence": 1}).support == 1
    with pytest.raises(ValidationError):
        AssociationRule(**{**RULE, "consequent_id": "fixture:p001"})


@pytest.mark.parametrize("field", ["support", "confidence", "lift"])
@pytest.mark.parametrize(
    "value", [0, -1, float("nan"), float("inf"), -float("inf"), True, None, "0.5"]
)
def test_rule_metrics_are_finite_positive_numbers(field: str, value: object) -> None:
    with pytest.raises(ValidationError):
        AssociationRule(**{**RULE, field: value})


@pytest.mark.parametrize("field", ["support", "confidence"])
def test_rule_probabilities_cannot_exceed_one(field: str) -> None:
    with pytest.raises(ValidationError):
        AssociationRule(**{**RULE, field: 1.01})


def test_api_request_defaults_and_bounds() -> None:
    assert ProductsQuery().model_dump() == {"limit": 20, "offset": 0}
    assert ProductsQuery(limit=100, offset=1000).limit == 100
    assert PersonalizedQuery(user_id="new_user").limit == 10
    assert PersonalizedQuery(user_id="u1", session_id="s1", limit=50).session_id == "s1"
    assert SubstitutesQuery(product_id="fixture:p001").limit == 10
    assert ComplementsRequest(product_ids=[]).product_ids == []
    assert ComplementsRequest(
        product_ids=["fixture:p002", "fixture:p001", "fixture:p002"]
    ).product_ids == ["fixture:p001", "fixture:p002"]
    assert len(ComplementsRequest(product_ids=["fixture:p001"] * 100).product_ids) == 1
    assert EventCreate(**EVENT).source == "demo"
    with pytest.raises(ValidationError):
        EventCreate(**{**EVENT, "source": "synthetic"})
    with pytest.raises(ValidationError):
        EventCreate(**{**EVENT, "event_type": "add_to_cart"})


@pytest.mark.parametrize("value", [0, -1, 51, True, 1.5, "10", None])
@pytest.mark.parametrize(
    "model,payload",
    [
        (PersonalizedQuery, {"user_id": "u1"}),
        (SubstitutesQuery, {"product_id": "fixture:p001"}),
        (ComplementsRequest, {"product_ids": []}),
    ],
)
def test_recommendation_limit_constraints(
    model: type[ContractModel], payload: dict[str, Any], value: object
) -> None:
    with pytest.raises(ValidationError):
        model.model_validate({**payload, "limit": value})


@pytest.mark.parametrize(
    "model,payload",
    [
        (ProductsQuery, {"limit": 101}),
        (ProductsQuery, {"limit": 0}),
        (ProductsQuery, {"offset": -1}),
        (ProductsQuery, {"offset": True}),
        (PersonalizedQuery, {}),
        (PersonalizedQuery, {"user_id": ""}),
        (PersonalizedQuery, {"user_id": "u1", "session_id": ""}),
        (SubstitutesQuery, {"product_id": "p001"}),
        (ComplementsRequest, {}),
        (ComplementsRequest, {"product_ids": None}),
        (ComplementsRequest, {"product_ids": "fixture:p001"}),
        (ComplementsRequest, {"product_ids": [None]}),
        (ComplementsRequest, {"product_ids": ["fixture:p001"] * 101}),
    ],
)
def test_invalid_api_request_shapes(
    model: type[ContractModel], payload: dict[str, Any]
) -> None:
    with pytest.raises(ValidationError):
        model.model_validate(payload)


def test_api_response_envelopes() -> None:
    assert HealthResponse().model_dump() == {"status": "ok"}
    assert ProductsResponse(items=[]).model_dump() == {"items": []}
    products = ProductsResponse(items=[Product(**PRODUCT)])
    assert "run_id" not in products.model_dump()["items"][0]
    assert EventResponse(event_id=EVENT_ID).model_dump() == {"event_id": EVENT_ID}
    assert RecommendationResponse(run_id=RUN_ID, items=[]).model_dump() == {
        "run_id": RUN_ID,
        "items": [],
    }
    response = RecommendationResponse(run_id=RUN_ID, items=[RECOMMENDATION])
    assert json.loads(response.model_dump_json()) == {
        "run_id": RUN_ID,
        "items": [RECOMMENDATION],
    }
    for method in ["session", "fpgrowth"]:
        assert RecommendationResponse(
            run_id=RUN_ID, items=[{**RECOMMENDATION, "method": method}]
        )
    error = ErrorResponse(
        detail={"code": "SNAPSHOT_UNAVAILABLE", "message": "No snapshot"}
    )
    assert error.model_dump() == {
        "detail": {
            "code": "SNAPSHOT_UNAVAILABLE",
            "message": "No snapshot",
            "fields": [],
        }
    }
    validation = ErrorResponse(
        detail={
            "code": "VALIDATION_ERROR",
            "message": "Invalid cart",
            "fields": [{"path": "body.cart_id", "message": "Required"}],
        }
    )
    assert validation.detail.fields[0].path == "body.cart_id"


@pytest.mark.parametrize(
    "model,payload",
    [
        (HealthResponse, {"status": "failed"}),
        (ProductsResponse, {"items": None}),
        (RecommendationResponse, {"run_id": RUN_ID, "items": None}),
        (
            RecommendationResponse,
            {"run_id": RUN_ID, "items": [{**RECOMMENDATION, "rank": 2}]},
        ),
        (
            RecommendationResponse,
            {
                "run_id": RUN_ID,
                "items": [RECOMMENDATION, {**RECOMMENDATION, "rank": 2}],
            },
        ),
        (ErrorResponse, {"detail": "Not found"}),
        (ErrorDetail, {"code": "OTHER", "message": "Invalid"}),
        (
            ErrorDetail,
            {"code": "VALIDATION_ERROR", "message": "Invalid", "fields": None},
        ),
        (ErrorField, {"path": 1, "message": "Invalid"}),
    ],
)
def test_invalid_api_response_shapes(
    model: type[ContractModel], payload: dict[str, Any]
) -> None:
    with pytest.raises(ValidationError):
        model.model_validate_json(json.dumps(payload))


@pytest.mark.parametrize(
    "model,payload",
    [
        (Product, PRODUCT),
        (Event, EVENT),
        (GoldProduct, {**PRODUCT, "run_id": RUN_ID}),
        (Interaction, INTERACTION),
        (Basket, BASKET),
        (
            SeenProduct,
            {"run_id": RUN_ID, "user_id": "u1", "product_id": "fixture:p001"},
        ),
        (RecommendationItem, GOLD_RECOMMENDATION),
        (AssociationRule, RULE),
        (ProductsQuery, {}),
        (PersonalizedQuery, {"user_id": "u1"}),
        (SubstitutesQuery, {"product_id": "fixture:p001"}),
        (ComplementsRequest, {"product_ids": []}),
        (EventCreate, EVENT),
        (HealthResponse, {}),
        (ProductsResponse, {"items": []}),
        (EventResponse, {"event_id": EVENT_ID}),
        (Recommendation, RECOMMENDATION),
        (RecommendationResponse, {"run_id": RUN_ID, "items": []}),
        (ErrorField, {"path": "body", "message": "Invalid"}),
        (ErrorDetail, {"code": "VALIDATION_ERROR", "message": "Invalid"}),
        (ErrorResponse, {"detail": {"code": "INTERNAL_ERROR", "message": "Error"}}),
    ],
)
def test_every_contract_rejects_unknown_fields(
    model: type[ContractModel], payload: dict[str, Any]
) -> None:
    assert model.model_validate_json(json.dumps(payload))
    with pytest.raises(ValidationError, match="extra_forbidden"):
        model.model_validate_json(json.dumps({**payload, "unknown": 1}))


@pytest.mark.parametrize(
    "field", ["received_at", "catalog_run_id", "interaction_weight", "schema_version"]
)
def test_event_rejects_server_and_derived_fields(field: str) -> None:
    with pytest.raises(ValidationError):
        EventCreate.model_validate({**EVENT, field: 1})


@pytest.fixture
def clean_config_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for key in ["DATA_LAKE_DIR", "DATABASE_URL", "API_BASE_URL", "RANDOM_SEED"]:
        monkeypatch.delenv(key, raising=False)


@pytest.mark.usefixtures("clean_config_env")
def test_config_defaults_are_independent_of_working_directory(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    settings = get_settings(env_file=None)
    assert settings.data_lake_dir == PROJECT_ROOT / "data"
    assert settings.database_url == DEFAULT_DATABASE_URL
    assert settings.api_base_url == DEFAULT_API_BASE_URL
    assert settings.random_seed == 42
    assert get_settings(env_file=tmp_path / "missing.env") == settings


@pytest.mark.usefixtures("clean_config_env")
def test_config_env_file_and_environment_precedence(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text(
        'DATA_LAKE_DIR="fixture lake"\nDATABASE_URL=postgresql://file/db\nAPI_BASE_URL=http://file:8000\nRANDOM_SEED=7\n',
        encoding="utf-8",
    )
    file_settings = get_settings(env_file)
    assert file_settings.data_lake_dir == PROJECT_ROOT / "fixture lake"
    assert file_settings.database_url == "postgresql://file/db"
    assert file_settings.api_base_url == "http://file:8000"
    assert file_settings.random_seed == 7
    assert get_settings(env_file=None).random_seed == 42
    # Reading .env does not leak values into later calls or os.environ.
    absolute_lake = tmp_path / "lake"
    for key, value in {
        "DATA_LAKE_DIR": str(absolute_lake),
        "DATABASE_URL": "postgresql://env/db",
        "API_BASE_URL": "http://env:9000",
        "RANDOM_SEED": "99",
    }.items():
        monkeypatch.setenv(key, value)
    settings = get_settings(env_file)
    assert settings.data_lake_dir == absolute_lake
    assert settings.database_url == "postgresql://env/db"
    assert settings.api_base_url == "http://env:9000"
    assert settings.random_seed == 99
    monkeypatch.setenv("RANDOM_SEED", "100")
    assert get_settings(env_file).random_seed == 100


@pytest.mark.usefixtures("clean_config_env")
@pytest.mark.parametrize(
    "key,value",
    [
        ("RANDOM_SEED", "abc"),
        ("RANDOM_SEED", "1.5"),
        ("RANDOM_SEED", ""),
        ("DATA_LAKE_DIR", ""),
        ("DATABASE_URL", " "),
        ("API_BASE_URL", ""),
    ],
)
def test_config_rejects_invalid_values(
    key: str, value: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv(key, value)
    with pytest.raises(ValueError, match=key):
        get_settings(env_file=None)
