"""Shared Pydantic v2 contracts for Bronze/Silver, Gold and HTTP API v1.

DATA_CONTRACTS.md is authoritative for wire enums and UUIDs. Silver uses the
same Product/Event models with UTC datetime values instead of wire strings.
Record validators do not perform I/O: catalog membership and session/cart
ownership across records must also be checked by ETL and database transactions.
"""

import re
import unicodedata
from collections import defaultdict
from collections.abc import Sequence
from datetime import UTC, datetime, timedelta
from types import MappingProxyType
from typing import Annotated, Literal, Self
from urllib.parse import urlsplit
from uuid import UUID

from pydantic import (
    AfterValidator,
    BaseModel,
    BeforeValidator,
    ConfigDict,
    Field,
    HttpUrl,
    PlainSerializer,
    StringConstraints,
    TypeAdapter,
    field_validator,
    model_validator,
)
from pydantic_core import PydanticCustomError

SCHEMA_VERSION = 1
EVENT_WEIGHTS = MappingProxyType({"view": 1, "click": 2, "add_to_cart": 4})
_UTC_PATTERN = re.compile(
    r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?(?:Z|\+00:00)$"
)
_HTTP_URL = TypeAdapter(HttpUrl)


def _validate_id(value: str) -> str:
    if value != value.strip() or any(
        unicodedata.category(char) == "Cc" for char in value
    ):
        raise ValueError("IDs must not contain controls or surrounding whitespace")
    return value


BusinessId = Annotated[
    str, StringConstraints(min_length=1, max_length=128), AfterValidator(_validate_id)
]
PathId = Annotated[
    str, StringConstraints(pattern=r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
]


def _validate_product_id(value: str) -> str:
    source, separator, raw_id = value.partition(":")
    if (
        not separator
        or not source
        or not raw_id
        or len(source) > 64
        or any(char.isspace() for char in value)
    ):
        raise ValueError("product_id must be <source>:<raw_id> without whitespace")
    return value


ProductId = Annotated[BusinessId, AfterValidator(_validate_product_id)]


def _parse_utc(value: object) -> datetime:
    if isinstance(value, str):
        if not _UTC_PATTERN.fullmatch(value):
            raise ValueError("timestamps require ISO 8601 UTC with Z or +00:00")
        value = datetime.fromisoformat(value)
    if not isinstance(value, datetime):
        raise PydanticCustomError(
            "utc_timestamp",
            "timestamps must be UTC datetime values or ISO 8601 strings",
        )
    if value.tzinfo is None or value.utcoffset() != timedelta(0):
        raise ValueError("timestamps must have an explicit UTC timezone")
    return value.astimezone(UTC)


UtcTimestamp = Annotated[
    datetime,
    BeforeValidator(_parse_utc),
    PlainSerializer(
        lambda value: value.isoformat().replace("+00:00", "Z"),
        return_type=str,
        when_used="json",
    ),
]


def _normalize_uuid(value: object) -> str:
    if not isinstance(value, (str, UUID)):
        raise PydanticCustomError("uuid", "event_id must be a UUID")
    try:
        return str(UUID(str(value)))
    except ValueError as exc:
        raise ValueError("event_id must be a UUID") from exc


EventId = Annotated[str, BeforeValidator(_normalize_uuid)]
EventType = Literal["view", "click", "add_to_cart"]
EventSource = Literal["synthetic", "demo"]
RecommendationKind = Literal["personalized", "substitute", "popular"]
BatchMethod = Literal["popularity", "tfidf", "als", "als_mmr"]
ApiMethod = Literal["popularity", "tfidf", "als", "als_mmr", "session", "fpgrowth"]
FiniteNumber = Annotated[float, Field(allow_inf_nan=False)]
PositiveNumber = Annotated[FiniteNumber, Field(gt=0)]
Probability = Annotated[FiniteNumber, Field(gt=0, le=1)]
Rank = Annotated[int, Field(ge=1, le=2147483647)]
RecommendationLimit = Annotated[int, Field(ge=1, le=50)]


def _validate_url(value: str) -> str:
    if any(char.isspace() or unicodedata.category(char) == "Cc" for char in value):
        raise ValueError("URLs must not contain whitespace or controls")
    parts = urlsplit(value)
    if parts.scheme not in {"http", "https"} or not parts.netloc:
        raise ValueError("URLs must be absolute HTTP/HTTPS URLs")
    _HTTP_URL.validate_python(value)
    return value


WebUrl = Annotated[
    str, StringConstraints(max_length=2048), AfterValidator(_validate_url)
]


class ContractModel(BaseModel):
    """Forbid unknown fields, coercion of JSON primitives and non-finite numbers."""

    model_config = ConfigDict(
        extra="forbid", strict=True, validate_default=True, allow_inf_nan=False
    )


class Product(ContractModel):
    """One version of Bronze/Silver product metadata; also the API product body."""

    product_id: ProductId
    name: Annotated[str, StringConstraints(min_length=1, max_length=512)]
    category_id: BusinessId
    source: Annotated[BusinessId, StringConstraints(max_length=64)]
    collected_at: UtcTimestamp
    description: Annotated[str, StringConstraints(max_length=20000)] | None = None
    brand: Annotated[str, StringConstraints(max_length=256)] | None = None
    image_url: WebUrl | None = None
    product_url: WebUrl | None = None
    attributes: dict[Annotated[str, StringConstraints(min_length=1)], str] = Field(
        default_factory=dict
    )

    @field_validator("description", "brand", mode="before")
    @classmethod
    def normalize_blank_text(cls, value: object) -> object:
        """Represent optional whitespace-only descriptions and brands as null."""
        return None if isinstance(value, str) and not value.strip() else value

    @field_validator("name")
    @classmethod
    def validate_name(cls, value: str) -> str:
        """Require at least one visible character without silently trimming text."""
        if not value.strip():
            raise ValueError("name must not be blank")
        return value

    @field_validator("category_id")
    @classmethod
    def validate_category(cls, value: str) -> str:
        """Keep category IDs lowercase as required by CONVENTIONS.md."""
        if value != value.lower():
            raise ValueError("category_id must be lowercase")
        return value

    @model_validator(mode="after")
    def validate_source(self) -> Self:
        """Require the catalog source to match the product ID prefix."""
        if ":" in self.source or self.product_id.partition(":")[0] != self.source:
            raise ValueError("source must match the product_id prefix")
        return self


class Event(ContractModel):
    """The eight immutable business fields of a Bronze/Silver event."""

    event_id: EventId
    user_id: BusinessId
    session_id: BusinessId
    product_id: ProductId
    event_type: EventType
    event_at: UtcTimestamp
    source: EventSource
    cart_id: BusinessId | None = None

    @model_validator(mode="after")
    def require_cart(self) -> Self:
        """An add_to_cart operation must identify its cart."""
        if self.event_type == "add_to_cart" and self.cart_id is None:
            raise ValueError("add_to_cart requires cart_id")
        return self


class GoldProduct(Product):
    """Catalog version selected for a particular batch run."""

    run_id: PathId


class Interaction(ContractModel):
    """A deduplicated weighted user/product aggregate for the training window."""

    run_id: PathId
    user_id: BusinessId
    product_id: ProductId
    interaction_weight: PositiveNumber
    last_event_at: UtcTimestamp


def _validate_sorted_products(value: list[str]) -> list[str]:
    if value != sorted(set(value)):
        raise ValueError("product IDs must be unique and sorted ascending")
    return value


SortedProducts = Annotated[
    list[ProductId], Field(min_length=1), AfterValidator(_validate_sorted_products)
]


class Basket(ContractModel):
    """One user's cart, aggregated exclusively from add_to_cart events."""

    run_id: PathId
    cart_id: BusinessId
    user_id: BusinessId
    session_id: BusinessId
    product_ids: SortedProducts
    first_event_at: UtcTimestamp
    last_event_at: UtcTimestamp

    @model_validator(mode="after")
    def validate_time_range(self) -> Self:
        """A basket cannot end before its first event."""
        if self.first_event_at > self.last_event_at:
            raise ValueError("first_event_at must not exceed last_event_at")
        return self


class SeenProduct(ContractModel):
    """Seen membership scoped to the same catalog and training run."""

    run_id: PathId
    user_id: BusinessId
    product_id: ProductId


class Recommendation(ContractModel):
    """A recommendation item in an API response."""

    product_id: ProductId
    rank: Rank
    score: FiniteNumber
    method: ApiMethod

    @model_validator(mode="after")
    def validate_score(self) -> Self:
        """Scores follow their method's domain; ALS and MMR may be negative."""
        if self.method == "popularity" and self.score <= 0:
            raise ValueError("popularity scores must be positive")
        if self.method in {"tfidf", "session"} and not 0 <= self.score <= 1:
            raise ValueError("tfidf/session scores must lie in [0, 1]")
        if self.method == "fpgrowth" and not 0 < self.score <= 1:
            raise ValueError("fpgrowth scores must lie in (0, 1]")
        return self


class RecommendationItem(Recommendation):
    """A row of Gold recommendation_items (batch algorithms only)."""

    run_id: PathId
    kind: RecommendationKind
    context_id: BusinessId
    method: BatchMethod

    @model_validator(mode="after")
    def validate_context(self) -> Self:
        """Validate the context and algorithm permitted for each batch kind."""
        if self.kind == "popular":
            if self.context_id != "global" or self.method != "popularity":
                raise ValueError(
                    "popular requires global context and popularity method"
                )
        elif self.kind == "substitute":
            _validate_product_id(self.context_id)
            if self.context_id == self.product_id or self.method != "tfidf":
                raise ValueError("substitute requires tfidf and a different product")
        return self


def validate_recommendation_items(items: Sequence[RecommendationItem]) -> None:
    """Check Gold row keys and ranks independently of physical file ordering.

    Catalog foreign keys, category membership and seen filtering additionally
    require the run's catalog/history and are checked by the importer in T08.
    """
    groups: dict[tuple[str, str, str], list[RecommendationItem]] = defaultdict(list)
    for item in items:
        groups[(item.run_id, item.kind, item.context_id)].append(item)
    for group in groups.values():
        if len({item.product_id for item in group}) != len(group):
            raise ValueError("duplicate recommendation product key")
        if len(group) > 200 or sorted(item.rank for item in group) != list(
            range(1, len(group) + 1)
        ):
            raise ValueError("Gold ranks must be consecutive from 1, at most 200 rows")


class AssociationRule(ContractModel):
    """An FP-Growth rule with a single consequent and canonical antecedent set."""

    run_id: PathId
    antecedent_ids: SortedProducts
    consequent_id: ProductId
    support: Probability
    confidence: Probability
    lift: PositiveNumber

    @model_validator(mode="after")
    def validate_consequent(self) -> Self:
        """The consequent cannot also belong to the antecedent."""
        if self.consequent_id in self.antecedent_ids:
            raise ValueError("consequent_id must not belong to antecedent_ids")
        return self


class ProductsQuery(ContractModel):
    """Validated pagination parameters for the product catalog."""

    limit: Annotated[int, Field(ge=1, le=100)] = 20
    offset: Annotated[int, Field(ge=0)] = 0


class PersonalizedQuery(ContractModel):
    """Validated parameters for personalized recommendations."""

    user_id: BusinessId
    limit: RecommendationLimit = 10
    session_id: BusinessId | None = None


class SubstitutesQuery(ContractModel):
    """Validated parameters for same-category substitutes."""

    product_id: ProductId
    limit: RecommendationLimit = 10


class ComplementsRequest(ContractModel):
    """A cart of at most 100 input IDs, deduplicated after validating its size."""

    product_ids: Annotated[list[ProductId], Field(max_length=100)]
    limit: RecommendationLimit = 10

    @field_validator("product_ids")
    @classmethod
    def deduplicate_products(cls, value: list[str]) -> list[str]:
        """Treat the cart as a set with deterministic iteration order."""
        return sorted(set(value))


class EventCreate(Event):
    """The event ingestion endpoint accepts demo events only."""

    source: Literal["demo"]


class HealthResponse(ContractModel):
    """Process liveness only, independent of database or snapshot availability."""

    status: Literal["ok"] = "ok"


class ProductsResponse(ContractModel):
    """A catalog page, including an empty items array past the last page."""

    items: list[Product]


class EventResponse(ContractModel):
    """Both a new event and an equivalent retry return the same UUID body."""

    event_id: EventId


class RecommendationResponse(ContractModel):
    """Recommendations reranked consecutively after request-specific filtering."""

    run_id: PathId
    items: list[Recommendation]

    @model_validator(mode="after")
    def validate_items(self) -> Self:
        """The response is ordered by rank and cannot contain duplicate products."""
        if [item.rank for item in self.items] != list(range(1, len(self.items) + 1)):
            raise ValueError("response ranks must be consecutive from 1 in order")
        if len({item.product_id for item in self.items}) != len(self.items):
            raise ValueError("response product IDs must be unique")
        return self


ErrorCode = Literal[
    "PRODUCT_NOT_FOUND",
    "EVENT_ID_CONFLICT",
    "IDENTITY_CONFLICT",
    "VALIDATION_ERROR",
    "SNAPSHOT_UNAVAILABLE",
    "DATABASE_UNAVAILABLE",
    "INTERNAL_ERROR",
]


class ErrorField(ContractModel):
    """A validation error at a particular request field."""

    path: str
    message: str


class ErrorDetail(ContractModel):
    """The standard v1 error detail; fields is always an array."""

    code: ErrorCode
    message: str
    fields: list[ErrorField] = Field(default_factory=list)


class ErrorResponse(ContractModel):
    """The error envelope shared by all HTTP error responses."""

    detail: ErrorDetail
