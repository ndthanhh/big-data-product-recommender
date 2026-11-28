"""Hợp đồng Pydantic v2 dùng chung cho Bronze/Silver, Gold và HTTP API v1.

DATA_CONTRACTS.md quy định chính thức enum và UUID trong dữ liệu trao đổi.
Silver dùng cùng mô hình Product/Event, với datetime UTC thay cho chuỗi.
Bộ kiểm tra bản ghi không thực hiện I/O: ETL và giao dịch cơ sở dữ liệu
còn phải kiểm tra sản phẩm có trong danh mục và quyền sở hữu phiên/giỏ
xuyên suốt các bản ghi.
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
    """Cấm trường lạ, tự ép kiểu dữ liệu JSON cơ bản và số không hữu hạn."""

    model_config = ConfigDict(
        extra="forbid", strict=True, validate_default=True, allow_inf_nan=False
    )


class UserProfile(ContractModel):
    """Nhân khẩu học Việt Nam mô phỏng; mã sở thích tham chiếu hệ phân loại."""

    user_id: BusinessId
    name: Annotated[str, StringConstraints(min_length=1, max_length=128)]
    gender: Literal["male", "female", "other"]
    age: Annotated[int, Field(ge=13, le=100)]
    city: str
    primary_interest: BusinessId
    secondary_interest: BusinessId | None = None
    created_at: UtcTimestamp


class Product(ContractModel):
    """Một phiên bản thông tin sản phẩm Bronze/Silver, cũng là nội dung sản phẩm trong API."""

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
        """Đưa mô tả và thương hiệu tùy chọn chỉ chứa khoảng trắng về null."""
        return None if isinstance(value, str) and not value.strip() else value

    @field_validator("name")
    @classmethod
    def validate_name(cls, value: str) -> str:
        """Yêu cầu ít nhất một ký tự nhìn thấy, không âm thầm cắt khoảng trắng."""
        if not value.strip():
            raise ValueError("name must not be blank")
        return value

    @field_validator("category_id")
    @classmethod
    def validate_category(cls, value: str) -> str:
        """Giữ mã nhóm sản phẩm ở dạng chữ thường theo CONVENTIONS.md."""
        if value != value.lower():
            raise ValueError("category_id must be lowercase")
        return value

    @model_validator(mode="after")
    def validate_source(self) -> Self:
        """Yêu cầu nguồn danh mục khớp tiền tố ID sản phẩm."""
        if ":" in self.source or self.product_id.partition(":")[0] != self.source:
            raise ValueError("source must match the product_id prefix")
        return self


class Event(ContractModel):
    """Tám trường nghiệp vụ bất biến của một sự kiện Bronze/Silver."""

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
        """Thao tác add_to_cart phải xác định giỏ hàng."""
        if self.event_type == "add_to_cart" and self.cart_id is None:
            raise ValueError("add_to_cart requires cart_id")
        return self


class GoldProduct(Product):
    """Phiên bản danh mục được chọn cho một lần chạy batch cụ thể."""

    run_id: PathId


class Interaction(ContractModel):
    """Tổng hợp có trọng số theo người dùng/sản phẩm trong cửa sổ huấn luyện, đã loại trùng."""

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
    """Giỏ của một người dùng, chỉ tổng hợp từ sự kiện add_to_cart."""

    run_id: PathId
    cart_id: BusinessId
    user_id: BusinessId
    session_id: BusinessId
    product_ids: SortedProducts
    first_event_at: UtcTimestamp
    last_event_at: UtcTimestamp

    @model_validator(mode="after")
    def validate_time_range(self) -> Self:
        """Giỏ hàng không thể kết thúc trước sự kiện đầu tiên của nó."""
        if self.first_event_at > self.last_event_at:
            raise ValueError("first_event_at must not exceed last_event_at")
        return self


class SeenProduct(ContractModel):
    """Sản phẩm đã xem được xác định trong cùng danh mục và lần huấn luyện."""

    run_id: PathId
    user_id: BusinessId
    product_id: ProductId


class Recommendation(ContractModel):
    """Một sản phẩm gợi ý trong phản hồi API."""

    product_id: ProductId
    rank: Rank
    score: FiniteNumber
    method: ApiMethod

    @model_validator(mode="after")
    def validate_score(self) -> Self:
        """Điểm tuân theo miền giá trị của phương pháp; điểm ALS và MMR có thể âm."""
        if self.method == "popularity" and self.score <= 0:
            raise ValueError("popularity scores must be positive")
        if self.method in {"tfidf", "session"} and not 0 <= self.score <= 1:
            raise ValueError("tfidf/session scores must lie in [0, 1]")
        if self.method == "fpgrowth" and not 0 < self.score <= 1:
            raise ValueError("fpgrowth scores must lie in (0, 1]")
        return self


class RecommendationItem(Recommendation):
    """Một dòng Gold recommendation_items, chỉ dùng thuật toán batch."""

    run_id: PathId
    kind: RecommendationKind
    context_id: BusinessId
    method: BatchMethod

    @model_validator(mode="after")
    def validate_context(self) -> Self:
        """Kiểm tra ngữ cảnh và thuật toán được phép cho từng loại gợi ý batch."""
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
    """Kiểm tra khóa và thứ hạng các dòng Gold, không phụ thuộc thứ tự vật lý trong file.

    Khóa ngoại danh mục, nhóm sản phẩm và lọc lịch sử đã xem còn cần danh mục
    và lịch sử của lần chạy, được bộ nhập dữ liệu kiểm tra trong T08.
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
    """Luật FP-Growth có một sản phẩm vế phải và tập vế trái ở dạng chuẩn."""

    run_id: PathId
    antecedent_ids: SortedProducts
    consequent_id: ProductId
    support: Probability
    confidence: Probability
    lift: PositiveNumber

    @model_validator(mode="after")
    def validate_consequent(self) -> Self:
        """Sản phẩm vế phải không được đồng thời thuộc vế trái."""
        if self.consequent_id in self.antecedent_ids:
            raise ValueError("consequent_id must not belong to antecedent_ids")
        return self


class ProductsQuery(ContractModel):
    """Tham số phân trang danh mục sản phẩm đã được kiểm tra."""

    limit: Annotated[int, Field(ge=1, le=100)] = 20
    offset: Annotated[int, Field(ge=0)] = 0


class PersonalizedQuery(ContractModel):
    """Tham số gợi ý cá nhân hóa đã được kiểm tra."""

    user_id: BusinessId
    limit: RecommendationLimit = 10
    session_id: BusinessId | None = None


class SubstitutesQuery(ContractModel):
    """Tham số gợi ý sản phẩm thay thế cùng nhóm đã được kiểm tra."""

    product_id: ProductId
    limit: RecommendationLimit = 10


class ComplementsRequest(ContractModel):
    """Giỏ tối đa 100 ID đầu vào; loại trùng sau khi kiểm tra kích thước."""

    product_ids: Annotated[list[ProductId], Field(max_length=100)]
    limit: RecommendationLimit = 10

    @field_validator("product_ids")
    @classmethod
    def deduplicate_products(cls, value: list[str]) -> list[str]:
        """Coi giỏ hàng là tập hợp có thứ tự duyệt xác định."""
        return sorted(set(value))


class EventCreate(Event):
    """Điểm tiếp nhận sự kiện chỉ chấp nhận sự kiện demo."""

    source: Literal["demo"]


class HealthResponse(ContractModel):
    """Chỉ kiểm tra tiến trình còn hoạt động, độc lập với cơ sở dữ liệu và snapshot."""

    status: Literal["ok"] = "ok"


class ProductsResponse(ContractModel):
    """Một trang danh mục; mảng items rỗng khi vượt quá trang cuối."""

    items: list[Product]


class EventResponse(ContractModel):
    """Sự kiện mới và yêu cầu gửi lại tương đương đều trả cùng nội dung UUID."""

    event_id: EventId


class RecommendationResponse(ContractModel):
    """Gợi ý được đánh lại thứ hạng liên tiếp sau bước lọc riêng của yêu cầu."""

    run_id: PathId
    items: list[Recommendation]

    @model_validator(mode="after")
    def validate_items(self) -> Self:
        """Phản hồi được sắp theo thứ hạng và không chứa sản phẩm trùng."""
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
    """Lỗi kiểm tra dữ liệu tại một trường cụ thể của yêu cầu."""

    path: str
    message: str


class ErrorDetail(ContractModel):
    """Chi tiết lỗi chuẩn v1; fields luôn là mảng."""

    code: ErrorCode
    message: str
    fields: list[ErrorField] = Field(default_factory=list)


class ErrorResponse(ContractModel):
    """Cấu trúc lỗi dùng chung cho mọi phản hồi HTTP lỗi."""

    detail: ErrorDetail
