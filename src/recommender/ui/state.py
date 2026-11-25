"""UI transitions generate events once; retries retain immutable payloads."""

from dataclasses import dataclass, field
from datetime import UTC, datetime
from uuid import uuid4

from recommender.schemas import EventCreate
from recommender.ui.client import ApiError


@dataclass
class StoreState:
    user_id: str
    session_id: str = field(default_factory=lambda: uuid4().hex)
    cart_id: str = field(default_factory=lambda: uuid4().hex)
    cart: set[str] = field(default_factory=set)
    selected: str | None = None
    page: int = 0
    pending: dict[str, dict] = field(default_factory=dict)
    failed_events: dict[str, dict] = field(default_factory=dict)

    def change_user(self, user_id: str):
        if user_id != self.user_id:
            self.user_id = user_id
            self.session_id = uuid4().hex
            self.cart_id = uuid4().hex
            self.selected = None
            self.cart.clear()
            self.page = 0

    def _queue(self, product_id: str, event_type: str):
        event = EventCreate(
            event_id=str(uuid4()),
            user_id=self.user_id,
            session_id=self.session_id,
            cart_id=self.cart_id if event_type == "add_to_cart" else None,
            product_id=product_id,
            event_type=event_type,
            event_at=datetime.now(UTC),
            source="demo",
        )
        self.pending[event.event_id] = event.model_dump(mode="json")

    def open_product(self, product_id: str):
        self._queue(product_id, "click")
        if self.selected != product_id:
            self._queue(product_id, "view")
        self.selected = product_id

    def add_to_cart(self):
        if self.selected:
            self._queue(self.selected, "add_to_cart")
            self.cart.add(self.selected)

    def clear_cart(self):
        self.cart.clear()
        self.cart_id = uuid4().hex

    def flush(self, client):
        for event_id, payload in list(self.pending.items()):
            try:
                result = client.request("POST", "/api/v1/events", json=payload)
            except ApiError as exc:
                if exc.retryable:
                    raise
                self.failed_events[event_id] = {
                    "payload": payload,
                    "code": exc.code,
                    "message": str(exc),
                }
                del self.pending[event_id]
                continue
            if result.get("event_id") != event_id:
                raise ValueError("API returned a different event ID")
            del self.pending[event_id]
