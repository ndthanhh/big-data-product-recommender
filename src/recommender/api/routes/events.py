"""Sự kiện demo bất biến, gửi lại an toàn, có khóa quyền sở hữu trong phạm vi giao dịch."""

from fastapi import APIRouter, Request, Response
from psycopg.rows import dict_row

from recommender.api.db import active_run, connect
from recommender.api.errors import error
from recommender.schemas import EventCreate, EventResponse

router = APIRouter(prefix="/api/v1/events")
EVENT_COLUMNS = (
    "event_id,user_id,session_id,product_id,event_type,event_at,source,cart_id"
)


def _existing(conn, event):
    with conn.cursor(row_factory=dict_row) as cur:
        row = cur.execute(
            f"SELECT {EVENT_COLUMNS} FROM events WHERE event_id=%s", (event.event_id,)
        ).fetchone()
    if row is None:
        return False
    row["event_id"] = str(row["event_id"])
    if EventCreate.model_validate(row) != event:
        raise error(409, "EVENT_ID_CONFLICT", "event_id đã tồn tại với nội dung khác")
    return True


@router.post("", response_model=EventResponse, status_code=201)
def post_event(request: Request, event: EventCreate, response: Response):
    retry = False
    with connect(request.app.state.database_url) as conn:
        conn.execute(
            "SELECT pg_advisory_xact_lock(hashtextextended(%s,0))",
            ("event:" + event.event_id,),
        )
        if _existing(conn, event):
            retry = True
        else:
            run = active_run(conn)
            if not conn.execute(
                "SELECT 1 FROM products WHERE run_id=%s AND product_id=%s",
                (run, event.product_id),
            ).fetchone():
                raise error(
                    404, "PRODUCT_NOT_FOUND", f"Không có sản phẩm {event.product_id}"
                )
            keys = ["session:" + event.session_id]
            if event.cart_id:
                keys.append("cart:" + event.cart_id)
            for key in sorted(keys):
                conn.execute(
                    "SELECT pg_advisory_xact_lock(hashtextextended(%s,0))", (key,)
                )
            if conn.execute(
                "SELECT 1 FROM events WHERE session_id=%s AND user_id<>%s LIMIT 1",
                (event.session_id, event.user_id),
            ).fetchone():
                raise error(409, "IDENTITY_CONFLICT", "Session đã thuộc user khác")
            if (
                event.cart_id
                and conn.execute(
                    "SELECT 1 FROM events WHERE cart_id=%s AND (user_id<>%s OR session_id<>%s) LIMIT 1",
                    (event.cart_id, event.user_id, event.session_id),
                ).fetchone()
            ):
                raise error(
                    409, "IDENTITY_CONFLICT", "Giỏ đã thuộc user hoặc session khác"
                )
            payload = event.model_dump()
            inserted = conn.execute(
                f"INSERT INTO events({EVENT_COLUMNS},catalog_run_id) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT(event_id) DO NOTHING RETURNING event_id",
                tuple(payload[key] for key in EVENT_COLUMNS.split(",")) + (run,),
            ).fetchone()
            if inserted is None:
                retry = _existing(conn, event)
    response.status_code = 200 if retry else 201
    return {"event_id": event.event_id}
