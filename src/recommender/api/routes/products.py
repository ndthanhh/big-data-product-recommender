"""Read catalog metadata under a single database snapshot."""

from fastapi import APIRouter, Query, Request
from psycopg.rows import dict_row

from recommender.api.db import read_snapshot
from recommender.api.errors import error
from recommender.schemas import Product, ProductId, ProductsResponse

router = APIRouter(prefix="/api/v1/products")


def product_rows(conn, run):
    with conn.cursor(row_factory=dict_row) as cur:
        return {
            row["product_id"]: Product.model_validate(
                {k: v for k, v in row.items() if k != "run_id"}
            )
            for row in cur.execute(
                'SELECT * FROM products WHERE run_id=%s ORDER BY product_id COLLATE "C"',
                (run,),
            )
        }


@router.get("", response_model=ProductsResponse)
def list_products(
    request: Request, limit: int = Query(20, ge=1, le=100), offset: int = Query(0, ge=0)
):
    with read_snapshot(request.app.state.database_url) as (conn, run):
        with conn.cursor(row_factory=dict_row) as cur:
            rows = cur.execute(
                'SELECT * FROM products WHERE run_id=%s ORDER BY product_id COLLATE "C" LIMIT %s OFFSET %s',
                (run, limit, offset),
            ).fetchall()
        return {
            "items": [
                Product.model_validate({k: v for k, v in row.items() if k != "run_id"})
                for row in rows
            ]
        }


@router.get("/{product_id}", response_model=Product)
def get_product(request: Request, product_id: ProductId):
    with read_snapshot(request.app.state.database_url) as (conn, run):
        with conn.cursor(row_factory=dict_row) as cur:
            row = cur.execute(
                "SELECT * FROM products WHERE run_id=%s AND product_id=%s",
                (run, product_id),
            ).fetchone()
        if row is None:
            raise error(404, "PRODUCT_NOT_FOUND", f"Không có sản phẩm {product_id}")
        return {k: v for k, v in row.items() if k != "run_id"}
