"""Ba ngữ cảnh gợi ý dùng chung snapshot nhất quán của yêu cầu."""

from fastapi import APIRouter, Query, Request

from recommender.api import recommendations as serving
from recommender.api.db import read_snapshot
from recommender.schemas import (
    BusinessId,
    ComplementsRequest,
    PersonalizedQuery,
    ProductId,
    RecommendationResponse,
    SubstitutesQuery,
)

router = APIRouter(prefix="/api/v1/recommendations")


@router.get("/personalized", response_model=RecommendationResponse)
def personalized(
    request: Request,
    user_id: BusinessId,
    limit: int = Query(10, ge=1, le=50),
    session_id: BusinessId | None = None,
):
    query = PersonalizedQuery(user_id=user_id, limit=limit, session_id=session_id)
    with read_snapshot(request.app.state.database_url) as (conn, run):
        return {"run_id": run, "items": serving.personalized(conn, run, query)}


@router.get("/substitutes", response_model=RecommendationResponse)
def substitutes(
    request: Request, product_id: ProductId, limit: int = Query(10, ge=1, le=50)
):
    query = SubstitutesQuery(product_id=product_id, limit=limit)
    with read_snapshot(request.app.state.database_url) as (conn, run):
        return {"run_id": run, "items": serving.substitutes(conn, run, query)}


@router.post("/complements", response_model=RecommendationResponse)
def complements(request: Request, query: ComplementsRequest):
    with read_snapshot(request.app.state.database_url) as (conn, run):
        return {
            "run_id": run,
            "items": serving.complements(
                conn, run, query, getattr(request.app.state, "compatibility", None)
            ),
        }
