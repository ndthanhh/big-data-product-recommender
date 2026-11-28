"""Điểm khởi chạy FastAPI. Chạy bằng uvicorn recommender.api.main:app."""

from fastapi import FastAPI

from recommender.api.errors import register_handlers
from recommender.api.routes import events, products, recommendations
from recommender.config import get_settings
from recommender.schemas import HealthResponse


def create_app(database_url: str | None = None) -> FastAPI:
    application = FastAPI(title="Product Recommender", version="0.1.0")
    application.state.database_url = database_url or get_settings().database_url
    register_handlers(application)
    application.include_router(products.router)
    application.include_router(events.router)
    application.include_router(recommendations.router)

    @application.get("/health", response_model=HealthResponse)
    def health():
        return {"status": "ok"}

    return application


app = create_app()
