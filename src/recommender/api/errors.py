"""One error envelope for validation, HTTP and database failures."""

import logging

import psycopg
from fastapi import HTTPException
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import ValidationError


def error(status: int, code: str, message: str) -> HTTPException:
    return HTTPException(
        status_code=status, detail={"code": code, "message": message, "fields": []}
    )


def register_handlers(app):
    async def validation_handler(request, exc):
        fields = [
            {"path": ".".join(map(str, item["loc"])), "message": item["msg"]}
            for item in exc.errors()
        ]
        return JSONResponse(
            status_code=422,
            content={
                "detail": {
                    "code": "VALIDATION_ERROR",
                    "message": "Dữ liệu không hợp lệ",
                    "fields": fields,
                }
            },
        )

    app.add_exception_handler(RequestValidationError, validation_handler)
    app.add_exception_handler(ValidationError, validation_handler)

    @app.exception_handler(psycopg.Error)
    async def database_handler(request, exc):
        logging.getLogger(__name__).error(
            "Database request failed: %s", type(exc).__name__
        )
        return JSONResponse(
            status_code=503,
            content={
                "detail": {
                    "code": "DATABASE_UNAVAILABLE",
                    "message": "Không thể truy cập hoặc commit database",
                    "fields": [],
                }
            },
        )

    @app.exception_handler(Exception)
    async def internal_handler(request, exc):
        logging.getLogger(__name__).exception("Unhandled API error")
        return JSONResponse(
            status_code=500,
            content={
                "detail": {
                    "code": "INTERNAL_ERROR",
                    "message": "Lỗi nội bộ",
                    "fields": [],
                }
            },
        )
