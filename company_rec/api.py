"""FastAPI endpoint for exact similar-company recommendations."""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from typing import Annotated, Protocol

from fastapi import FastAPI, Query, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict

from company_rec.config import DATABASE_URL
from company_rec.database import (
    CompanyDatabase,
    DatabaseUnavailableError,
    SimilarCompany,
)
from company_rec.events import EventValidationError, normalize_registration_number

logger = logging.getLogger(__name__)


class RecommendationResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    registration_number: str
    name: str
    industry: str | None
    state: str | None
    similarity: float


class SimilarCompaniesResponse(BaseModel):
    registration_number: str
    recommendations: list[RecommendationResponse]


class ErrorDetail(BaseModel):
    code: str
    message: str


class ErrorResponse(BaseModel):
    error: ErrorDetail


class RecommendationStore(Protocol):
    async def get_similar_companies(
        self,
        registration_number: str,
        limit: int,
    ) -> list[SimilarCompany] | None: ...

    async def close(self) -> None: ...


StoreFactory = Callable[[str], Awaitable[RecommendationStore]]


def error_response(status_code: int, code: str, message: str) -> JSONResponse:
    return JSONResponse(
        status_code=status_code,
        content={"error": {"code": code, "message": message}},
    )


def create_app(
    database: RecommendationStore | None = None,
    database_factory: StoreFactory | None = None,
) -> FastAPI:
    """Build the API and own its production database pool through lifespan."""
    if database is not None and database_factory is not None:
        raise ValueError("pass either a database or a database factory, not both")

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        owned_store = None
        if database is None:
            factory = database_factory or CompanyDatabase.connect
            owned_store = await factory(DATABASE_URL)
            store = owned_store
        else:
            store = database
        app.state.database = store
        try:
            yield
        finally:
            if owned_store is not None:
                await owned_store.close()

    app = FastAPI(
        title="Company Recommendations",
        version="1.0.0",
        lifespan=lifespan,
    )
    if database is not None:
        app.state.database = database

    @app.exception_handler(RequestValidationError)
    async def invalid_request_handler(_request: Request, _error: RequestValidationError) -> JSONResponse:
        return error_response(422, "invalid_request", "Invalid request")

    @app.exception_handler(Exception)
    async def internal_error_handler(_request: Request, _error: Exception) -> JSONResponse:
        logger.exception("unexpected recommendation API error")
        return error_response(500, "internal_error", "Internal server error")

    @app.get(
        "/companies/{registration_number}/similar",
        response_model=SimilarCompaniesResponse,
        responses={
            404: {"model": ErrorResponse},
            422: {"model": ErrorResponse},
            500: {"model": ErrorResponse},
            503: {"model": ErrorResponse},
        },
    )
    async def similar_companies(
        request: Request,
        registration_number: str,
        limit: Annotated[int, Query(ge=1, le=4)] = 4,
    ) -> SimilarCompaniesResponse | JSONResponse:
        try:
            normalized_registration_number = normalize_registration_number(registration_number)
        except EventValidationError:
            return error_response(422, "invalid_request", "Invalid request")

        try:
            recommendations = await request.app.state.database.get_similar_companies(
                normalized_registration_number,
                limit,
            )
        except (DatabaseUnavailableError, TimeoutError):
            logger.exception("recommendation database request failed")
            return error_response(503, "database_unavailable", "Database unavailable")

        if recommendations is None:
            return error_response(404, "company_not_found", "Company not found")

        return SimilarCompaniesResponse(
            registration_number=normalized_registration_number,
            recommendations=[RecommendationResponse.model_validate(row) for row in recommendations],
        )

    return app


app = create_app()
