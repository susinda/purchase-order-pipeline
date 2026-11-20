import json
import logging
import os
from contextlib import asynccontextmanager
from typing import Annotated, Literal

import pymysql
from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict, Field

from .auth import create_api
from .database import CHECK_SCHEMA, connection_options, initialize_schema


logger = logging.getLogger(__name__)

UPSERT_ORDER = """
INSERT INTO purchase_orders (order_number, snapshot)
VALUES (%s, CAST(%s AS JSON)) AS incoming
ON DUPLICATE KEY UPDATE snapshot = IF(
    purchase_orders.snapshot = incoming.snapshot,
    purchase_orders.snapshot,
    incoming.snapshot
)
"""


class PurchaseOrder(BaseModel):
    model_config = ConfigDict(extra="allow")

    OrderNumber: Annotated[str, Field(min_length=1, max_length=255, pattern=r"\S")]


class DeliveryResult(BaseModel):
    order_number: str
    status: Literal["inserted", "updated", "duplicate"]


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.mysql_options = connection_options()
    await run_in_threadpool(initialize_schema, app.state.mysql_options)
    yield


app, protected = create_api(
    title="Purchase Order Target API",
    realm="purchase-order-target",
    credentials_prefix="TARGET_API",
    lifespan=lifespan,
)


@app.exception_handler(pymysql.MySQLError)
async def mysql_error_handler(request: Request, error: pymysql.MySQLError) -> JSONResponse:
    error_code = error.args[0] if error.args and isinstance(error.args[0], int) else None
    logger.error("MySQL request failed (%s, code %s)", type(error).__name__, error_code)
    if isinstance(error, pymysql.OperationalError):
        return JSONResponse(status_code=503, content={"detail": "Purchase order database unavailable"})
    return JSONResponse(status_code=500, content={"detail": "Purchase order database operation failed"})


@app.get("/health")
def health(request: Request, response: Response) -> dict[str, str]:
    with pymysql.connect(**request.app.state.mysql_options) as connection:
        with connection.cursor() as cursor:
            cursor.execute(CHECK_SCHEMA)
    if image_tag := os.environ.get("APP_IMAGE_TAG"):
        response.headers["X-Image-Tag"] = image_tag
    return {"status": "ok"}


@protected.post(
    "/purchase-orders",
    responses={
        200: {"description": "Purchase order inserted, updated, or already current"},
        401: {"description": "Basic authentication required"},
        500: {"description": "Database operation failed"},
        503: {"description": "Database unavailable"},
    },
)
def post_purchase_order(order: PurchaseOrder, request: Request) -> DeliveryResult:
    try:
        snapshot = json.dumps(order.model_dump(), allow_nan=False)
    except ValueError as error:
        raise HTTPException(status_code=422, detail="Order must contain valid JSON values") from error

    with pymysql.connect(**request.app.state.mysql_options) as connection:
        with connection.cursor() as cursor:
            # MySQL reports 1 for inserted, 2 for changed, and 0 for unchanged.
            affected_rows = cursor.execute(UPSERT_ORDER, (order.OrderNumber, snapshot))

    if affected_rows == 1:
        return DeliveryResult(order_number=order.OrderNumber, status="inserted")
    if affected_rows == 2:
        return DeliveryResult(order_number=order.OrderNumber, status="updated")
    return DeliveryResult(order_number=order.OrderNumber, status="duplicate")


app.include_router(protected)
