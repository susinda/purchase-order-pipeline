import hashlib
import json
import logging
import os
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Annotated, Literal

import pymysql
from fastapi import FastAPI, Header, HTTPException, Request, Response
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
INSERT_EVENT = """
INSERT INTO purchase_order_events (event_id, payload_hash)
VALUES (%s, %s) ON DUPLICATE KEY UPDATE event_id = purchase_order_events.event_id
"""


class PurchaseOrder(BaseModel):
    model_config = ConfigDict(extra="allow")

    OrderNumber: Annotated[str, Field(min_length=1, max_length=255, pattern=r"\S")]


class DeliveryResult(BaseModel):
    order_number: str
    status: Literal["inserted", "updated", "duplicate", "stale"]


def version(order):
    timestamp = datetime.fromisoformat(order["LastUpdated"])
    if timestamp.utcoffset() is None:
        raise ValueError("LastUpdated must have a timezone")
    canonical = json.dumps(order, ensure_ascii=False, allow_nan=False, sort_keys=True, separators=(",", ":"))
    return timestamp, hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def upsert_order(cursor, order_number, snapshot):
    # MySQL reports 1 for inserted, 2 for changed, and 0 for unchanged.
    affected_rows = cursor.execute(UPSERT_ORDER, (order_number, snapshot))
    return {1: "inserted", 2: "updated", 0: "duplicate"}[affected_rows]


def receive_event(connection, order_number, snapshot, event_id, incoming_version):
    connection.begin()
    with connection.cursor() as cursor:
        inserted = cursor.execute(INSERT_EVENT, (event_id, incoming_version[1]))
        if not inserted:
            cursor.execute("SELECT payload_hash FROM purchase_order_events WHERE event_id = %s FOR UPDATE", (event_id,))
            if cursor.fetchone()[0] != incoming_version[1]:
                raise HTTPException(409, "Idempotency-Key is already registered for a different payload")
            result = "duplicate"
        else:
            cursor.execute("SELECT snapshot FROM purchase_orders WHERE order_number = %s FOR UPDATE", (order_number,))
            stored = cursor.fetchone()
            stored_version = None
            if stored:
                try:
                    stored_version = version(json.loads(stored[0]))
                except (KeyError, TypeError, ValueError):
                    # Legacy unkeyed requests accepted snapshots without a version.
                    pass
            if stored_version is not None and incoming_version < stored_version:
                result = "stale"
            else:
                result = upsert_order(cursor, order_number, snapshot)
    connection.commit()
    return result


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
        200: {"description": "Purchase order inserted, updated, duplicate, or stale"},
        401: {"description": "Basic authentication required"},
        409: {"description": "Idempotency key was reused with another payload"},
        500: {"description": "Database operation failed"},
        503: {"description": "Database unavailable"},
    },
)
def post_purchase_order(
    order: PurchaseOrder,
    request: Request,
    event_id: Annotated[str | None, Header(alias="Idempotency-Key", pattern="^[0-9a-f]{64}$")] = None,
) -> DeliveryResult:
    payload = order.model_dump()
    try:
        snapshot = json.dumps(payload, allow_nan=False)
    except ValueError as error:
        raise HTTPException(status_code=422, detail="Order must contain valid JSON values") from error

    if event_id is not None:
        try:
            incoming_version = version(payload)
        except (KeyError, TypeError, ValueError) as error:
            raise HTTPException(422, "A keyed request requires a timezone-aware LastUpdated") from error
        with pymysql.connect(**request.app.state.mysql_options) as connection:
            result = receive_event(connection, order.OrderNumber, snapshot, event_id, incoming_version)
    else:
        with pymysql.connect(**request.app.state.mysql_options) as connection, connection.cursor() as cursor:
            result = upsert_order(cursor, order.OrderNumber, snapshot)
    return DeliveryResult(order_number=order.OrderNumber, status=result)


app.include_router(protected)
