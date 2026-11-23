import json
import os
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Literal

from fastapi import FastAPI, Request, Response

from .auth import create_api


LoadType = Literal["initial", "incremental"]
SAMPLE_DATA_DIR = Path(__file__).with_name("sample_data")


def read_extract(load_type: LoadType) -> dict:
    path = SAMPLE_DATA_DIR / f"purchased_order_{load_type}.json"
    payload = json.loads(path.read_text(encoding="utf-8"))

    if not isinstance(payload, dict) or payload.get("load_type") != load_type:
        raise ValueError(f"{path.name} must contain a {load_type} extract")

    orders = payload.get("purchased_orders")
    count = payload.get("record_count")
    if not isinstance(orders, list) or type(count) is not int or count != len(orders):
        raise ValueError(f"{path.name} record_count must match the purchased_orders array")

    return payload


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.extracts = {
        "initial": read_extract("initial"),
        "incremental": read_extract("incremental"),
    }
    yield


app, protected = create_api(
    title="Purchase Order Source API",
    realm="purchase-order-source",
    credentials_prefix="SOURCE_API",
    lifespan=lifespan,
)


@app.get("/health")
def health(response: Response) -> dict[str, str]:
    if image_tag := os.environ.get("APP_IMAGE_TAG"):
        response.headers["X-Image-Tag"] = image_tag
    return {"message": "Source API OK "}


@protected.get("/purchase-orders", responses={401: {"description": "Basic authentication required"}})
def get_purchase_orders(request: Request, load_type: LoadType) -> dict:
    return request.app.state.extracts[load_type]


app.include_router(protected)
