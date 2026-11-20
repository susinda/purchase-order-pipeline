import os
from contextlib import asynccontextmanager
from secrets import compare_digest
from typing import Annotated

from fastapi import APIRouter, Depends, FastAPI, HTTPException, Request
from fastapi.openapi.docs import get_swagger_ui_html
from fastapi.responses import JSONResponse
from fastapi.security import HTTPBasic, HTTPBasicCredentials


def create_api(*, title, realm, credentials_prefix, lifespan):
    basic_auth = HTTPBasic(realm=realm)

    @asynccontextmanager
    async def authenticated_lifespan(app):
        username = os.environ.get(f"{credentials_prefix}_USERNAME")
        password = os.environ.get(f"{credentials_prefix}_PASSWORD")
        if not username or not password:
            raise RuntimeError(f"Set {credentials_prefix}_USERNAME and {credentials_prefix}_PASSWORD")
        if not username.isascii() or not password.isascii() or ":" in username:
            raise RuntimeError("Basic Auth requires ASCII credentials and a username without a colon")
        app.state.basic_credentials = (username.encode("ascii"), password.encode("ascii"))
        async with lifespan(app):
            yield

    def authenticate(
        request: Request,
        credentials: Annotated[HTTPBasicCredentials, Depends(basic_auth)],
    ) -> None:
        username, password = request.app.state.basic_credentials
        username_matches = compare_digest(credentials.username.encode("ascii"), username)
        password_matches = compare_digest(credentials.password.encode("ascii"), password)
        if not (username_matches and password_matches):
            raise HTTPException(
                status_code=401,
                detail="Invalid credentials",
                headers={"WWW-Authenticate": f'Basic realm="{realm}"'},
            )

    app = FastAPI(
        title=title,
        lifespan=authenticated_lifespan,
        docs_url=None,
        redoc_url=None,
        openapi_url=None,
    )
    protected = APIRouter(dependencies=[Depends(authenticate)])

    @protected.get("/openapi.json", include_in_schema=False)
    def openapi_schema():
        return JSONResponse(app.openapi(), headers={"Cache-Control": "no-store"})

    @protected.get("/docs", include_in_schema=False)
    def swagger_docs(request: Request):
        response = get_swagger_ui_html(
            openapi_url=request.url_for("openapi_schema").path,
            title=f"{app.title} - Swagger UI",
        )
        response.headers["Cache-Control"] = "no-store"
        return response

    return app, protected
