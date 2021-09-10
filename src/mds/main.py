import asyncio

import click
import pkg_resources
from fastapi import FastAPI, APIRouter, HTTPException
import httpx
from urllib.parse import urlparse
from starlette.status import (
    HTTP_500_INTERNAL_SERVER_ERROR,
)

from mds.agg_mds import datastore as aggregate_datastore

try:
    from importlib.metadata import entry_points
except ImportError:
    from importlib_metadata import entry_points

from . import logger, config
from .models import db


def get_app():
    app = FastAPI(
        title="Framework Services Object Management Service",
        version=pkg_resources.get_distribution("mds").version,
        debug=config.DEBUG,
        openapi_prefix=config.URL_PREFIX,
    )
    app.include_router(router)
    db.init_app(app)
    app.add_middleware(ClientDisconnectMiddleware)
    load_modules(app)
    app.async_client = httpx.AsyncClient()

    @app.on_event("shutdown")
    async def shutdown_event():
        if config.USE_AGG_MDS:
            logger.info("Closing aggregate datastore.")
            await aggregate_datastore.close()
        logger.info("Closing async client.")
        await app.async_client.aclose()

    @app.on_event("startup")
    async def startup_event():
        if config.USE_AGG_MDS:
            logger.info("Creating aggregate datastore.")
            url_parts = urlparse(config.ES_ENDPOINT)
            await aggregate_datastore.init(
                hostname=url_parts.hostname, port=url_parts.port
            )

    return app


class ClientDisconnectMiddleware:
    def __init__(self, app):
        self._app = app

    async def __call__(self, scope, receive, send):
        loop = asyncio.get_running_loop()
        rv = loop.create_task(self._app(scope, receive, send))
        waiter = None
        cancelled = False
        if scope["type"] == "http":

            def add_close_watcher():
                nonlocal waiter

                async def wait_closed():
                    nonlocal cancelled
                    while True:
                        message = await receive()
                        if message["type"] == "http.disconnect":
                            if not rv.done():
                                cancelled = True
                                rv.cancel()
                            break

                waiter = loop.create_task(wait_closed())

            scope["add_close_watcher"] = add_close_watcher
        try:
            await rv
        except asyncio.CancelledError:
            if not cancelled:
                raise
        if waiter and not waiter.done():
            waiter.cancel()


def load_modules(app=None):
    logger.info("Start to load modules.")
    # FIXME: Identify the cause for duplicate entry points (PXP-8443)
    # Added a set on entry points to dodge the intermittent duplicate modules issue
    for ep in set(entry_points()["mds.modules"]):
        mod = ep.load()
        if app and hasattr(mod, "init_app"):
            mod.init_app(app)
        msg = "Loaded module: "
        logger.info(
            msg + "%s",
            ep.name,
            extra={"color_message": msg + click.style("%s", fg="cyan")},
        )


router = APIRouter()


@router.get("/version")
def get_version():
    return pkg_resources.get_distribution("mds").version


@router.get("/_status")
async def get_status():
    """
    Returns the status of the MDS:
     * error: if there was no error this will be "none"
     * last_update: timestamp of the last data pull from the commons
     * count: number of entries
    :return:
    """
    now = await db.scalar("SELECT now()")

    if config.USE_AGG_MDS:
        try:
            await aggregate_datastore.get_status()
        except Exception as error:
            logger.error("error with aggregate datastore connection: %s", error)
            raise HTTPException(
                HTTP_500_INTERNAL_SERVER_ERROR,
                {
                    "message": "aggregate datastore offline",
                    "code": HTTP_500_INTERNAL_SERVER_ERROR,
                },
            )

    return dict(
        status="OK", timestamp=now, aggregate_metadata_enabled=config.USE_AGG_MDS
    )
