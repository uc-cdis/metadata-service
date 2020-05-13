import asyncio

import click
import pkg_resources
from fastapi import FastAPI, APIRouter

try:
    from importlib.metadata import entry_points
except ImportError:
    from importlib_metadata import entry_points

from . import logger, config
from .models import db


def get_app():
    app = FastAPI(
        title="DCFS Metadata Service",
        version=pkg_resources.get_distribution("mds").version,
        debug=config.DEBUG,
        openapi_prefix=config.URL_PREFIX,
    )
    app.include_router(router)
    db.init_app(app)
    app.add_middleware(ClientDisconnectMiddleware)
    load_modules(app)
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
    for ep in entry_points()["mds.modules"]:
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
    now = await db.scalar("SELECT now()")
    return dict(status="OK", timestamp=now)
