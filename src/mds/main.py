import asyncio

from fastapi import FastAPI, APIRouter, HTTPException, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
import httpx
from urllib.parse import urlparse
from contextlib import asynccontextmanager
from starlette.status import (
    HTTP_500_INTERNAL_SERVER_ERROR,
)

from mds.agg_mds import datastore as aggregate_datastore

try:
    from importlib.metadata import entry_points, version
except ImportError:
    from importlib_metadata import entry_points, version

from . import logger, config
from mds.db import initiate_db, get_db_engine_and_sessionmaker, get_session


def get_app():
    async def setup_aggregate_datastore(app):
        if config.USE_AGG_MDS:
            logger.info("Creating aggregate datastore.")
            url_parts = urlparse(config.ES_ENDPOINT)
            await aggregate_datastore.init(
                hostname=url_parts.hostname, port=url_parts.port
            )

    async def close_aggregate_datastore(app):
        if config.USE_AGG_MDS:
            logger.info("Closing aggregate datastore.")
            await aggregate_datastore.close()

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        # Startup actions
        await initiate_db()
        engine, session_maker = get_db_engine_and_sessionmaker()
        app.state.engine = engine
        app.state.async_sessionmaker = session_maker
        app.async_client = httpx.AsyncClient()
        await setup_aggregate_datastore(app)
        yield
        # Shutdown actions
        await close_aggregate_datastore(app)
        logger.info("Closing async client.")
        await app.async_client.aclose()
        await engine.dispose()

    app = FastAPI(
        title="Framework Services Object Management Service",
        version=version("mds"),
        debug=config.DEBUG,
        root_path=config.URL_PREFIX,
        lifespan=lifespan,
    )
    app.include_router(router)
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
    """
    Loop through Poetry [plugins](https://python-poetry.org/docs/master/pyproject/#plugins)
    which behave like setuptools Entry Points, then load() each one

    See also:
        https://setuptools.pypa.io/en/latest/userguide/entry_point.html#entry-points-for-plugins
    """
    logger.info("Start to load modules.")
    # sorted set ensures deterministic loading order
    eps = entry_points(group="mds.modules")
    for ep in eps:
        mod = ep.load()
        if app and hasattr(mod, "init_app"):
            mod.init_app(app)
        msg = "Loaded module: "
        logger.info(msg + "%s", ep.name)


router = APIRouter()


@router.get("/version")
def get_version():
    return version("mds")


@router.get("/_status")
async def get_status(session: AsyncSession = Depends(get_session)):
    """
    Returns the status of the MDS:
     * error: if there was no error this will be "none"
     * last_update: timestamp of the last data pull from the commons
     * count: number of entries
    """
    result = await session.execute(text("SELECT now()"))
    now = result.scalar()

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
