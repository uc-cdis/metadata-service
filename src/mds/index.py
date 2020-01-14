import hashlib
import re

from asyncpg import DuplicateTableError, UndefinedObjectError
from fastapi import HTTPException, APIRouter, Depends
from starlette.status import (
    HTTP_201_CREATED,
    HTTP_204_NO_CONTENT,
    HTTP_404_NOT_FOUND,
    HTTP_409_CONFLICT,
)

from .admin_login import admin_required
from .models import db, Metadata

INDEX_REGEXP = re.compile(r"data #>> '{(.+)}'::text")

mod = APIRouter()


@mod.get("/metadata_index")
async def list_metadata_indexes():
    """List all the metadata key paths indexed in the database."""
    oid = await db.scalar(
        db.text(
            """
        SELECT c.oid
        FROM pg_catalog.pg_class c
        LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        WHERE pg_catalog.pg_table_is_visible(c.oid)
        AND c.relname = :table_name AND c.relkind in
        ('r', 'v', 'm', 'f', 'p')
    """
        ).bindparams(table_name=Metadata.__tablename__)
    )
    indexes = await db.all(
        db.text(
            """
              SELECT
                  i.relname as relname,
                  pg_get_expr(ix.indexprs, :table_oid) as indexprs
              FROM
                  pg_class t
                        join pg_index ix on t.oid = ix.indrelid
                        join pg_class i on i.oid = ix.indexrelid
              WHERE
                  t.relkind IN ('r', 'v', 'f', 'm', 'p')
                  and t.oid = :table_oid
                  and ix.indisprimary = 'f'
              ORDER BY
                  t.relname,
                  i.relname
    """
        ).bindparams(table_oid=oid)
    )

    rv = []
    for name, prs in indexes:
        if name.startswith("path_idx_"):
            rv.append(".".join(INDEX_REGEXP.findall(prs)[0].split(",")))
    return rv


@mod.post("/metadata_index/{path}", status_code=HTTP_201_CREATED)
async def create_metadata_indexes(path):
    """Create a database index on the given metadata key path."""
    path = ",".join(path.split(".")).strip()
    name = hashlib.sha256(path.encode()).hexdigest()[:8]
    rv = ".".join(path.split(","))
    try:
        await db.status(
            "CREATE INDEX path_idx_%s ON %s ((data #>> '{%s}'))"
            % (name, Metadata.__tablename__, path)
        )
    except DuplicateTableError:
        raise HTTPException(HTTP_409_CONFLICT, f"Conflict: {rv}")
    return rv


@mod.delete("/metadata_index/{path}", status_code=HTTP_204_NO_CONTENT)
async def drop_metadata_indexes(path):
    """Drop the database index on the given metadata key path."""
    path = ",".join(path.split(".")).strip()
    name = hashlib.sha256(path.encode()).hexdigest()[:8]
    try:
        await db.status(f"DROP INDEX path_idx_{name}")
    except UndefinedObjectError:
        raise HTTPException(
            HTTP_404_NOT_FOUND, f"Not found: {'.'.join(path.split(','))}"
        )


def init_app(app):
    app.include_router(mod, tags=["Index"], dependencies=[Depends(admin_required)])
