from gino.ext.starlette import Gino
import datetime
from sqlalchemy import DateTime
from sqlalchemy.dialects.postgresql import JSONB

from . import config

db = Gino(
    dsn=config.DB_DSN,
    pool_min_size=config.DB_POOL_MIN_SIZE,
    pool_max_size=config.DB_POOL_MAX_SIZE,
    echo=config.DB_ECHO,
    ssl=config.DB_SSL,
    use_connection_for_request=config.DB_USE_CONNECTION_FOR_REQUEST,
    retry_limit=config.DB_RETRY_LIMIT,
    retry_interval=config.DB_RETRY_INTERVAL,
)


class Metadata(db.Model):
    __tablename__ = "metadata"

    guid = db.Column(db.Unicode(), primary_key=True)
    baseid = db.Column(db.Unicode())
    data = db.Column(JSONB())
    # Note: default function needs to be defined as lambda in order to support function mocking for unit testing
    created_date = db.Column(DateTime, default=lambda: datetime.datetime.utcnow())
    authz = db.Column(JSONB(), nullable=False)


class MetadataAlias(db.Model):
    __tablename__ = "metadata_alias"

    alias = db.Column(db.String(), primary_key=True)
    guid = db.Column(
        db.Unicode(), db.ForeignKey("metadata.guid", ondelete="CASCADE"), nullable=False
    )
