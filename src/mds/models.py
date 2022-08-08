from gino.ext.starlette import Gino
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
    data = db.Column(JSONB())
    authz = db.Column(JSONB(), nullable=False)


"""
async for child in Child.load(parent=Parent).gino.iterate():
    print(f'Parent of {child.id} is {child.parent.id}')
"""


class MetadataAlias(db.Model):
    __tablename__ = "metadata_alias"

    alias = db.Column(db.String(), primary_key=True)
    guid = db.Column(db.Unicode(), db.ForeignKey("metadata.guid"), nullable=False)
