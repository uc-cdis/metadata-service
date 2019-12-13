from gino import Gino
from sqlalchemy.dialects.postgresql import JSONB

db = Gino()


class Metadata(db.Model):
    __tablename__ = "metadata"

    guid = db.Column(db.Unicode(), primary_key=True)
    data = db.Column(JSONB())
