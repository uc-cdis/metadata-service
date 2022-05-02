"""Add authz column to metadata table

Revision ID: 4d93784a25e5
Revises: f96cb3b2c523
Create Date: 2022-04-19 10:30:03.967205

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql
import json


# revision identifiers, used by Alembic.
revision = "4d93784a25e5"
down_revision = "f96cb3b2c523"
branch_labels = None
depends_on = None

DEFAULT_RESOURCE_PATH = "/open"


def upgrade():
    # add the new `authz` column (nullable for now)
    op.add_column(
        "metadata", sa.Column("authz", postgresql.JSONB(astext_type=sa.Text()))
    )

    # extract existing PK (guid) and authz data (resource_path) from the metadata column
    connection = op.get_bind()
    results = connection.execute("SELECT guid, data from metadata").fetchall()

    # create the authz data and update authz column
    for r in results:
        guid, data = r[0], r[1]
        # default values for authz
        authz_data = {"version": 0, "_resource_paths": [DEFAULT_RESOURCE_PATH]}
        # overwrite default
        authz_keys = ["version", "_resource_paths"]
        for authz_key in authz_keys:
            if authz_key in data.keys():
                authz_data[authz_key] = data.pop(authz_key)
        sql_statement = f"UPDATE metadata SET authz='{json.dumps(authz_data)}', data='{json.dumps(data)}' WHERE guid='{guid}'"
        connection.execute(sql_statement)

    # now that there are no null values, make the column non-nullable
    op.alter_column("metadata", "authz", nullable=False)


def downgrade():
    # migrate the authz data back into the `data` column
    connection = op.get_bind()
    results = connection.execute("SELECT guid, authz, data from metadata").fetchall()

    for r in results:
        guid = r[0]
        authz_data = r[1]
        data = r[2]

        # keep only the values that were previously stored in the data column
        authz_key = "_resource_paths"
        if authz_key in authz_data.keys() and authz_data[authz_key] != [
            DEFAULT_RESOURCE_PATH
        ]:
            data[authz_key] = authz_data.pop(authz_key)
            sql_statement = (
                f"UPDATE metadata SET data='{json.dumps(data)}' WHERE guid='{guid}'"
            )
            connection.execute(sql_statement)

    # drop the `authz` column
    op.drop_column("metadata", "authz")
