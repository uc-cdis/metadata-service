"""Add authz column to metadata table

Revision ID: 4d93784a25e5
Revises: f96cb3b2c523
Create Date: 2022-04-19 10:30:03.967205

"""
import json

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from mds.config import DEFAULT_AUTHZ_STR

# revision identifiers, used by Alembic.
revision = "4d93784a25e5"
down_revision = "f96cb3b2c523"
branch_labels = None
depends_on = None


def escape(str):
    # escape single quotes for SQL statement
    return str.replace("'", "''")


def upgrade():
    """Migrate the authz data from the `data` column and remove some metadata fields."""

    authz_key = "_resource_paths"
    default_version = json.loads(DEFAULT_AUTHZ_STR).get("version", 0)
    authz_data = {"version": default_version}
    remove_metadata_keys = ["_uploader_id", "_filename", "_bucket", "_file_extension"]

    # add the new `authz` column (nullable for now)
    op.add_column(
        "metadata", sa.Column("authz", postgresql.JSONB(astext_type=sa.Text()))
    )

    # extract existing PK (guid) and authz data (resource_path) from the metadata column
    connection = op.get_bind()
    offset = 0
    limit = 500
    query = (
        f"SELECT guid, data FROM metadata ORDER BY guid LIMIT {limit} OFFSET {offset}"
    )
    results = connection.execute(query).fetchall()
    while results:
        for r in results:
            guid, data = r[0], r[1]
            # check for any existing authz-resource-paths in the `data` column
            if data is not None and authz_key in data:
                authz_data[authz_key] = data.pop(authz_key)
                # scrub internal fields from metadata
                for metadata_key in remove_metadata_keys:
                    if metadata_key in data.keys():
                        data.pop(metadata_key)
                sql_statement = f"""UPDATE metadata
                                    SET authz='{escape(json.dumps(authz_data))}',
                                    data='{escape(json.dumps(data))}'
                                    WHERE guid='{guid}'"""
            else:
                # default values for authz (["/open"])
                sql_statement = f"UPDATE metadata SET authz='{DEFAULT_AUTHZ_STR}' WHERE guid='{guid}'"
            connection.execute(sql_statement)
        # Grab another batch of rows
        offset += limit
        query = f"SELECT guid, data FROM metadata ORDER BY guid LIMIT {limit} OFFSET {offset} "
        results = connection.execute(query).fetchall()

    # now that there are no null values, make the column non-nullable
    op.alter_column("metadata", "authz", nullable=False)


def downgrade():
    """
    Migrate the non-default authz data back into the `data` column.

    This will remove any default _resource_paths (["/open"]) on the downgrade
    based on the assumption that empty authz data were replaced with defaults
    on the upgrade.

    Any _resource_paths=["/open"] existing before the migration will be
    removed on the downgrade.
    """

    # get the default resource path from config
    authz_key = "_resource_paths"
    default_authz = json.loads(DEFAULT_AUTHZ_STR)
    default_paths = default_authz.get(authz_key, ["/open"])

    connection = op.get_bind()
    offset = 0
    limit = 500
    query = f"SELECT guid, authz, data FROM metadata ORDER BY guid LIMIT {limit} OFFSET {offset}"
    results = connection.execute(query).fetchall()
    while results:
        for r in results:

            guid = r[0]
            authz_data = r[1]
            data = r[2]
            # keep only non-default resource paths
            if authz_data.get(authz_key) != default_paths:
                if data is None:
                    data = {authz_key: authz_data.pop(authz_key)}
                else:
                    data[authz_key] = authz_data.pop(authz_key)
                sql_statement = f"UPDATE metadata SET data='{escape(json.dumps(data))}' WHERE guid='{guid}'"
                connection.execute(sql_statement)

        # Grab another batch of rows
        offset += limit
        query = f"SELECT guid, authz, data FROM metadata ORDER BY guid LIMIT {limit} OFFSET {offset}"
        results = connection.execute(query).fetchall()

    # drop the `authz` column
    op.drop_column("metadata", "authz")
