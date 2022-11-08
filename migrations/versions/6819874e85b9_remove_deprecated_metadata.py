"""remove deprecated metadata

Revision ID: 6819874e85b9
Revises: 3354f2c466ec
Create Date: 2022-09-27 13:43:39.827523

"""
import json

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "6819874e85b9"
down_revision = "3354f2c466ec"
branch_labels = None
depends_on = None


def escape(str):
    # escape colon for sa.text() and single quotes for SQL statement
    return str.replace(":", "\\:").replace("'", "''")


def upgrade():
    """Remove deprecated metadata keys."""

    remove_metadata_keys = ["_uploader_id", "_filename", "_bucket", "_file_extension"]

    # extract existing PK (guid) and metadata (data) columns
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
            # scrub internal fields from metadata
            if data is not None:
                for metadata_key in remove_metadata_keys:
                    if metadata_key in data.keys():
                        data.pop(metadata_key)
                sql_statement = f"""UPDATE metadata
                                    SET data='{escape(json.dumps(data))}'
                                    WHERE guid='{guid}'"""
                connection.execute(sa.text(sql_statement))
        # Grab another batch of rows
        offset += limit
        query = f"SELECT guid, data FROM metadata ORDER BY guid LIMIT {limit} OFFSET {offset} "
        results = connection.execute(query).fetchall()


def downgrade():
    pass
