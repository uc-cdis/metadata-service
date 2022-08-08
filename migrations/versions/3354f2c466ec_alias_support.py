"""alias support

Revision ID: 3354f2c466ec
Revises: 4d93784a25e5
Create Date: 2022-08-04 14:53:41.476049

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


# revision identifiers, used by Alembic.
revision = "3354f2c466ec"
down_revision = "4d93784a25e5"  # pragma: allowlist secret
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "metadata_alias",
        sa.Column("alias", sa.String(), primary_key=True),
        sa.Column("guid", sa.Unicode(), sa.ForeignKey("metadata.guid"), nullable=False),
    )


def downgrade():
    op.drop_table("metadata_alias")
