"""Add baseid and created_date columns

Revision ID: 1f19cdfc4d64
Revises: 4d93784a25e5
Create Date: 2022-09-16 03:16:36.805415

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "1f19cdfc4d64"
down_revision = "6819874e85b9"  # pragma: allowlist secret
branch_labels = None
depends_on = None


def upgrade():
    """Add nullable baseid and created_date columns to support versioning."""
    op.add_column("metadata", sa.Column("baseid", sa.Unicode()))
    op.add_column("metadata", sa.Column("created_date", sa.DateTime))


def downgrade():
    """Remove baseid and created_date columns."""
    op.drop_column("metadata", "baseid")
    op.drop_column("metadata", "created_date")
