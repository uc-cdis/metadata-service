"""metadata

Revision ID: f96cb3b2c523
Revises: 
Create Date: 2019-12-09 16:23:39.943713

"""
import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "f96cb3b2c523"
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "metadata",
        sa.Column("guid", sa.Unicode(), nullable=False),
        sa.Column("data", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.PrimaryKeyConstraint("guid"),
    )


def downgrade():
    op.drop_table("metadata")
