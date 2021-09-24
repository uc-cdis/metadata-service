"""file_objects

Revision ID: 98ca5863b75e
Revises: f96cb3b2c523
Create Date: 2021-09-10 14:04:47.730134

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB


# revision identifiers, used by Alembic.
revision = "98ca5863b75e"
down_revision = "f96cb3b2c523"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "file_objects",
        sa.Column("did", sa.Unicode(), nullable=False),
        sa.Column("baseid", sa.Unicode(), nullable=False),
        sa.Column("rev", sa.Unicode(), nullable=False),
        sa.Column("form", sa.Unicode(), nullable=False),
        sa.Column("size", sa.BigInteger(), nullable=False),
        sa.Column("file_name", sa.Unicode()),
        sa.Column("version", sa.Unicode()),
        sa.Column("uploader", sa.Unicode()),
        sa.Column("created_date", sa.DateTime(), nullable=False),
        sa.Column("updated_date", sa.DateTime(), nullable=False),
        sa.Column("urls", sa.ARRAY(sa.String())),
        sa.Column("urls_metadata", JSONB(astext_type=sa.Text())),
        sa.Column("acl", sa.ARRAY(sa.String())),
        sa.Column("authz", sa.ARRAY(sa.String())),
        sa.Column("hashes", JSONB(astext_type=sa.Text())),
        sa.Column("metadata", JSONB(astext_type=sa.Text())),
        sa.PrimaryKeyConstraint("did"),
    )


def downgrade():
    op.drop_table("file_objects")
