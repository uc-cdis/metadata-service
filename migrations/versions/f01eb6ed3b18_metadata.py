"""metadata

Revision ID: f01eb6ed3b18
Revises: f96cb3b2c523
Create Date: 2022-04-19 14:41:33.790539

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.schema import Sequence, CreateSequence, DropSequence


# revision identifiers, used by Alembic.
revision = "f01eb6ed3b18"
down_revision = "f96cb3b2c523"
branch_labels = None
depends_on = None


def upgrade():
    op.execute(CreateSequence(Sequence("metadata_internal_id_seq")))
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table(
        "metadata_internal",
        sa.Column("guid", sa.Unicode(), nullable=False),
        sa.Column(
            "id",
            sa.BigInteger(),
            server_default=sa.text("nextval('metadata_internal_id_seq')"),
            nullable=True,
        ),
        sa.PrimaryKeyConstraint("guid"),
    )
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_table("metadata_internal")
    # ### end Alembic commands ###
    op.execute(DropSequence(Sequence("metadata_internal_id_seq")))