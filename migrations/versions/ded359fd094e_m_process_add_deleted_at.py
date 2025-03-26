"""m_process : add deleted_at

Revision ID: ded359fd094e
Revises: 760205b453af
Create Date: 2024-03-18 11:16:13.766836

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = 'ded359fd094e'
down_revision = '7385ad240a01'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('m_process', sa.Column('deleted_at', sa.TIMESTAMP(), nullable=True))
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column('m_process', 'deleted_at')
    # ### end Alembic commands ###
