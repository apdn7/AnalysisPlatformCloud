"""Add column note into cfg_process_function_column

Revision ID: 9c8bbf2d1914
Revises: 9a78d918f9ce
Create Date: 2024-04-17 10:27:48.320787

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = '9c8bbf2d1914'
down_revision = '9a78d918f9ce'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('cfg_process_function_column', sa.Column('note', sa.Text(), nullable=True))
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column('cfg_process_function_column', 'note')
    # ### end Alembic commands ###
