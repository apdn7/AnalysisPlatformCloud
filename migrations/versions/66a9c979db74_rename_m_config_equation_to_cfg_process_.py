"""Rename m_config_equation to cfg_process_function_column

Revision ID: 66a9c979db74
Revises: f5a6d1cb4741
Create Date: 2024-04-15 15:50:00.849261

"""
from alembic import op

# revision identifiers, used by Alembic.
revision = '66a9c979db74'
down_revision = 'f5a6d1cb4741'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.rename_table('m_config_equation', 'cfg_process_function_column')
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.rename_table('cfg_process_function_column', 'm_config_equation')
    # ### end Alembic commands ###
