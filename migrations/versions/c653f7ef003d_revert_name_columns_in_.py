"""revert name columns in CfgDataTableColumn

Revision ID: c653f7ef003d
Revises: 8063f4762a43
Create Date: 2023-11-15 11:07:20.395139

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = 'c653f7ef003d'
down_revision = '8063f4762a43'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('cfg_data_table_column', sa.Column('english_name', sa.Text(), nullable=True))
    op.add_column('cfg_data_table_column', sa.Column('name', sa.Text(), nullable=True))
    op.drop_column('cfg_data_table_column', 'name_jp')
    op.drop_column('cfg_data_table_column', 'name_local')
    op.drop_column('cfg_data_table_column', 'name_en')
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('cfg_data_table_column', sa.Column('name_en', sa.TEXT(), autoincrement=False, nullable=True))
    op.add_column('cfg_data_table_column', sa.Column('name_local', sa.TEXT(), autoincrement=False, nullable=True))
    op.add_column('cfg_data_table_column', sa.Column('name_jp', sa.TEXT(), autoincrement=False, nullable=True))
    op.drop_column('cfg_data_table_column', 'name')
    op.drop_column('cfg_data_table_column', 'english_name')
    # ### end Alembic commands ###
