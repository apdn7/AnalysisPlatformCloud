"""remove job lock table

Revision ID: 5829a545afca
Revises: ded359fd094e
Create Date: 2024-03-25 09:31:38.124932

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = '5829a545afca'
down_revision = 'ded359fd094e'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_table('t_job_lock')
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table(
        't_job_lock',
        sa.Column('job_name', sa.TEXT(), autoincrement=False, nullable=False),
        sa.PrimaryKeyConstraint('job_name', name='t_job_lock_pkey'),
    )
    # ### end Alembic commands ###
