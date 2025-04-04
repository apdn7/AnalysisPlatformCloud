"""Add type for m_function

Revision ID: 9a78d918f9ce
Revises: 66a9c979db74
Create Date: 2024-04-16 11:30:12.067602

"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.orm import Session

from ap.setting_module.models import MFunction

# revision identifiers, used by Alembic.
revision = '9a78d918f9ce'
down_revision = '1a675cde28b6'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('m_function', sa.Column('return_type', sa.Text(), nullable=True))
    op.add_column('m_function', sa.Column('x_type', sa.Text(), nullable=True))
    op.add_column('m_function', sa.Column('y_type', sa.Text(), nullable=True))
    conn = op.get_bind()
    m_function_count = conn.execute(sa.text('select count(*) from m_function')).fetchone()[0]
    # only migrate when `m_function` has existing data
    if m_function_count == 0:
        return
    session = Session(bind=conn)
    # delete all existing records
    session.query(MFunction).delete()
    session.commit()
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column('m_function', 'y_type')
    op.drop_column('m_function', 'x_type')
    op.drop_column('m_function', 'return_type')
    # ### end Alembic commands ###
