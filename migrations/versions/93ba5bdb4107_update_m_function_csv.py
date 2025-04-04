"""update m_function.csv

Revision ID: 93ba5bdb4107
Revises: 93ba5bdb4106
Create Date: 2024-10-15 10:10:00.000000

"""
import sqlalchemy as sa
from alembic import op
from sqlalchemy.orm import Session

from ap.setting_module.models import MFunction

# revision identifiers, used by Alembic.
revision = '93ba5bdb4107'
down_revision = '93ba5bdb4106'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
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
    ...
    # ### end Alembic commands ###
