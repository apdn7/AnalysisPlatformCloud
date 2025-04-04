"""update new function m_tsv file 2024-05-08

Revision ID: a446042efa10
Revises: 6b4633b411e8
Create Date: 2024-05-08 14:20:44.135113

"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.orm import Session

from ap.setting_module.models import MFunction

# revision identifiers, used by Alembic.
revision = 'a446042efa10'
down_revision = '6b4633b411e8'
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
