"""New function type

Revision ID: d026d5370dc2
Revises: c491aa6bbe8d
Create Date: 2024-04-12 11:09:43.979704

"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.orm import Session

from ap.setting_module.models import CfgProcessFunctionColumn, MFunction

# revision identifiers, used by Alembic.
revision = 'd026d5370dc2'
down_revision = 'c491aa6bbe8d'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###

    op.add_column('m_function', sa.Column('description_en', sa.Text(), nullable=True))
    op.add_column('m_function', sa.Column('description_jp', sa.Text(), nullable=True))
    conn = op.get_bind()
    m_config_equation_ids = [x[0] for x in conn.execute(sa.text('select id from m_config_equation')).fetchall()]
    for m_config_equation_id in m_config_equation_ids:
        m_data_id = conn.execute(
            sa.text(f'select id from m_data where config_equation_id = {m_config_equation_id}'),
        ).fetchone()[0]
        conn.execute(sa.text(f'update m_config_equation set id = {m_data_id} where id={m_config_equation_id}'))

    m_function_count = conn.execute(sa.text('select count(*) from m_function')).fetchone()[0]
    # only migrate when `m_function` has existing data
    if m_function_count == 0:
        return
    session = Session(bind=conn)
    # delete all existing records
    session.query(MFunction).delete()
    session.commit()

    # update id in m_config_equation
    conn.execute(sa.text('update m_config_equation set eq_type_id=30 where eq_type_id=2'))
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    old_data = [
        'aX + b',
        'sub(X, n, k)',
        'aX - bY',
        'aX + bY + cZ',
        'aX * Y',
        'aX / (bY)',
        'aX * 10 ^ Y',
        'alog10(X)',
    ]
    session = Session(bind=op.get_bind())

    # import old data
    session.query(MFunction).delete()
    for i, data in enumerate(old_data):
        session.add(MFunction(id=i + 1, function_type=data))

    # revert eq_type_id for da-sky02 testing
    for m_config_equation in session.query(CfgProcessFunctionColumn).all():
        # da-sky02 only use sub(X, n, k)
        if m_config_equation.eq_type_id == 30:
            m_config_equation.eq_type_id = 2

    session.commit()

    op.drop_column('m_function', 'description_jp')
    op.drop_column('m_function', 'description_en')

    # ### end Alembic commands ###
