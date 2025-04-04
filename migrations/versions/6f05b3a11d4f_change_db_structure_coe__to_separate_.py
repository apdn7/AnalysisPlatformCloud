"""Change db structure coe_... to separate coef

Revision ID: 6f05b3a11d4f
Revises: 35633b04b8af
Create Date: 2024-05-24 15:50:18.117868

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = '6f05b3a11d4f'
down_revision = '35633b04b8af'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###

    op.add_column('m_function', sa.Column('a', sa.Text(), nullable=True))
    op.add_column('m_function', sa.Column('b', sa.Text(), nullable=True))
    op.add_column('m_function', sa.Column('c', sa.Text(), nullable=True))
    op.add_column('m_function', sa.Column('k', sa.Text(), nullable=True))
    op.add_column('m_function', sa.Column('n', sa.Text(), nullable=True))
    op.add_column('m_function', sa.Column('s', sa.Text(), nullable=True))
    op.add_column('m_function', sa.Column('t', sa.Text(), nullable=True))
    op.drop_column('m_function', 'coe_b_k_t')
    op.drop_column('m_function', 'coe_a_n_s')
    op.drop_column('m_function', 'coe_c')

    # delete all record inside m_function, these records will automatically be inserted when start ap
    conn = op.get_bind()
    sql = 'DELETE FROM m_function;'
    conn.execute(sa.text(sql))

    op.add_column('cfg_process_function_column', sa.Column('a', sa.Text(), nullable=True))
    op.add_column('cfg_process_function_column', sa.Column('b', sa.Text(), nullable=True))
    op.add_column('cfg_process_function_column', sa.Column('c', sa.Text(), nullable=True))
    op.add_column('cfg_process_function_column', sa.Column('k', sa.Text(), nullable=True))
    op.add_column('cfg_process_function_column', sa.Column('n', sa.Text(), nullable=True))
    op.add_column('cfg_process_function_column', sa.Column('s', sa.Text(), nullable=True))
    op.add_column('cfg_process_function_column', sa.Column('t', sa.Text(), nullable=True))

    # separate a,b,c,n,k,s,t
    conn = op.get_bind()
    sql = '''
    update cfg_process_function_column set a = coe_a_n_s;
    update cfg_process_function_column set n = coe_a_n_s;
    update cfg_process_function_column set s = coe_a_n_s;
    update cfg_process_function_column set b = coe_b_k_t;
    update cfg_process_function_column set k = coe_b_k_t;
    update cfg_process_function_column set t = coe_b_k_t;
    update cfg_process_function_column set c = coe_c;
    '''
    conn.execute(sa.text(sql))

    op.drop_column('cfg_process_function_column', 'var_z')
    op.drop_column('cfg_process_function_column', 'coe_a_n_s')
    op.drop_column('cfg_process_function_column', 'coe_b_k_t')
    op.drop_column('cfg_process_function_column', 'coe_c')

    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('m_function', sa.Column('coe_c', sa.TEXT(), autoincrement=False, nullable=True))
    op.add_column('m_function', sa.Column('coe_a_n_s', sa.TEXT(), autoincrement=False, nullable=True))
    op.add_column('m_function', sa.Column('coe_b_k_t', sa.TEXT(), autoincrement=False, nullable=True))
    op.add_column('cfg_process_function_column', sa.Column('coe_c', sa.TEXT(), autoincrement=False, nullable=True))
    op.add_column('cfg_process_function_column', sa.Column('coe_a_n_s', sa.TEXT(), autoincrement=False, nullable=True))
    op.add_column('cfg_process_function_column', sa.Column('var_z', sa.INTEGER(), autoincrement=False, nullable=True))
    op.add_column('cfg_process_function_column', sa.Column('coe_b_k_t', sa.TEXT(), autoincrement=False, nullable=True))

    # separate a,b,c,n,k,s,t
    conn = op.get_bind()
    sql = '''
    update cfg_process_function_column set coe_a_n_s = coalesce(a, n, s);
    update cfg_process_function_column set coe_b_k_t = coalesce(b, k, t);
    update cfg_process_function_column set coe_c = c;
    '''
    conn.execute(sa.text(sql))

    op.drop_column('m_function', 't')
    op.drop_column('m_function', 's')
    op.drop_column('m_function', 'n')
    op.drop_column('m_function', 'k')
    op.drop_column('m_function', 'c')
    op.drop_column('m_function', 'b')
    op.drop_column('m_function', 'a')
    op.drop_column('cfg_process_function_column', 't')
    op.drop_column('cfg_process_function_column', 's')
    op.drop_column('cfg_process_function_column', 'n')
    op.drop_column('cfg_process_function_column', 'k')
    op.drop_column('cfg_process_function_column', 'c')
    op.drop_column('cfg_process_function_column', 'b')
    op.drop_column('cfg_process_function_column', 'a')
    # ### end Alembic commands ###
