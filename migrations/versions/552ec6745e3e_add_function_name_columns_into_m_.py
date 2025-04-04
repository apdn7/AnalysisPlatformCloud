"""add-function-name-columns-into-m-function-table

Revision ID: 552ec6745e3e
Revises: 4e1fb8106754
Create Date: 2024-05-22 10:49:36.668254

"""
from io import StringIO

import pandas as pd
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = '552ec6745e3e'
down_revision = '4e1fb8106754'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.alter_column('cfg_process_column', 'column_raw_name', existing_type=sa.TEXT(), nullable=False)
    op.add_column('m_function', sa.Column('function_name_en', sa.Text(), nullable=True))
    op.add_column('m_function', sa.Column('function_name_jp', sa.Text(), nullable=True))

    function_names_data = '''id,function_name_en,function_name_jp
10,Linear function transformation,一次関数変換
11,Linear combination,線形結合
12,Interaction,交互作用
13,Ratio,比率
14,Restoration of mantissa/exponent,仮数/指数の復元
15,Power & exponential transformation,累乗・べき乗変換
16,Logarithmic transformation,対数変換
20,,
21,,
22,,
28,Radius,半径
29,Angle,角度
30,String extraction,文字列抽出
32,Category synthesis,カテゴリ合成
40,Datetime generation,日時生成
41,Datetime synthesis,日時合成
42,Datetime output,日時出力
43,Timezone extraction,時間帯抽出
44,Day of the week extraction,曜日抽出
45,Week number extraction,週番号抽出
46,Day extraction,日抽出
47,Month extraction,月抽出
48,Year extraction,年抽出
50,Logical,論理
51,Logical,論理
55,Logical Or,論理 Or
56,Logical And,論理 And
60,Pattern extraction,パターン抽出
61,Pattern deletion,パターン削除
62,Pattern replacement,パターン置換
70,Merge,マージ
71,Value replacement,値置換
72,Type conversion,型変換
80,Shift,シフト
90,NA replacement,NA置換
120,Value deletion,値削除
121,Within range deletion,範囲内削除
122,Outside range deletion,範囲外削除
130,String extraction,文字列抽出
131,Value deletion,値削除
160,Pattern extraction,パターン抽出
161,Pattern deletion,パターン削除
162,Pattern replacement,パターン置換
170,Merge,マージ
171,Value reference,値参照
172,Type conversion,型変換
190,NA replacement,NA置換
    '''

    df = pd.read_csv(StringIO(function_names_data), dtype='string')
    df.fillna('', inplace=True)
    update_statements = []
    for _, row in df.iterrows():
        function_name_en = f"'{row['function_name_en']}'" if row['function_name_en'] != '' else 'null'
        function_name_jp = f"'{row['function_name_jp']}'" if row['function_name_jp'] != '' else 'null'
        update_statements.append(
            "update m_function"
            f" set function_name_en = {function_name_en}"
            f" , function_name_jp = {function_name_jp}"
            f" where id = {row['id']};",
        )

    conn = op.get_bind()
    conn.execute(sa.text(''.join(update_statements)))
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column('m_function', 'function_name_jp')
    op.drop_column('m_function', 'function_name_en')
    op.alter_column('cfg_process_column', 'column_raw_name', existing_type=sa.TEXT(), nullable=True)
    # ### end Alembic commands ###
