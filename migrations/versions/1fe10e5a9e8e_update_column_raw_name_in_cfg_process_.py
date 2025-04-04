"""Update column raw name in cfg_process_column

Revision ID: 1fe10e5a9e8e
Revises: 93ba5bdb4107
Create Date: 2024-09-20 10:24:42.248123

"""
import pandas as pd
from alembic import op
from sqlalchemy.orm import Session

from ap.setting_module.models import (
    CfgDataTableColumn,
    CfgProcessColumn,
    MappingFactoryMachine,
    MappingProcessData,
    RFactoryMachine,
)

# revision identifiers, used by Alembic.
revision = '1fe10e5a9e8e'
down_revision = '93ba5bdb4107'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    session = Session(bind=op.get_bind())
    results = session.query(MappingProcessData.data_id, MappingProcessData.t_data_name).all()
    df = pd.DataFrame(results, columns=[MappingProcessData.data_id.name, MappingProcessData.t_data_name.name])
    dict_data_id_with_column_raw_name = df.set_index(MappingProcessData.data_id.name)[
        MappingProcessData.t_data_name.name
    ].to_dict()
    for data_id, column_raw_name in dict_data_id_with_column_raw_name.items():
        op.execute(
            f'''
            UPDATE {CfgProcessColumn.__tablename__} SET column_raw_name = '{column_raw_name}'
            WHERE {CfgProcessColumn.id.name} = {data_id};
            ''',
        )

    data_results = (
        session.query(RFactoryMachine.process_id, MappingFactoryMachine.data_table_id)
        .join(RFactoryMachine, RFactoryMachine.id == MappingFactoryMachine.factory_machine_id)
        .distinct(RFactoryMachine.process_id)
        .all()
    )

    for data in data_results:
        process_id = data[0]
        data_table_id = data[1]
        if not process_id:
            continue

        results = (
            session.query(CfgDataTableColumn.column_name, CfgDataTableColumn.data_group_type)
            .filter(CfgDataTableColumn.data_table_id == data_table_id)
            .all()
        )
        df = pd.DataFrame(
            results,
            columns=[CfgDataTableColumn.column_name.name, CfgDataTableColumn.data_group_type.name],
        )
        dict_data_group_type_with_column_name = df.set_index(CfgDataTableColumn.data_group_type.name)[
            CfgDataTableColumn.column_name.name
        ].to_dict()
        for data_group_type, column_name in dict_data_group_type_with_column_name.items():
            op.execute(
                f'''
                UPDATE {CfgProcessColumn.__tablename__} SET column_raw_name = '{column_name}'
                WHERE {CfgProcessColumn.process_id.name} = {process_id}
                AND {CfgProcessColumn.column_type.name} = {data_group_type};
                ''',
            )

    #
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###
