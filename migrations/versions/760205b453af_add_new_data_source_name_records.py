"""Add new data source name records

Revision ID: 760205b453af
Revises: 782a39997ead
Create Date: 2024-03-12 14:49:11.393798

"""
import os

import pandas as pd
import sqlalchemy as sa
from alembic import op
from sqlalchemy.orm import Session

from ap.common.common_utils import gen_bridge_column_name, get_dummy_data_path
from ap.common.constants import EMPTY_STRING, DataGroupType, DataType
from ap.setting_module.models import CfgProcess, CfgProcessColumn, MData, MDataGroup

# revision identifiers, used by Alembic.
revision = '760205b453af'
down_revision = '782a39997ead'
branch_labels = None
depends_on = None


def get_data_source_name_info() -> pd.DataFrame:
    path = os.path.join(get_dummy_data_path(), '16.m_data_group.tsv')
    df = pd.read_csv(path, sep='\t')
    data_source_name = df[df[MDataGroup.data_group_type.name] == DataGroupType.DATA_SOURCE_NAME.value].reset_index(
        drop=True,
    )
    return data_source_name


def upgrade():
    data_source_name = get_data_source_name_info()
    data_group_id = int(data_source_name['id'].values[0])

    # ### commands auto generated by Alembic - please adjust! ###
    session = Session(bind=op.get_bind())

    if (
        session.query(MDataGroup).count() != 0
        and session.query(MDataGroup).filter(MDataGroup.id == data_group_id).first() is None
    ):
        m_data_group = MDataGroup()
        m_data_group.id = data_group_id
        m_data_group.data_name_jp = data_source_name.data_name_jp[0]
        m_data_group.data_name_en = data_source_name.data_name_en[0]
        m_data_group.data_name_sys = data_source_name.data_name_sys[0]
        m_data_group.data_group_type = DataGroupType.DATA_SOURCE_NAME.value
        session.add(m_data_group)

    max_column_id = session.query(sa.func.max(MData.id)).first()[0]
    process_ids = session.query(CfgProcess.id).all()

    for process_id in process_ids:
        has_data_source_name_column = (
            session.query(CfgProcessColumn.id).filter(CfgProcessColumn.process_id == process_id).count() > 0
        )
        if has_data_source_name_column:
            continue

        max_column_id += 1

        total_columns = session.query(CfgProcessColumn.id).filter(CfgProcessColumn.process_id == process_id).count()

        # add to cfg_process_column
        cfg_process_column = CfgProcessColumn()
        cfg_process_column.id = int(max_column_id)
        cfg_process_column.process_id = int(process_id)
        cfg_process_column.column_name = data_source_name[MDataGroup.data_name_en.name].values[0]
        cfg_process_column.data_type = DataType.TEXT.value
        cfg_process_column.column_type = DataGroupType.DATA_SOURCE_NAME.value
        cfg_process_column.is_serial_no = False
        cfg_process_column.is_get_date = False
        cfg_process_column.is_linking_column = True
        cfg_process_column.order = total_columns
        cfg_process_column.bridge_column_name = gen_bridge_column_name(max_column_id, cfg_process_column.column_name)
        cfg_process_column.raw_data_type = DataType.TEXT.value
        cfg_process_column.source_column_name = cfg_process_column.column_name
        cfg_process_column.format = EMPTY_STRING
        cfg_process_column.name_en = data_source_name[MDataGroup.data_name_en.name].values[0]
        cfg_process_column.name_jp = data_source_name[MDataGroup.data_name_jp.name].values[0]
        cfg_process_column.name_local = EMPTY_STRING

        session.add(cfg_process_column)

        # add to m_data
        m_data = MData()
        m_data.id = int(max_column_id)
        m_data.process_id = int(process_id)
        m_data.data_group_id = data_group_id
        m_data.data_type = DataType.TEXT.value
        m_data.unit_id = 1
        m_data.is_hide = False

        session.add(m_data)

    session.commit()
    # ### end Alembic commands ###


def downgrade():
    data_source_name = get_data_source_name_info()
    data_group_id = int(data_source_name['id'].values[0])

    if MDataGroup.query.get(data_group_id) is not None:
        m_data_group = MDataGroup()
        m_data_group.id = data_source_name.id[0]
        m_data_group.data_name_jp = data_source_name.data_name_jp[0]
        m_data_group.data_name_en = data_source_name.data_name_en[0]
        m_data_group.data_name_sys = data_source_name.data_name_sys[0]
        m_data_group.data_group_type = data_source_name.data_group_type[0]

    # ### commands auto generated by Alembic - please adjust! ###
    session = Session(bind=op.get_bind())

    # delete from m_data_group
    session.query(MDataGroup).filter(MDataGroup.data_group_type == DataGroupType.DATA_SOURCE_NAME.value).delete()
    # delete from m_data
    session.query(MData).filter(MData.data_group_id == data_group_id).delete()
    # delete from cfg_process_column
    session.query(CfgProcessColumn).filter(
        CfgProcessColumn.column_type == DataGroupType.DATA_SOURCE_NAME.value,
    ).delete()

    session.commit()
    # ### end Alembic commands ###
