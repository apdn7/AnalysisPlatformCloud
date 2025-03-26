from typing import Iterator, List, Tuple

import numpy as np
from pandas import DataFrame

from ap.common.constants import DataGroupType, RawDataTypeDB
from ap.common.pydn.dblib.db_common import SqlComparisonOperator
from ap.common.pydn.dblib.postgresql import PostgreSQL
from ap.setting_module import models as ap_models
from ap.setting_module.models import insert_or_update_config, make_session
from bridge.models.bridge_station import BridgeStationModel, MasterModel
from bridge.models.model_utils import TableColumn
from bridge.services.sql.utils import run_sql_from_query_with_casted


class MDataGroup(MasterModel):
    class Columns(TableColumn):
        id = (1, RawDataTypeDB.INTEGER)
        data_name_jp = (2, RawDataTypeDB.TEXT)
        data_name_en = (3, RawDataTypeDB.TEXT)
        data_name_sys = (4, RawDataTypeDB.TEXT)
        data_name_local = (5, RawDataTypeDB.TEXT)
        data_abbr_jp = (6, RawDataTypeDB.TEXT)
        data_abbr_en = (7, RawDataTypeDB.TEXT)
        data_abbr_local = (8, RawDataTypeDB.TEXT)
        data_group_type = (9, RawDataTypeDB.SMALL_INT)
        created_at = (97, RawDataTypeDB.DATETIME)
        updated_at = (98, RawDataTypeDB.DATETIME)

    def __init__(self, dict_proc=None):
        dict_proc = dict_proc if dict_proc else {}
        self.id = dict_proc.get(MDataGroup.Columns.id.name)
        self.data_name_jp = dict_proc.get(MDataGroup.Columns.data_name_jp.name)
        self.data_name_en = dict_proc.get(MDataGroup.Columns.data_name_en.name)
        self.data_name_sys = dict_proc.get(MDataGroup.Columns.data_name_sys.name)
        self.data_name_local = dict_proc.get(MDataGroup.Columns.data_name_local.name)
        self.data_abbr_jp = dict_proc.get(MDataGroup.Columns.data_abbr_jp.name)
        self.data_abbr_en = dict_proc.get(MDataGroup.Columns.data_abbr_en.name)
        self.data_abbr_local = dict_proc.get(MDataGroup.Columns.data_abbr_local.name)
        self.data_group_type = dict_proc.get(MDataGroup.Columns.data_group_type.name)

    _table_name = 'm_data_group'
    primary_keys = [Columns.id]
    not_null_columns = [Columns.data_name_jp]

    # message_cls = MsgMPart
    @classmethod
    def get_predefined_data_group(cls, db_instance):
        # dic_conditions = {cls.Columns.data_group_type.name: [(SqlComparisonOperator.LESS_THAN_OR_EQ, 20)]}
        dic_conditions = {
            cls.Columns.data_group_type.name: [(SqlComparisonOperator.LESS_THAN, DataGroupType.GENERATED.value)],
        }
        cols, rows = MDataGroup.select_records(db_instance, dic_conditions, row_is_dict=True)
        if not rows:
            return []
        return [MDataGroup(row) for row in rows]

    @classmethod
    def get_data_group_by_group_type(cls, db_instance, group_type):
        dic_conditions = {cls.Columns.data_group_type.name: group_type}
        cols, rows = MDataGroup.select_records(db_instance, dic_conditions, row_is_dict=True)
        if not rows:
            return []
        return [MDataGroup(row) for row in rows]

    @classmethod
    def get_data_group_in_group_types(cls, db_instance, group_types: List):
        # DO NOT order this this return lst
        dic_conditions = {cls.Columns.data_group_type.name: [(SqlComparisonOperator.IN, tuple(group_types))]}
        cols, rows = MDataGroup.select_records(db_instance, dic_conditions, row_is_dict=True)
        if not rows:
            return []
        return [MDataGroup(row) for row in rows]

    @classmethod
    def get_in_ids(cls, db_instance, ids: [List, Tuple], is_return_dict=False):
        if not ids:
            return {} if is_return_dict else []
        id_col = cls.get_pk_column_names()[0]
        _, rows = cls.select_records(
            db_instance,
            {id_col: [(SqlComparisonOperator.IN, tuple(ids))]},
            filter_deleted=False,
        )
        if not rows:
            return []
        data_groups = [MDataGroup(row) for row in rows]
        if is_return_dict:
            return {data_group.id: data_group for data_group in data_groups}
        return data_groups

    @classmethod
    def get_generated_columns_as_df(cls, db_instance):
        df = cls.get_all_as_df(db_instance)
        df = df[df[cls.Columns.data_group_type.name] == DataGroupType.GENERATED.value]
        return df

    @classmethod
    def get_unique_by_data_table_ids(
        cls,
        *,
        db_instance: PostgreSQL,
        data_table_ids: list[int],
    ) -> Iterator[ap_models.MDataGroup]:
        query = (
            ap_models.MDataGroup.query.join(
                ap_models.MData,
                ap_models.MData.data_group_id == ap_models.MDataGroup.id,
            )
            .join(
                ap_models.MappingProcessData,
                ap_models.MappingProcessData.data_id == ap_models.MData.id,
            )
            .filter(ap_models.MappingProcessData.data_table_id.in_(data_table_ids))
            .order_by(ap_models.MDataGroup.id)
        )

        yield from run_sql_from_query_with_casted(
            query=query,
            db_instance=db_instance,
            cls=ap_models.MDataGroup,
        )

    @classmethod
    def get_by_data_name_sys(cls, db_instance, data_name_sys, is_return_object: bool = False):
        _, rows = cls.select_records(db_instance, {cls.Columns.data_name_sys.name: data_name_sys})

        if is_return_object and rows:
            return cls(rows[0])
        return rows


class PrimaryGroup:
    """
    this name is for developers. Don't need to exact name with data in m_data_group. we base on data_group_type.
    """

    def __init__(self, initial_data):
        for key in initial_data:
            setattr(self, key, initial_data[key])

    DATA_SERIAL = None
    DATA_TIME = None
    DATA_VALUE = None
    EQUIP_ID = None
    EQUIP_NAME = None
    LINE_ID = None
    LINE_NAME = None
    PART_NO = None
    PROCESS_ID = None
    PROCESS_NAME = None
    DATA_ID = None
    DATA_NAME = None
    SERIAL = None
    SUB_PART_NO = None
    SUB_TRAY_NO = None
    SUB_LOT_NO = None
    SUB_SERIAL = None
    FACTORY_ID = None
    FACTORY_NAME = None
    PLANT_ID = None
    DEPT_ID = None
    DEPT_NAME = None
    LINE_GROUP_ID = None
    LINE_GROUP_NAME = None
    PART_NO_FULL = None
    EQUIP_NO = None
    HORIZONTAL_DATA = None
    DATA_TABLE_ID = None
    FORGING_DATE = None
    DELIVERY_ASSY_FASTEN_TORQUE = None

    FACTORY_ABBR = None
    PLANT_NAME = None
    PLANT_ABBR = None
    PROD_FAMILY_ID = None
    PROD_FAMILY_NAME = None
    PROD_FAMILY_ABBR = None
    OUTSOURCE = None
    DEPT_ABBR = None
    SECT_ID = None
    SECT_NAME = None
    SECT_ABBR = None
    PROD_ID = None
    PROD_NAME = None
    PROD_ABBR = None
    PART_TYPE = None
    PART_NAME = None
    PART_ABBR = None
    EQUIP_PRODUCT_NO = None
    EQUIP_PRODUCT_DATE = None
    STATION_NO = None
    PROCESS_ABBR = None
    DATA_ABBR = None
    UNIT = None
    LOCATION_NAME = None
    LOCATION_ABBR = None
    LINE_NO = None


def get_primary_groups_by_config(cfg_columns=None):
    dict_primary_group = {
        DataGroupType(col.m_data_group.data_group_type).name: col for col in cfg_columns if col.m_data_group
    }
    primary_group = PrimaryGroup(dict_primary_group)
    return primary_group


@BridgeStationModel.use_db_instance()
def get_primary_group(db_instance: PostgreSQL = None):
    data_groups = MDataGroup.get_predefined_data_group(db_instance)
    dict_primary_group = {
        DataGroupType(m_data_group.data_group_type).name: m_data_group.get_sys_name() for m_data_group in data_groups
    }
    primary_group = PrimaryGroup(dict_primary_group)
    return primary_group


BS_COMMON_COLUMNS = {
    # data_group_type : (data_group_type, data_type, is_date_time, group, sort_order)
    DataGroupType.FACTORY_ID: (
        DataGroupType.FACTORY_ID.value,
        RawDataTypeDB.TEXT.name,
        False,
        1,
        1,
    ),
    DataGroupType.FACTORY_NAME: (
        DataGroupType.FACTORY_NAME.value,
        RawDataTypeDB.TEXT.name,
        False,
        1,
        2,
    ),
    DataGroupType.FACTORY_ABBR: (
        DataGroupType.FACTORY_ABBR.value,
        RawDataTypeDB.TEXT.name,
        False,
        1,
        3,
    ),
    DataGroupType.PLANT_ID: (DataGroupType.PLANT_ID.value, RawDataTypeDB.TEXT.name, False, 1, 4),
    DataGroupType.PLANT_NAME: (
        DataGroupType.PLANT_NAME.value,
        RawDataTypeDB.TEXT.name,
        False,
        1,
        5,
    ),
    DataGroupType.PLANT_ABBR: (
        DataGroupType.PLANT_ABBR.value,
        RawDataTypeDB.TEXT.name,
        False,
        1,
        6,
    ),
    DataGroupType.PROD_FAMILY_ID: (
        DataGroupType.PROD_FAMILY_ID.value,
        RawDataTypeDB.TEXT.name,
        True,
        1,
        7,
    ),
    DataGroupType.PROD_FAMILY_NAME: (
        DataGroupType.PROD_FAMILY_NAME.value,
        RawDataTypeDB.TEXT.name,
        False,
        1,
        8,
    ),
    DataGroupType.PROD_FAMILY_ABBR: (
        DataGroupType.PROD_FAMILY_ABBR.value,
        RawDataTypeDB.TEXT.name,
        False,
        1,
        9,
    ),
    DataGroupType.LINE_ID: (DataGroupType.LINE_ID.value, RawDataTypeDB.TEXT.name, False, 1, 10),
    DataGroupType.LINE_NAME: (DataGroupType.LINE_NAME.value, RawDataTypeDB.TEXT.name, False, 1, 11),
    DataGroupType.LINE_NO: (DataGroupType.LINE_NO.value, RawDataTypeDB.TEXT.name, False, 1, 12),
    DataGroupType.OUTSOURCE: (DataGroupType.OUTSOURCE.value, RawDataTypeDB.TEXT.name, False, 1, 13),
    DataGroupType.DEPT_ID: (DataGroupType.DEPT_ID.value, RawDataTypeDB.TEXT.name, False, 1, 14),
    DataGroupType.DEPT_NAME: (DataGroupType.DEPT_NAME.value, RawDataTypeDB.TEXT.name, False, 1, 14),
    DataGroupType.DEPT_ABBR: (DataGroupType.DEPT_ABBR.value, RawDataTypeDB.TEXT.name, False, 1, 15),
    DataGroupType.SECT_ID: (DataGroupType.SECT_ID.value, RawDataTypeDB.TEXT.name, False, 1, 16),
    DataGroupType.SECT_NAME: (DataGroupType.SECT_NAME.value, RawDataTypeDB.TEXT.name, False, 1, 17),
    DataGroupType.SECT_ABBR: (DataGroupType.SECT_ABBR.value, RawDataTypeDB.TEXT.name, False, 1, 18),
    DataGroupType.PROD_ID: (DataGroupType.PROD_ID.value, RawDataTypeDB.TEXT.name, False, 1, 19),
    DataGroupType.PROD_NAME: (DataGroupType.PROD_NAME.value, RawDataTypeDB.TEXT.name, False, 1, 20),
    DataGroupType.PROD_ABBR: (DataGroupType.PROD_ABBR.value, RawDataTypeDB.TEXT.name, False, 1, 21),
    DataGroupType.PART_TYPE: (DataGroupType.PART_TYPE.value, RawDataTypeDB.TEXT.name, False, 1, 22),
    DataGroupType.PART_NAME: (DataGroupType.PART_NAME.value, RawDataTypeDB.TEXT.name, False, 1, 23),
    DataGroupType.PART_ABBR: (DataGroupType.PART_ABBR.value, RawDataTypeDB.TEXT.name, False, 1, 24),
    DataGroupType.PART_NO_FULL: (
        DataGroupType.PART_NO_FULL.value,
        RawDataTypeDB.TEXT.name,
        False,
        1,
        25,
    ),
    DataGroupType.PART_NO: (DataGroupType.PART_NO.value, RawDataTypeDB.TEXT.name, False, 1, 26),
    DataGroupType.EQUIP_ID: (DataGroupType.EQUIP_ID.value, RawDataTypeDB.TEXT.name, False, 1, 27),
    DataGroupType.EQUIP_NAME: (
        DataGroupType.EQUIP_NAME.value,
        RawDataTypeDB.TEXT.name,
        False,
        1,
        28,
    ),
    DataGroupType.EQUIP_PRODUCT_NO: (
        DataGroupType.EQUIP_PRODUCT_NO.value,
        RawDataTypeDB.TEXT.name,
        False,
        1,
        29,
    ),
    DataGroupType.EQUIP_PRODUCT_DATE: (
        DataGroupType.EQUIP_PRODUCT_DATE.value,
        RawDataTypeDB.DATETIME.name,
        True,
        1,
        30,
    ),
    DataGroupType.EQUIP_NO: (DataGroupType.EQUIP_NO.value, RawDataTypeDB.TEXT.name, False, 1, 31),
    DataGroupType.STATION_NO: (
        DataGroupType.STATION_NO.value,
        RawDataTypeDB.TEXT.name,
        False,
        1,
        32,
    ),
    DataGroupType.PROCESS_ID: (
        DataGroupType.PROCESS_ID.value,
        RawDataTypeDB.TEXT.name,
        False,
        1,
        33,
    ),
    DataGroupType.PROCESS_NAME: (
        DataGroupType.PROCESS_NAME.value,
        RawDataTypeDB.TEXT.name,
        False,
        1,
        34,
    ),
    DataGroupType.PROCESS_ABBR: (
        DataGroupType.PROCESS_ABBR.value,
        RawDataTypeDB.TEXT.name,
        False,
        1,
        35,
    ),
    DataGroupType.DATA_ID: (DataGroupType.DATA_ID.value, RawDataTypeDB.TEXT.name, False, 2, 36),
    DataGroupType.DATA_NAME: (DataGroupType.DATA_NAME.value, RawDataTypeDB.TEXT.name, False, 2, 37),
    DataGroupType.DATA_ABBR: (DataGroupType.DATA_ABBR.value, RawDataTypeDB.TEXT.name, False, 2, 38),
    DataGroupType.DATA_VALUE: (
        DataGroupType.DATA_VALUE.value,
        RawDataTypeDB.TEXT.name,
        False,
        2,
        39,
    ),
    DataGroupType.UNIT: (
        DataGroupType.UNIT.value,
        RawDataTypeDB.TEXT.name,
        False,
        2,
        48,
    ),
    DataGroupType.DATA_TIME: (
        DataGroupType.DATA_TIME.value,
        RawDataTypeDB.DATETIME.name,
        True,
        1,
        40,
    ),
    DataGroupType.AUTO_INCREMENTAL: (
        DataGroupType.AUTO_INCREMENTAL.value,
        RawDataTypeDB.TEXT.name,
        False,
        1,
        41,
    ),
    DataGroupType.DATA_SERIAL: (
        DataGroupType.DATA_SERIAL.value,
        RawDataTypeDB.TEXT.name,
        False,
        1,
        42,
    ),
    DataGroupType.SUB_PART_NO: (
        DataGroupType.SUB_PART_NO.value,
        RawDataTypeDB.TEXT.name,
        False,
        1,
        43,
    ),
    DataGroupType.SUB_TRAY_NO: (
        DataGroupType.SUB_TRAY_NO.value,
        RawDataTypeDB.TEXT.name,
        False,
        1,
        44,
    ),
    DataGroupType.SUB_LOT_NO: (
        DataGroupType.SUB_LOT_NO.value,
        RawDataTypeDB.TEXT.name,
        False,
        1,
        45,
    ),
    DataGroupType.SUB_SERIAL: (
        DataGroupType.SUB_SERIAL.value,
        RawDataTypeDB.TEXT.name,
        False,
        1,
        46,
    ),
    DataGroupType.HORIZONTAL_DATA: (
        DataGroupType.HORIZONTAL_DATA.value,
        RawDataTypeDB.TEXT.name,
        False,
        1,
        47,
    ),
}

additional_column = ['data_group_type', 'data_type', 'is_datetime', 'group', 'sort_order']

# set sample
dict_sample = {
    DataGroupType.LINE_ID: ('mapping_factory_machine', 't_line_id'),
    DataGroupType.PROCESS_ID: ('mapping_process_data', 't_process_id'),
    DataGroupType.PART_NO: ('mapping_part', 't_part_no'),
    DataGroupType.EQUIP_ID: ('mapping_factory_machine', 't_equip_id'),
    DataGroupType.DATA_ID: ('mapping_process_data', 't_data_id'),
    DataGroupType.DATA_TIME: (),
    DataGroupType.DATA_VALUE: (),
    DataGroupType.DATA_SERIAL: (),
    DataGroupType.LINE_NAME: ('mapping_factory_machine', 't_line_name'),
    DataGroupType.PROCESS_NAME: ('mapping_process_data', 't_process_name'),
    DataGroupType.EQUIP_NAME: ('mapping_factory_machine', 't_equip_name'),
    DataGroupType.DATA_NAME: ('mapping_process_data', 't_data_name'),
    DataGroupType.SUB_PART_NO: (),
    DataGroupType.SUB_LOT_NO: (),
    DataGroupType.SUB_TRAY_NO: (),
    DataGroupType.SUB_SERIAL: (),
    DataGroupType.FACTORY_ID: (),
    DataGroupType.FACTORY_NAME: (),
    DataGroupType.PLANT_ID: (),
    DataGroupType.DEPT_ID: (),
    DataGroupType.DEPT_NAME: (),
    DataGroupType.PART_NO_FULL: ('m_part', 'part_factid'),
}


def get_yoyakugo(ui_lang=None):
    if not ui_lang:
        ui_lang = 'ja'

    df_config = DataFrame(list(BS_COMMON_COLUMNS.values()), columns=additional_column)

    name_col = MDataGroup.pick_column_by_language(ui_lang)
    with BridgeStationModel.get_db_proxy() as db_instance:
        df = MDataGroup.get_all_as_df(db_instance)
        df = df.merge(df_config, how='inner', on=MDataGroup.Columns.data_group_type.name)
        df = df[df[MDataGroup.Columns.data_group_type.name] < DataGroupType.GENERATED.value]
        df = df.sort_values(by='sort_order')

    df[['id', 'name_sys', 'name']] = df[[MDataGroup.get_foreign_id_column_name(), 'data_name_en', name_col]]

    df = df[['id', 'name_sys', 'name'] + additional_column]

    # set hint
    df['hint'] = 'ユニークな英数字を入力してください'

    df['sample'] = None

    with BridgeStationModel.get_db_proxy() as db_instance:
        for key, value in dict_sample.items():  # type: DataGroupType, Tuple[str, str]
            if not value:
                continue
            id = key.value
            table, column = value
            sql = f'SELECT {column} from {table} LIMIT 1'
            # _, rows = db_instance.run_sql(sql, row_is_dict=False, params=(table, column))
            _, rows = db_instance.run_sql(sql, row_is_dict=False)
            if rows:
                df['sample'] = np.where(df['id'] == id, rows[0], df['sample'])

    df.dropna(subset=['name'], inplace=True)
    return df.to_dict('records')


def insert_default_v2_history_data_group_old():
    from ap.setting_module.models import MDataGroup as ESMDataGroup

    key_names = [*MDataGroup.get_all_name_columns(), MDataGroup.Columns.data_group_type.name]
    with make_session() as meta_session:
        for data_group_type in DataGroupType.get_v2_history_generated_columns():
            m_data_group = ESMDataGroup()
            m_data_group.data_name_jp = data_group_type.name
            m_data_group.data_name_en = data_group_type.name
            m_data_group.data_name_sys = data_group_type.name
            m_data_group.data_group_type = data_group_type.value
            insert_or_update_config(meta_session, m_data_group, key_names)


def insert_default_v2_history_data_group(db_instance: PostgreSQL):
    data_group_df = MDataGroup.get_all_as_df(db_instance)
    insert_rows = []
    for data_group_type in DataGroupType.get_v2_history_generated_columns():
        if data_group_df[
            (data_group_df[MDataGroup.Columns.data_name_jp.name] == data_group_type.name)
            & (data_group_df[MDataGroup.Columns.data_name_en.name] == data_group_type.name)
            & (data_group_df[MDataGroup.Columns.data_name_sys.name] == data_group_type.name)
            & (data_group_df[MDataGroup.Columns.data_group_type.name] == data_group_type.value)
        ].empty:
            insert_rows.append(
                (data_group_type.name, data_group_type.name, data_group_type.name, data_group_type.value),
            )

    db_instance.bulk_insert(
        MDataGroup.get_table_name(),
        [
            MDataGroup.Columns.data_name_jp.name,
            MDataGroup.Columns.data_name_en.name,
            MDataGroup.Columns.data_name_sys.name,
            MDataGroup.Columns.data_group_type.name,
        ],
        insert_rows,
    )
