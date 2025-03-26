from typing import Dict, List, Tuple

from ap.common.constants import BaseEnum, DataType
from ap.common.pydn.dblib.db_common import (
    SqlComparisonOperator,
    gen_select_by_condition_sql,
    gen_truncate_sql,
)
from ap.common.pydn.dblib.postgresql import PostgreSQL
from bridge.models.bridge_station import BridgeStationModel
from bridge.models.m_equip import MEquip
from bridge.models.m_line import MLine
from bridge.models.m_part import MPart
from bridge.models.m_plant import MPlant
from bridge.models.m_process import MProcess
from bridge.models.model_utils import TableColumn


class EtlMapping(BridgeStationModel):
    _bridge_table = 'bridge_table'
    _source_table = 'source_table'
    _bridge_column = 'bridge_column'
    _source_column = 'source_column'

    @classmethod
    def _get_filter_datasource_id_sql(cls, datasource_id):
        parameter_marker = cls.get_parameter_marker()

        if datasource_id:
            return f'{cls._source_table}.datasource_id = {parameter_marker}', (datasource_id,)
        else:
            return '', None

    @classmethod
    def _get_filter_table_id_sql(cls, table_ids: List):
        parameter_marker = cls.get_parameter_marker()
        if table_ids:
            return f'{cls._bridge_table}.id IN {parameter_marker}', (table_ids,)
        else:
            return '', None

    @classmethod
    def get_records_by_data_src(cls, data_src_id, table_ids=None):
        filter_datasource_id_sql, filter_datasource_id_params = cls._get_filter_datasource_id_sql(data_src_id)
        filter_table_id_sql, filter_table_id_params = cls._get_filter_table_id_sql(table_ids)

        where_sql = ','.join([sub_sql for sub_sql in [filter_datasource_id_sql, filter_table_id_sql] if sub_sql])
        params = tuple(filter(None, [filter_datasource_id_params, filter_table_id_params]))
        where_clause = f'WHERE {where_sql}' if where_sql else ''

        sql = f'''
            SELECT bridge_table.id,
                   bridge_table.table_name,
                   source_table.table_name,
                   bridge_column.column_name,
                   source_column.column_name,
                   source_column.is_auto_incremental
            FROM etl_table {cls._bridge_table}
                     JOIN etl_table {cls._source_table}
                          ON source_table.bridge_station_table_id = bridge_table.id
                     JOIN etl_column {cls._bridge_column}
                          ON bridge_column.table_id = bridge_table.id
                     JOIN etl_column {cls._source_column}
                          ON source_column.table_id = source_table.id
                              AND source_column.bridge_station_column_id = bridge_column.id
            {where_clause}
            ORDER BY bridge_table.insert_order asc, bridge_column.is_auto_incremental desc
        '''

        return sql, params


class EtlTable(EtlMapping):
    class Columns(TableColumn):
        id = (1, DataType.INTEGER)
        table_name = (2, DataType.INTEGER)
        datasource_type = (3, DataType.INTEGER)
        datasource_id = (4, DataType.INTEGER)
        insert_order = (5, DataType.INTEGER)
        bridge_station_table_id = (6, DataType.INTEGER)

    _table_name = 'etl_table'
    primary_keys = [Columns.id]

    @classmethod
    def get_by_name(cls, db_instance, data_source_type, table_name):
        dic_conditions = {
            cls.Columns.datasource_type.name: data_source_type,
            cls.Columns.table_name.name: table_name,
        }
        sql, params = gen_select_by_condition_sql(cls, dic_conditions)
        _, rows = db_instance.run_sql(sql, params=params)

        return rows[0]

    @classmethod
    def get_all_bridge_station_table(cls, db_instance):
        dic_conditions = {cls.Columns.datasource_type.name: 'BRIDGE'}
        sql, params = gen_select_by_condition_sql(cls, dic_conditions)
        _, tables = db_instance.run_sql(sql, params=params, row_is_dict=True)

        dic_conditions = {EtlColumn.Columns.bridge_station_column_id.name: None}
        sql, params = gen_select_by_condition_sql(cls, dic_conditions)
        _, columns = db_instance.run_sql(sql, params=params, row_is_dict=True)

        return tables, columns


class EtlColumn(EtlMapping):
    class Columns(TableColumn):
        id = (1, DataType.INTEGER)
        table_id = (2, DataType.INTEGER)
        column_name = (3, DataType.INTEGER)
        bridge_station_column_id = (4, DataType.INTEGER)
        is_auto_incremental = (5, DataType.BOOLEAN)
        is_ignore = (6, DataType.BOOLEAN)

    _table_name = 'etl_column'
    primary_keys = [Columns.id]


class EtlColumnTransaction(EtlMapping):
    class Columns(TableColumn):
        id = (1, DataType.INTEGER)
        data_source_id = (2, DataType.INTEGER)
        column_name = (3, DataType.TEXT)
        master_type = (4, DataType.INTEGER)

    _table_name = 'etl_column_transaction'
    primary_keys = [Columns.id]

    @classmethod
    def get_by_data_source_id_column_name(cls, db_instance, data_source_id, column_name):
        cond = {
            cls.Columns.data_source_id.name: data_source_id,
            cls.Columns.column_name.name: column_name,
        }
        _, row = cls.select_records(db_instance, dic_conditions=cond, limit=1)
        if not row:
            return None
        return row

    @classmethod
    def get_by_column_names(cls, db_instance, column_names: [List, Tuple], is_get_dict_name_type=False):
        cond = {cls.Columns.column_name.name: [(SqlComparisonOperator.IN, tuple(column_names))]}
        _, rows = cls.select_records(db_instance, dic_conditions=cond)

        if not rows:
            return []
        if is_get_dict_name_type:
            column_name_col = cls.Columns.column_name.name
            master_type_col = cls.Columns.master_type.name
            rows = {row[column_name_col]: row[master_type_col] for row in rows}
        return rows


class MasterType(BaseEnum):
    PLANT = (1, MPlant)
    LINE = (2, MLine)
    PROCESS = (3, MProcess)
    EQUIP = (4, MEquip)
    PART = (5, MPart)

    # todo: add cache for all these static methods

    @staticmethod
    def get_all_members():
        return list(MasterType.__members__.values())

    @staticmethod
    def get_dict_master_type_table_class():
        return {type.value[0]: type.value[1] for type in MasterType.get_all_members()}

    @staticmethod
    def get_dict_master_type_table_name():
        return {type.value[0]: type.value[1].get_table_name() for type in MasterType.get_all_members()}

    @staticmethod
    def get_dict_master_type_table_pk_keys():
        return {_type.value[0]: _type.value[1].primary_keys for _type in MasterType.get_all_members()}

    @staticmethod
    def get_dict_master_type_column_names():
        """
        Ex:  {1: ('plant_no',), 2: ('plant_no', 'line_no'), 3: ('plant_no', 'line_no', 'process_no'),}
        :return:
        """
        master_types = MasterType.get_dict_master_type_table_pk_keys()
        dict_master_type_column_names = {}
        for master_type, pk_columns in master_types.items():
            dict_master_type_column_names[master_type] = tuple([col.name for col in pk_columns])
        return dict_master_type_column_names

    @staticmethod
    def get_mapping_config_master_type(dict_cfg_master_columns: Dict[int, str], is_revered=False):
        """
        Gets mapping config master type column name with master table PK column names.

        { master column : transaction column}

        :return:
        """
        master_pk_columns = MasterType.get_dict_master_type_column_names()

        dict_columns = {}  # { master column : transaction column}
        for master_type, columns in master_pk_columns.items():
            for col in columns:
                if col not in dict_columns:
                    dict_columns[col] = dict_cfg_master_columns.get(master_type, None)
        if is_revered:
            dict_columns = {value: key for (key, value) in dict_columns.items() if key}
        return dict_columns

    @staticmethod
    def get_master_type_by_name(master_table_name: str):
        """
        Gets master type by master table name

        :return: enum
        """
        result = None
        if master_table_name:
            masters = MasterType.get_all_members()
            target = next(
                (master for master in masters if master_table_name == master.value[1].get_original_table_name()),
                None,
            )
            if target:
                result = MasterType(target)
        return result

    @staticmethod
    def get_master_model_by_type(type_number: int):
        """
        Gets master model class by type number

        :return: model class
        """
        masters = MasterType.get_all_members()
        return next((master.value[1] for master in masters if type_number == master.value[0]), None)

    @staticmethod
    def get_master_data_by_id(db_instance: PostgreSQL, master_type: BaseEnum, ids: [List, Tuple]):
        _, model_class = master_type.value
        dic_cond = {model_class.Columns.id.name: [(SqlComparisonOperator.IN, tuple(ids))]}
        cols, rows = model_class.select_records(db_instance, dic_conditions=dic_cond, row_is_dict=False)
        return cols, rows


class EtlMappingUtil:
    @classmethod
    def truncate_all_mapping_data(cls, db_instance):
        etl_mapping_classes = EtlMapping.get_all_subclasses()
        for etl_mapping in etl_mapping_classes:
            sql = gen_truncate_sql(etl_mapping, with_cascade=True)
            db_instance.execute_sql(sql)
