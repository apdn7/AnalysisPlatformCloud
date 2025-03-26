from typing import Dict, List

from ap.common.constants import DataGroupType, DataType
from ap.common.pydn.dblib.db_common import SqlComparisonOperator
from ap.common.pydn.dblib.postgresql import PostgreSQL
from bridge.models.bridge_station import ConfigModel
from bridge.models.model_utils import TableColumn


class CfgDataTableColumn(ConfigModel):
    class Columns(TableColumn):
        id = (1, DataType.INTEGER)
        name = (2, DataType.TEXT)

        data_table_id = (3, DataType.INTEGER)
        column_name = (4, DataType.TEXT)
        english_name = (5, DataType.TEXT)
        data_type = (6, DataType.TEXT)
        data_group_type = (7, DataType.INTEGER)
        order = (12, DataType.INTEGER)
        created_at = (98, DataType.DATETIME)
        updated_at = (99, DataType.DATETIME)

    def __init__(self, dict_proc=None):
        dict_proc = dict_proc if dict_proc else {}
        self.id = dict_proc.get(CfgDataTableColumn.Columns.id.name)
        if self.id is None:
            del self.id
        self.name = dict_proc.get(CfgDataTableColumn.Columns.name.name)

        self.data_table_id = dict_proc.get(CfgDataTableColumn.Columns.data_table_id.name)
        self.column_name = dict_proc.get(CfgDataTableColumn.Columns.column_name.name)
        self.english_name = dict_proc.get(CfgDataTableColumn.Columns.english_name.name)
        self.data_type = dict_proc.get(CfgDataTableColumn.Columns.data_type.name)
        self.data_group_type = dict_proc.get(CfgDataTableColumn.Columns.data_group_type.name)
        # self.is_serial_no = dict_proc.get(CfgDataTableColumn.Columns.is_serial_no.name)
        # self.is_get_date = dict_proc.get(CfgDataTableColumn.Columns.is_get_date.name)
        # self.is_linking_column = dict_proc.get(CfgDataTableColumn.Columns.is_linking_column.name)
        # self.is_auto_increment = dict_proc.get(CfgDataTableColumn.Columns.is_auto_increment.name)
        self.order = dict_proc.get(CfgDataTableColumn.Columns.order.name)
        self.created_at = dict_proc.get(CfgDataTableColumn.Columns.created_at.name)
        self.updated_at = dict_proc.get(CfgDataTableColumn.Columns.updated_at.name)

        self.m_data_group = None  # type: Optional[MDataGroup]
        self.m_data = None  # type: Optional[MData]

    _table_name = 'cfg_data_table_column'
    primary_keys = [Columns.id]

    @classmethod
    def get_by_data_table_id(cls, db_instance, data_table_id, is_return_dict=False):
        _, rows = cls.select_records(
            db_instance,
            dic_conditions={cls.Columns.data_table_id.name: data_table_id},
        )
        if not rows:
            return []
        return [CfgDataTableColumn(row) for row in rows] if not is_return_dict else rows

    @classmethod
    def get_auto_increment_col_else_get_date(cls, db_instance, process_id, column_name_only=True):
        return cls.get_auto_increment_col(db_instance, process_id, column_name_only) or cls.get_date_col(
            db_instance,
            process_id,
            column_name_only,
        )

    @classmethod
    def get_auto_increment_col(cls, db_instance, process_id, column_name_only=True):
        """
        get auto increment column
        :param column_name_only:
        :return:
        """
        dict_cond = {
            cls.Columns.data_table_id.name: process_id,
            cls.Columns.is_auto_increment.name: cls.parse_bool(True),
        }
        _col, rows = cls.select_records(db_instance, dic_conditions=dict_cond)
        if rows:
            if column_name_only:
                return rows[0][cls.Columns.column_name.name]
            else:
                return rows[0]
        else:
            return None

    @classmethod
    def get_date_col(cls, db_instance, cfg_data_table_id, column_name_only=True):
        """
        get date column
        :param column_name_only:
        :return:
        """
        dict_cond = {
            cls.Columns.data_table_id.name: cfg_data_table_id,
            cls.Columns.is_get_date.name: cls.parse_bool(True),
        }
        _col, rows = cls.select_records(db_instance, dic_conditions=dict_cond)
        if rows:
            if column_name_only:
                return rows[0][cls.Columns.column_name.name]
            else:
                return rows[0]
        else:
            return None

    @classmethod
    def get_column_names_by_data_group_types(
        cls,
        db_instance: PostgreSQL,
        data_table_id: int,
        data_group_types: List[DataGroupType],
    ):
        """
        get date column
        :param db_instance: an instance of database connection
        :param data_table_id: data table it
        :param data_group_types: list of data group type
        :return:
        """
        data_group_type_ids = [key.value for key in data_group_types]

        _, rows = cls.select_records(
            db_instance,
            dic_conditions={
                cls.Columns.data_table_id.name: data_table_id,
                cls.Columns.data_group_type.name: [(SqlComparisonOperator.IN, tuple(data_group_type_ids))],
            },
        )

        dic_recs = {rec[cls.Columns.data_group_type.name]: rec[cls.Columns.column_name.name] for rec in rows}
        cols = [dic_recs.get(id) for id in data_group_type_ids if id in dic_recs]
        return cols

    @classmethod
    def get_data_group_types_by_column_names(
        cls,
        db_instance: PostgreSQL,
        data_table_id: int,
        column_names: List[str],
    ) -> Dict[str, DataGroupType]:
        """
        Get dictionary of column name & data group type
        :param db_instance: a db instance
        :param data_table_id: data table id
        :param column_names: list of column name
        :return:
        """
        _, rows = cls.select_records(
            db_instance,
            dic_conditions={
                cls.Columns.data_table_id.name: data_table_id,
                cls.Columns.column_name.name: [(SqlComparisonOperator.IN, tuple(column_names))],
            },
        )
        dic_recs: Dict[str, DataGroupType] = {
            row[cls.Columns.column_name.name]: DataGroupType(row[cls.Columns.data_group_type.name]) for row in rows
        }

        return dic_recs

    @classmethod
    def get_order_column(cls, db_instance: PostgreSQL, data_table_id):
        _, rows = cls.select_records(
            db_instance,
            dic_conditions={
                cls.Columns.data_table_id.name: data_table_id,
                cls.Columns.data_group_type.name: DataGroupType.AUTO_INCREMENTAL.value,
            },
        )

        return [CfgDataTableColumn(row) for row in rows]

    @classmethod
    def get_split_columns(cls, db_instance: PostgreSQL, data_table_id):
        order_col = cls.get_order_column(db_instance, data_table_id)
        if order_col:
            return [
                DataGroupType.AUTO_INCREMENTAL,
                DataGroupType.PROCESS_NAME,
                DataGroupType.LINE_NAME,
            ]
        else:
            return [DataGroupType.DATA_TIME, DataGroupType.PROCESS_NAME, DataGroupType.LINE_NAME]
