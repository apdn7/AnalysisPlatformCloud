from ap.common.constants import DataType
from ap.common.pydn.dblib.db_common import OrderBy, SqlComparisonOperator
from bridge.models.bridge_station import OthersDBModel
from bridge.models.model_utils import TableColumn


class CsvManagement(OthersDBModel):
    class Columns(TableColumn):
        id = (1, DataType.INTEGER)
        file_name = (2, DataType.TEXT)
        data_table_id = (3, DataType.INTEGER)
        data_time = (4, DataType.INTEGER)
        data_process = (5, DataType.TEXT)
        data_line = (6, DataType.TEXT)
        data_delimiter = (7, DataType.TEXT)
        data_encoding = (8, DataType.TEXT)
        data_size = (8, DataType.REAL)
        scan_status = (10, DataType.BOOLEAN)
        dump_status = (11, DataType.BOOLEAN)
        created_at = (97, DataType.DATETIME)
        updated_at = (98, DataType.DATETIME)

    _table_name = 't_csv_management'
    primary_keys = [Columns.file_name, Columns.data_table_id]

    def __init__(self, dic_csv_mana):
        self.id = dic_csv_mana.get(self.Columns.id.name)
        self.file_name = dic_csv_mana.get(self.Columns.file_name.name)
        self.data_table_id = dic_csv_mana.get(self.Columns.data_table_id)
        self.data_time = dic_csv_mana.get(self.Columns.data_time.name, '0000')
        self.data_process = dic_csv_mana.get(self.Columns.data_process.name, 'P0')
        self.data_line = dic_csv_mana.get(self.Columns.data_line.name, 'L0')
        self.data_delimiter = dic_csv_mana.get(self.Columns.data_delimiter.name)
        self.data_encoding = dic_csv_mana.get(self.Columns.data_encoding.name)
        self.data_size = dic_csv_mana.get(self.Columns.data_size.name)
        self.scan_status = dic_csv_mana.get(self.Columns.scan_status.name)
        self.dump_status = dic_csv_mana.get(self.Columns.dump_status.name)
        self.created_at = dic_csv_mana.get(self.Columns.created_at.name)
        self.updated_at = dic_csv_mana.get(self.Columns.updated_at.name)

    @classmethod
    def get_import_target_files(cls, db_instance, data_table_id, from_month=None, to_month=None, row_is_dict=True):
        dic_conditions = {
            cls.Columns.data_table_id.name: data_table_id,
            cls.Columns.dump_status.name: None,
        }
        if from_month:
            dic_conditions.update(
                {cls.Columns.data_time.name: [(SqlComparisonOperator.GREATER_THAN_OR_EQ, from_month)]},
            )

        if to_month:
            dic_conditions.update({cls.Columns.data_time.name: [(SqlComparisonOperator.LESS_THAN, to_month)]})

        sort_cols = [
            cls.Columns.data_time.name,
            cls.Columns.data_process.name,
            cls.Columns.data_line.name,
        ]
        cols, rows = cls.select_records(
            db_instance,
            dic_conditions=dic_conditions,
            dic_order_by=sort_cols,
            row_is_dict=row_is_dict,
            select_cols=[
                cls.Columns.id.name,
                cls.Columns.file_name.name,
                cls.Columns.data_time.name,
                cls.Columns.data_process.name,
                cls.Columns.data_line.name,
                cls.Columns.data_encoding.name,
                cls.Columns.data_delimiter.name,
                cls.Columns.data_size.name,
            ],
        )

        return cols, rows

    @classmethod
    def get_scan_master_target_files(cls, db_instance, data_table_id, scan_status=False, row_is_dict=True):
        dic_conditions = {
            cls.Columns.data_table_id.name: data_table_id,
            cls.Columns.scan_status.name: scan_status,
        }
        sort_cols = [
            cls.Columns.data_time.name,
            cls.Columns.data_process.name,
            cls.Columns.data_line.name,
        ]
        cols, rows = cls.select_records(
            db_instance,
            select_cols=[
                cls.Columns.id.name,
                cls.Columns.file_name.name,
                cls.Columns.data_time.name,
                cls.Columns.data_process.name,
                cls.Columns.data_line.name,
                cls.Columns.data_encoding.name,
                cls.Columns.data_delimiter.name,
                cls.Columns.data_size.name,
            ],
            dic_conditions=dic_conditions,
            dic_order_by=sort_cols,
            row_is_dict=row_is_dict,
        )

        return cols, rows

    @classmethod
    def get_by_data_table_id(cls, db_instance, data_table_id, row_is_dict=True):
        dic_conditions = {
            cls.Columns.data_table_id.name: data_table_id,
        }
        sort_cols = [
            cls.Columns.data_time.name,
            cls.Columns.data_process.name,
            cls.Columns.data_line.name,
        ]
        cols, rows = cls.select_records(
            db_instance,
            dic_conditions=dic_conditions,
            dic_order_by=sort_cols,
            row_is_dict=True,
        )

        if not row_is_dict:
            return [CsvManagement(row) for row in rows]

        return cols, rows

    @classmethod
    def save_scan_master_target(cls, db_instance, ids, status):
        dic_update_values = {cls.Columns.scan_status.name: status}
        CsvManagement.bulk_update_by_ids(db_instance, ids=ids, dic_update_values=dic_update_values)

    @classmethod
    def get_min_max_date_time(cls, db_instance, data_table_id):
        dic_conditions = {cls.Columns.data_table_id.name: data_table_id}
        dic_order_by = {cls.Columns.data_time.name: OrderBy.ASC.name}
        _, rows = cls.select_records(
            db_instance,
            select_cols=[cls.Columns.data_table_id.name, cls.Columns.data_time.name],
            dic_conditions=dic_conditions,
            dic_order_by=dic_order_by,
            row_is_dict=True,
        )
        if rows:
            return rows[0], rows[-1]
        else:
            return None, None

    @classmethod
    def get_last_pull(cls, db_instance, data_table_id):
        dic_conditions = {
            cls.Columns.data_table_id.name: data_table_id,
            cls.Columns.dump_status.name: True,
        }
        dic_order_by = {cls.Columns.data_time.name: OrderBy.ASC.name}
        _, rows = cls.select_records(
            db_instance,
            select_cols=[cls.Columns.data_table_id.name, cls.Columns.data_time.name],
            dic_conditions=dic_conditions,
            dic_order_by=dic_order_by,
            row_is_dict=True,
        )
        if len(rows):
            return rows[0]
        else:
            return None
