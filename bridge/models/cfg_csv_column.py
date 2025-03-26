from ap.common.constants import DataType
from ap.common.pydn.dblib.db_common import OrderBy
from bridge.models.bridge_station import ConfigModel
from bridge.models.model_utils import TableColumn


class CfgCsvColumn(ConfigModel):
    class Columns(TableColumn):
        id = (1, DataType.INTEGER)
        data_source_id = (2, DataType.INTEGER)
        column_name = (3, DataType.TEXT)
        data_type = (4, DataType.TEXT)
        order = (5, DataType.INTEGER)
        directory_no = (8, DataType.INTEGER)
        created_at = (6, DataType.DATETIME)
        updated_at = (7, DataType.DATETIME)

    def __init__(self, dict_row=None):
        if not dict_row:
            dict_row = {}
        self.id = dict_row.get(CfgCsvColumn.Columns.id.name)
        if self.id is None:
            del self.id
        self.data_source_id = dict_row.get(CfgCsvColumn.Columns.data_source_id.name)
        self.column_name = dict_row.get(CfgCsvColumn.Columns.column_name.name)
        self.data_type = dict_row.get(CfgCsvColumn.Columns.data_type.name)
        self.order = dict_row.get(CfgCsvColumn.Columns.order.name)
        self.directory_no = dict_row.get(CfgCsvColumn.Columns.directory_no.name)
        self.created_at = dict_row.get(CfgCsvColumn.Columns.created_at.name)
        self.updated_at = dict_row.get(CfgCsvColumn.Columns.updated_at.name)

    _table_name = 'cfg_csv_column'
    primary_keys = [Columns.id]

    @classmethod
    def get_by_data_source_id(cls, db_instance, db_source_id):
        select_cond = {cls.Columns.data_source_id.name: db_source_id}
        dic_order_by = {cls.Columns.data_source_id.name: OrderBy.DESC.name}
        _, rows = cls.select_records(db_instance, dic_conditions=select_cond, dic_order_by=dic_order_by)
        if not rows:
            return []
        return [CfgCsvColumn(row) for row in rows]
