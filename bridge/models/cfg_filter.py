from ap.common.constants import DataType
from bridge.models.bridge_station import ConfigModel
from bridge.models.model_utils import TableColumn


class CfgFilter(ConfigModel):
    def __init__(self, dic_row=None):
        if not dic_row:
            dic_row = {}

        self.id = dic_row.get(self.Columns.id.name)
        if self.id is None:
            del self.id
        self.process_id = dic_row.get(self.Columns.process_id.name)
        self.name = dic_row.get(self.Columns.name.name)
        self.column_id = dic_row.get(self.Columns.column_id.name)
        self.filter_type = dic_row.get(self.Columns.filter_type.name)
        self.parent_id = dic_row.get(self.Columns.parent_id.name)

        self.created_at = dic_row.get(self.Columns.created_at.name)
        self.updated_at = dic_row.get(self.Columns.updated_at.name)

    class Columns(TableColumn):
        id = (1, DataType.INTEGER)
        process_id = (2, DataType.INTEGER)
        name = (3, DataType.TEXT)
        column_id = (4, DataType.INTEGER)
        filter_type = (5, DataType.TEXT)
        parent_id = (6, DataType.INTEGER)

        created_at = (7, DataType.DATETIME)
        updated_at = (8, DataType.DATETIME)

    _table_name = 'cfg_filter'
    primary_keys = [Columns.id]

    def update_by_dict(self, **kwargs):
        for key, value in kwargs.items():
            if hasattr(self, key):
                setattr(self, key, value)
