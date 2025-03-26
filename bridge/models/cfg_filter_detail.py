from ap.common.constants import DataType
from bridge.models.bridge_station import ConfigModel
from bridge.models.model_utils import TableColumn


class CfgFilterDetail(ConfigModel):
    def __init__(self, dic_row=None):
        if not dic_row:
            dic_row = {}

        self.id = dic_row.get('id')
        if self.id is None:
            del self.id
        self.filter_id = dic_row.get('filter_id')
        self.parent_detail_id = dic_row.get('parent_detail_id')
        self.name = dic_row.get('name ')
        self.filter_condition = dic_row.get('filter_condition')
        self.filter_function = dic_row.get('filter_function')
        self.filter_from_pos = dic_row.get('filter_from_pos')
        self.order = dic_row.get('order')
        self.created_at = dic_row.get('created_at')
        self.updated_at = dic_row.get('updated_at')

    class Columns(TableColumn):
        id = (1, DataType.INTEGER)
        filter_id = (2, DataType.INTEGER)
        parent_detail_id = (3, DataType.INTEGER)
        name = (4, DataType.TEXT)
        filter_condition = (5, DataType.TEXT)
        filter_function = (6, DataType.TEXT)
        filter_from_pos = (7, DataType.INTEGER)
        order = (8, DataType.INTEGER)
        created_at = (9, DataType.DATETIME)
        updated_at = (10, DataType.DATETIME)

    _table_name = 'cfg_filter_detail'
    primary_keys = [Columns.id]

    def update_by_dict(self, **kwargs):
        for key, value in kwargs.items():
            if hasattr(self, key):
                setattr(self, key, value)

    @classmethod
    def get_filters(cls, ids):
        return cls.query.filter(cls.id.in_(ids))
