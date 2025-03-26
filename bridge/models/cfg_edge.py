from ap.common.constants import DataType
from bridge.models.model_utils import TableColumn


class CfgEdge:
    def __init__(self, dict_edge=None):
        if not dict_edge:
            dict_edge = {}
        self.repository_id = dict_edge.get(CfgEdge.Columns.repository_id.name, None)
        self.edge_id = dict_edge.get(CfgEdge.Columns.edge_id.name, None)
        self.ip_address = dict_edge.get(CfgEdge.Columns.ip_address.name, None)
        self.created_at = dict_edge.get(CfgEdge.Columns.created_at.name, None)
        self.updated_at = dict_edge.get(CfgEdge.Columns.updated_at.name, None)

    class Columns(TableColumn):
        repository_id = (1, DataType.INTEGER)
        edge_id = (2, DataType.INTEGER)
        ip_address = (3, DataType.TEXT)
        created_at = (4, DataType.DATETIME)
        updated_at = (5, DataType.DATETIME)

    _table_name = 'cfg_edge'
    primary_keys = [Columns.repository_id, Columns.edge_id]
