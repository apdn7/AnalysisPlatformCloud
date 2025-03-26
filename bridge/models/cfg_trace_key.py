from ap.common.constants import DataType
from bridge.models.bridge_station import ConfigModel
from bridge.models.model_utils import TableColumn


class CfgTraceKey(ConfigModel):
    def __init__(self, dict_trace_key=None):
        if not dict_trace_key:
            dict_trace_key = {}
        self.id = dict_trace_key.get(CfgTraceKey.Columns.id.name)
        if self.id is None:
            del self.id
        self.trace_id = dict_trace_key.get(CfgTraceKey.Columns.trace_id.name)

        self.self_column_id = dict_trace_key.get(CfgTraceKey.Columns.self_column_id.name)
        self.self_column_substr_from = dict_trace_key.get(CfgTraceKey.Columns.self_column_substr_from.name)
        self.self_column_substr_to = dict_trace_key.get(CfgTraceKey.Columns.self_column_substr_to.name)

        self.target_column_id = dict_trace_key.get(CfgTraceKey.Columns.target_column_id.name)
        self.target_column_substr_from = dict_trace_key.get(CfgTraceKey.Columns.target_column_substr_from.name)
        self.target_column_substr_to = dict_trace_key.get(CfgTraceKey.Columns.target_column_substr_to.name)

        self.delta_time = dict_trace_key.get(CfgTraceKey.Columns.delta_time.name)
        self.cut_off = dict_trace_key.get(CfgTraceKey.Columns.cut_off.name)

        self.order = dict_trace_key.get(CfgTraceKey.Columns.order.name)

        self.created_at = dict_trace_key.get(CfgTraceKey.Columns.created_at.name)
        self.updated_at = dict_trace_key.get(CfgTraceKey.Columns.updated_at.name)

    class Columns(TableColumn):
        id = (1, DataType.INTEGER)
        trace_id = (2, DataType.INTEGER)

        self_column_id = (3, DataType.INTEGER)
        self_column_substr_from = (4, DataType.INTEGER)
        self_column_substr_to = (5, DataType.INTEGER)

        target_column_id = (6, DataType.INTEGER)
        target_column_substr_from = (7, DataType.INTEGER)
        target_column_substr_to = (8, DataType.INTEGER)

        delta_time = (9, DataType.REAL)
        cut_off = (10, DataType.REAL)
        order = (11, DataType.INTEGER)

        created_at = (12, DataType.DATETIME)
        updated_at = (13, DataType.DATETIME)

    _table_name = 'cfg_trace_key'
    primary_keys = [Columns.id]

    def as_dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}

    @classmethod
    def get_trace_keys(cls, db_instance, trace_id):
        """
        get keys of an edge
        :param db_instance:
        :param trace_id:
        :return:
        """

        _, keys = cls.select_records(
            db_instance,
            dic_conditions={cls.Columns.trace_id.name: trace_id},
            dic_order_by=[cls.Columns.trace_id.name],
        )
        return [CfgTraceKey(record) for record in keys]
