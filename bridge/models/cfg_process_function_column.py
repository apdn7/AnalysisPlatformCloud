from ap.common.constants import DataType
from ap.common.pydn.dblib.postgresql import PostgreSQL
from bridge.models.bridge_station import BridgeStationModel, ConfigModel
from bridge.models.model_utils import TableColumn


class CfgProcessFunctionColumn(ConfigModel):
    class Columns(TableColumn):
        id = (1, DataType.INTEGER)
        function_id = (2, DataType.INTEGER)
        var_x = (3, DataType.INTEGER)
        var_y = (4, DataType.INTEGER)
        a = (5, DataType.TEXT)
        b = (6, DataType.TEXT)
        c = (7, DataType.TEXT)
        n = (8, DataType.TEXT)
        k = (9, DataType.TEXT)
        s = (10, DataType.TEXT)
        t = (11, DataType.TEXT)
        return_type = (9, DataType.TEXT)
        note = (10, DataType.TEXT)
        order = (11, DataType.INTEGER)
        process_column_id = (12, DataType.INTEGER)
        created_at = (97, DataType.DATETIME)
        updated_at = (98, DataType.DATETIME)

    def __init__(self, dict_proc=None):
        dict_proc = dict_proc if dict_proc else {}
        self.id = dict_proc.get(CfgProcessFunctionColumn.Columns.id.name)
        self.function_id = dict_proc.get(CfgProcessFunctionColumn.Columns.function_id.name)
        self.var_x = dict_proc.get(CfgProcessFunctionColumn.Columns.var_x.name)
        self.var_y = dict_proc.get(CfgProcessFunctionColumn.Columns.var_y.name)
        self.a = dict_proc.get(CfgProcessFunctionColumn.Columns.a.name)
        self.b = dict_proc.get(CfgProcessFunctionColumn.Columns.b.name)
        self.c = dict_proc.get(CfgProcessFunctionColumn.Columns.c.name)
        self.n = dict_proc.get(CfgProcessFunctionColumn.Columns.n.name)
        self.k = dict_proc.get(CfgProcessFunctionColumn.Columns.k.name)
        self.s = dict_proc.get(CfgProcessFunctionColumn.Columns.s.name)
        self.t = dict_proc.get(CfgProcessFunctionColumn.Columns.t.name)
        self.return_type = dict_proc.get(CfgProcessFunctionColumn.Columns.return_type.name)
        self.note = dict_proc.get(CfgProcessFunctionColumn.Columns.note.name)
        self.order = dict_proc.get(CfgProcessFunctionColumn.Columns.order.name)
        self.process_column_id = dict_proc.get(CfgProcessFunctionColumn.Columns.process_column_id.name)
        self.m_function = None  # type: MFunction

    _table_name = 'cfg_process_function_column'
    primary_keys = [Columns.id]

    @classmethod
    @BridgeStationModel.use_db_instance()
    def get_all_cfg_col_ids(cls, db_instance: PostgreSQL = None):
        data = cls.get_all_records(db_instance, row_is_dict=True, is_return_object=True)
        cfg_col_ids = []
        for row in data:
            if row.var_x:
                cfg_col_ids.append(row.var_x)

            if row.var_y:
                cfg_col_ids.append(row.var_y)

        return cfg_col_ids
