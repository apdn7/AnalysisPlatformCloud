from ap.common.constants import RawDataTypeDB
from bridge.models.bridge_station import MasterModel
from bridge.models.model_utils import TableColumn


class MFunction(MasterModel):
    class Columns(TableColumn):
        id = (1, RawDataTypeDB.INTEGER)
        function_type = (2, RawDataTypeDB.TEXT)
        function_name_en = (3, RawDataTypeDB.TEXT)
        function_name_jp = (4, RawDataTypeDB.TEXT)
        description_en = (5, RawDataTypeDB.TEXT)
        description_jp = (6, RawDataTypeDB.TEXT)
        return_type = (7, RawDataTypeDB.TEXT)
        x_type = (8, RawDataTypeDB.TEXT)
        y_type = (9, RawDataTypeDB.TEXT)
        show_serial = (10, RawDataTypeDB.BOOLEAN)
        a = (11, RawDataTypeDB.TEXT)
        b = (12, RawDataTypeDB.TEXT)
        c = (13, RawDataTypeDB.TEXT)
        n = (14, RawDataTypeDB.TEXT)
        k = (15, RawDataTypeDB.TEXT)
        s = (16, RawDataTypeDB.TEXT)
        t = (17, RawDataTypeDB.TEXT)
        created_at = (97, RawDataTypeDB.DATETIME)
        updated_at = (98, RawDataTypeDB.DATETIME)

    _table_name = 'm_function'
    primary_keys = [Columns.id]

    def __init__(self, dict_proc=None):
        dict_proc = dict_proc if dict_proc else {}
        self.id = dict_proc.get(MFunction.Columns.id.name)
        self.function_type = dict_proc.get(MFunction.Columns.function_type.name)

    @classmethod
    def get_all(cls, db_instance, is_return_dict=False):
        _col, rows = cls.select_records(db_instance, row_is_dict=True)
        m_functions = [MFunction(row) for row in rows]
        if is_return_dict:
            return {m_function.id: m_function for m_function in m_functions}
        return m_functions
