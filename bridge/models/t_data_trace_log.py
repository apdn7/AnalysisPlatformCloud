from ap.common.constants import DataType
from bridge.models.bridge_station import ConfigModel
from bridge.models.model_utils import TableColumn


class DataTraceLog(ConfigModel):
    class Columns(TableColumn):
        id = (1, DataType.INTEGER)
        date_time = (2, DataType.TEXT)
        dataset_id = (3, DataType.TEXT)
        event_type = (4, DataType.TEXT)
        event_action = (5, DataType.TEXT)
        target = (6, DataType.TEXT)
        exe_time = (7, DataType.INTEGER)
        data_size = (8, DataType.INTEGER)
        rows = (9, DataType.INTEGER)
        cols = (10, DataType.INTEGER)
        dumpfile = (11, DataType.TEXT)
        created_at = (12, DataType.DATETIME)
        updated_at = (13, DataType.DATETIME)

    _table_name = 't_data_trace_log'
    primary_keys = [Columns.id]

    @classmethod
    def get_max_id(cls):
        pass
        # out = cls.query.options(load_only(cls.id)).order_by(cls.id.desc()).first()
        # if out:
        #     return out.id
        # else:
        #     return 0
