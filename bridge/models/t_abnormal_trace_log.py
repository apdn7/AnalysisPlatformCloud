from ap.common.constants import DataType
from bridge.models.bridge_station import OthersDBModel
from bridge.models.model_utils import TableColumn


class AbnormalTraceLog(OthersDBModel):
    class Columns(TableColumn):
        id = (1, DataType.INTEGER)
        date_time = (2, DataType.TEXT)
        dataset_id = (3, DataType.INTEGER)
        log_level = (4, DataType.TEXT)
        event_type = (5, DataType.TEXT)
        event_action = (6, DataType.TEXT)
        location = (7, DataType.TEXT)
        return_code = (8, DataType.TEXT)
        message = (9, DataType.TEXT)
        dumpfile = (10, DataType.TEXT)
        created_at = (11, DataType.DATETIME)
        updated_at = (12, DataType.DATETIME)

    _table_name = 't_abnormal_trace_log'
    primary_keys = [Columns.id]
