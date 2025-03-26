from ap.common.constants import DataType
from bridge.models.bridge_station import OthersDBModel
from bridge.models.model_utils import TableColumn


class ProcDataCount(OthersDBModel):
    class Columns(TableColumn):
        id = (1, DataType.INTEGER)
        job_id = (2, DataType.INTEGER)
        process_id = (3, DataType.INTEGER)
        datetime = (4, DataType.DATETIME)
        count = (5, DataType.INTEGER)
        count_file = (6, DataType.INTEGER)
        created_at = (7, DataType.DATETIME)

    _table_name = 't_proc_data_count'
    primary_keys = [Columns.id]
