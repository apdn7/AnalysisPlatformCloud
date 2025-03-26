from ap.common.constants import DataType
from bridge.models.bridge_station import BridgeStationModel
from bridge.models.model_utils import TableColumn


class ArchivedCycle(BridgeStationModel):
    def __init__(self):
        pass

    class Columns(TableColumn):
        id = (1, DataType.INTEGER)
        job_id = (2, DataType.INTEGER)
        process_id = (3, DataType.INTEGER)
        archived_ids = (4, DataType.TEXT)
        created_at = (6, DataType.DATETIME)
        updated_at = (7, DataType.DATETIME)

    _table_name = 'archived_cycle'
    primary_keys = [Columns.id]
