from ap.common.constants import DataType
from bridge.models.bridge_station import BridgeStationModel
from bridge.models.model_utils import TableColumn


class ArchivedConfigMaster(BridgeStationModel):
    def __init__(self):
        pass

    class Columns(TableColumn):
        id = (1, DataType.INTEGER)
        table_name = (2, DataType.TEXT)
        archived_id = (3, DataType.INTEGER)
        data = (4, DataType.TEXT)
        created_at = (6, DataType.DATETIME)
        updated_at = (7, DataType.DATETIME)

    _table_name = 'archived_config_master'
    primary_keys = [Columns.id]
