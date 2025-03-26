from ap.common.constants import DataType
from bridge.models.bridge_station import OthersDBModel
from bridge.models.model_utils import TableColumn


class AppLog(OthersDBModel):
    class Columns(TableColumn):
        id = (1, DataType.INTEGER)
        ip = (2, DataType.TEXT)
        action = (3, DataType.TEXT)
        description = (4, DataType.TEXT)
        created_at = (5, DataType.DATETIME)

    _table_name = 't_app_log'
    primary_keys = [Columns.id]
