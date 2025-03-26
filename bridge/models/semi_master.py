from ap.common.constants import DataType
from bridge.models.bridge_station import SemiMasterModel
from bridge.models.model_utils import TableColumn


class SemiMaster(SemiMasterModel):
    class Columns(TableColumn):
        factor = (1, DataType.INTEGER)
        group_id = (2, DataType.INTEGER)
        value = (3, DataType.TEXT)
        updated_at = (6, DataType.DATETIME)

    _table_name = 'semi_master'
    primary_keys = []
    partition_columns = []

    def __init__(self, dict_proc=None):
        dict_proc = dict_proc if dict_proc else {}
        self.factor = dict_proc.get(SemiMaster.Columns.factor.name)
        self.group_id = dict_proc.get(SemiMaster.Columns.group_id.name)
        self.value = dict_proc.get(SemiMaster.Columns.value.name)
