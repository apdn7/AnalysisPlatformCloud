from ap.common.constants import DataType
from bridge.models.bridge_station import MasterModel
from bridge.models.model_utils import TableColumn


class RProdPart(MasterModel):
    class Columns(TableColumn):
        id = (1, DataType.INTEGER)
        prod_id = (2, DataType.INTEGER)
        part_id = (3, DataType.INTEGER)
        created_at = (97, DataType.DATETIME)
        updated_at = (98, DataType.DATETIME)

    _table_name = 'r_prod_part'
    primary_keys = [Columns.id]
    not_null_columns = []

    def __init__(self, dict_proc=None):
        dict_proc = dict_proc if dict_proc else {}
        self.id = dict_proc.get(RProdPart.Columns.id.name)
        self.prod_id = dict_proc.get(RProdPart.Columns.prod_id.name)
        self.part_id = dict_proc.get(RProdPart.Columns.part_id.name)

    @classmethod
    def get_by_part_id(cls, db_instance, part_id):
        if not part_id or part_id == '':
            return None
        _, row = cls.select_records(db_instance, {cls.Columns.part_id.name: int(part_id)}, limit=1)
        if not row:
            return None
        return row
