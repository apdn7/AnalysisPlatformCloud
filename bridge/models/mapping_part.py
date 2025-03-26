from ap.common.constants import RawDataTypeDB
from ap.common.pydn.dblib.db_common import SqlComparisonOperator
from ap.common.pydn.dblib.postgresql import PostgreSQL
from bridge.models.bridge_station import MasterModel
from bridge.models.model_utils import TableColumn


class MappingPart(MasterModel):
    class Columns(TableColumn):
        id = (1, RawDataTypeDB.INTEGER)
        t_part_type = (2, RawDataTypeDB.TEXT)
        t_part_name = (3, RawDataTypeDB.TEXT)
        t_part_abbr = (4, RawDataTypeDB.TEXT)
        t_part_no_full = (5, RawDataTypeDB.TEXT)
        t_part_no = (6, RawDataTypeDB.TEXT)
        part_id = (3, RawDataTypeDB.INTEGER)
        data_table_id = (4, RawDataTypeDB.INTEGER)

    __is_mapping_table__ = True
    _table_name = 'mapping_part'
    primary_keys = []
    unique_keys = [
        Columns.t_part_type,
        Columns.t_part_name,
        Columns.t_part_abbr,
        Columns.t_part_no_full,
        Columns.t_part_no,
    ]
    not_null_columns = [Columns.t_part_no]

    def __init__(self, dict_proc=None):
        dict_proc = dict_proc if dict_proc else {}
        self.t_part_type = dict_proc.get(MappingPart.Columns.t_part_type.name)
        self.t_part_name = dict_proc.get(MappingPart.Columns.t_part_name.name)
        self.t_part_abbr = dict_proc.get(MappingPart.Columns.t_part_abbr.name)
        self.t_part_no_full = dict_proc.get(MappingPart.Columns.t_part_no_full.name)
        self.t_part_no = dict_proc.get(MappingPart.Columns.t_part_no.name)
        self.part_id = dict_proc.get(MappingPart.Columns.part_id.name)
        self.data_table_id = dict_proc.get(MappingPart.Columns.data_table_id.name)

    @classmethod
    def get_part_id(
        cls,
        db_instance: PostgreSQL,
        t_part_abbr: str = None,
        t_part_name: str = None,
        t_part_no_full: str = None,
        t_part_type: str = None,
        t_part_no: str = None,
        data_table_id: int = None,
    ):
        dict_cond = {}
        if t_part_abbr != '' and t_part_abbr is not None:
            dict_cond[cls.Columns.t_part_abbr.name] = [(SqlComparisonOperator.EQUAL, t_part_abbr)]
        if t_part_name != '' and t_part_name is not None:
            dict_cond[cls.Columns.t_part_name.name] = [(SqlComparisonOperator.EQUAL, t_part_name)]
        if t_part_no_full != '' and t_part_no_full is not None:
            dict_cond[cls.Columns.t_part_no_full.name] = [(SqlComparisonOperator.EQUAL, t_part_no_full)]
        if t_part_type != '' and t_part_type is not None:
            dict_cond[cls.Columns.t_part_type.name] = [(SqlComparisonOperator.EQUAL, t_part_type)]
        if t_part_no != '' and t_part_no is not None:
            dict_cond[cls.Columns.t_part_no.name] = [(SqlComparisonOperator.EQUAL, t_part_no)]
        if data_table_id != '' and data_table_id is not None:
            dict_cond[cls.Columns.data_table_id.name] = [(SqlComparisonOperator.EQUAL, data_table_id)]

        _, row = cls.select_records(db_instance, dic_conditions=dict_cond, row_is_dict=True, limit=1)
        return row.get(cls.Columns.part_id.name) if row else None
