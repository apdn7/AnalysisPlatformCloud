from typing import List, Tuple

from ap.common.constants import RawDataTypeDB
from ap.common.pydn.dblib.db_common import SqlComparisonOperator
from ap.common.pydn.dblib.postgresql import PostgreSQL
from bridge.models.bridge_station import AbstractProcess, MasterModel
from bridge.models.m_data import MData
from bridge.models.m_data_group import MDataGroup, PrimaryGroup
from bridge.models.m_process import MProcess
from bridge.models.model_utils import TableColumn


class MappingProcessData(MasterModel, AbstractProcess):
    class Columns(TableColumn):
        id = (1, RawDataTypeDB.INTEGER)
        t_process_id = (2, RawDataTypeDB.TEXT)
        t_process_name = (3, RawDataTypeDB.TEXT)
        t_process_abbr = (4, RawDataTypeDB.TEXT)
        t_data_id = (5, RawDataTypeDB.TEXT)
        t_data_name = (6, RawDataTypeDB.TEXT)
        t_data_abbr = (7, RawDataTypeDB.TEXT)
        t_prod_family_id = (11, RawDataTypeDB.TEXT)
        t_prod_family_name = (12, RawDataTypeDB.TEXT)
        t_prod_family_abbr = (13, RawDataTypeDB.TEXT)
        t_unit = (8, RawDataTypeDB.TEXT)
        data_id = (9, RawDataTypeDB.INTEGER)
        data_table_id = (10, RawDataTypeDB.INTEGER)

    def __init__(self, dict_proc=None):
        dict_proc = dict_proc if dict_proc else {}
        self.t_process_id = dict_proc.get(MappingProcessData.Columns.t_process_id.name)
        self.t_process_name = dict_proc.get(MappingProcessData.Columns.t_process_name.name)
        self.t_process_abbr = dict_proc.get(MappingProcessData.Columns.t_process_abbr.name)
        self.t_data_id = dict_proc.get(MappingProcessData.Columns.t_data_id.name)
        self.t_data_name = dict_proc.get(MappingProcessData.Columns.t_data_name.name)
        self.t_data_abbr = dict_proc.get(MappingProcessData.Columns.t_data_abbr.name)
        self.t_prod_family_id = dict_proc.get(MappingProcessData.Columns.t_prod_family_id.name)
        self.t_prod_family_name = dict_proc.get(MappingProcessData.Columns.t_prod_family_name.name)
        self.t_prod_family_abbr = dict_proc.get(MappingProcessData.Columns.t_prod_family_abbr.name)
        self.t_unit = dict_proc.get(MappingProcessData.Columns.t_unit.name)
        self.data_id = int(dict_proc.get(MappingProcessData.Columns.data_id.name))
        self.data_table_id = int(dict_proc.get(MappingProcessData.Columns.data_table_id.name))
        self.m_data: MData = None

    __is_mapping_table__ = True
    _table_name = 'mapping_process_data'
    primary_keys = []
    unique_keys = [
        Columns.t_process_id,
        Columns.t_process_name,
        Columns.t_data_id,
        Columns.t_data_name,
    ]

    # not_null_columns = [Columns.t_process_id, Columns.t_data_id]

    @classmethod
    def get_in_process_no(cls, db_instance, t_process_no_s: [Tuple, List], is_cascade=False):
        dict_cond = {cls.Columns.t_process_id.name: [(SqlComparisonOperator.IN, tuple(t_process_no_s))]}
        _col, rows = cls.select_records(db_instance, dic_conditions=dict_cond, row_is_dict=True)
        mapping_data_s = [MappingProcessData(row) for row in rows]

        if is_cascade:
            cols, rows = MProcess.select_records(db_instance)
            dict_m_process = {row['id']: MProcess(row) for row in rows}

            cols, rows = MData.select_records(db_instance)
            dict_m_data = {row['id']: MData(row) for row in rows}

            cols, rows = MDataGroup.select_records(db_instance)
            dict_m_data_group = {row['id']: MDataGroup(row) for row in rows}
            for m_data in dict_m_data.values():
                m_data.m_process = dict_m_process[m_data.process_id]
                m_data.m_data_group = dict_m_data_group[m_data.data_group_id]

            for mapping_data in mapping_data_s:
                mapping_data.m_data = dict_m_data[mapping_data.data_id]
        return mapping_data_s

    @classmethod
    def get_data_id(
        cls,
        db_instance: PostgreSQL,
        t_process_id: str = None,
        t_process_name: str = None,
        t_process_abbr: str = None,
        t_data_id: str = None,
        t_data_name: str = None,
        t_data_abbr: str = None,
        data_table_id: int = None,
    ):
        dict_cond = {}
        if t_process_id != '' and t_process_id is not None:
            dict_cond[cls.Columns.t_process_id.name] = [(SqlComparisonOperator.EQUAL, t_process_id)]
        if t_process_name != '' and t_process_name is not None:
            dict_cond[cls.Columns.t_process_name.name] = [(SqlComparisonOperator.EQUAL, t_process_name)]
        if t_process_abbr != '' and t_process_abbr is not None:
            dict_cond[cls.Columns.t_process_abbr.name] = [(SqlComparisonOperator.EQUAL, t_process_abbr)]
        if t_data_id != '' and t_data_id is not None:
            dict_cond[cls.Columns.t_data_id.name] = [(SqlComparisonOperator.EQUAL, t_data_id)]
        if t_data_name != '' and t_data_name is not None:
            dict_cond[cls.Columns.t_data_name.name] = [(SqlComparisonOperator.EQUAL, t_data_name)]
        if t_data_abbr != '' and t_data_abbr is not None:
            dict_cond[cls.Columns.t_data_abbr.name] = [(SqlComparisonOperator.EQUAL, t_data_abbr)]
        if data_table_id != '' and data_table_id is not None:
            dict_cond[cls.Columns.data_table_id.name] = [(SqlComparisonOperator.EQUAL, data_table_id)]

        _, row = cls.select_records(db_instance, dic_conditions=dict_cond, row_is_dict=True, limit=1)
        return row.get(cls.Columns.data_id.name) if row else None

    @classmethod
    def get_data_ids_by_data_table_id(cls, db_instance: PostgreSQL, data_table_id: int):
        dict_cond = {cls.Columns.data_table_id.name: data_table_id}
        cols, rows = cls.select_records(
            db_instance,
            dic_conditions=dict_cond,
            select_cols=[cls.Columns.data_id.name],
            row_is_dict=False,
        )
        return cols, rows

    @classmethod
    def get_by_data_table_ids(cls, db_instance: PostgreSQL, data_table_ids: list[int], row_is_dict=False):
        dict_cond = {cls.Columns.data_table_id.name: [(SqlComparisonOperator.IN, tuple(data_table_ids))]}
        return cls.select_records(db_instance, dic_conditions=dict_cond, row_is_dict=row_is_dict)

    def get_name(self):
        return self.m_data.m_process.get_name() or None

    def get_id(self):
        return self.m_data.m_process.get_id() or None

    @classmethod
    def mapping_process_for_join(cls, primary_groups: PrimaryGroup) -> dict[str, str]:
        return {
            cls.Columns.t_process_id.name: primary_groups.PROCESS_ID,
            cls.Columns.t_process_name.name: primary_groups.PROCESS_NAME,
            cls.Columns.t_process_abbr.name: primary_groups.PROCESS_ABBR,
            cls.Columns.t_data_id.name: primary_groups.DATA_ID,
            cls.Columns.t_data_name.name: primary_groups.DATA_NAME,
            cls.Columns.t_data_abbr.name: primary_groups.DATA_ABBR,
            cls.Columns.t_prod_family_id.name: primary_groups.PROD_FAMILY_ID,
            cls.Columns.t_prod_family_name.name: primary_groups.PROD_FAMILY_NAME,
            cls.Columns.t_prod_family_abbr.name: primary_groups.PROD_FAMILY_ABBR,
            cls.Columns.data_table_id.name: cls.Columns.data_table_id.name,
        }
