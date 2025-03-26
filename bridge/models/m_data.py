from typing import List

from ap.common.common_utils import get_list_attr
from ap.common.constants import RawDataTypeDB
from ap.common.pydn.dblib.db_common import SqlComparisonOperator
from ap.common.pydn.dblib.postgresql import PostgreSQL
from bridge.models.bridge_station import MasterModel
from bridge.models.cfg_process_function_column import CfgProcessFunctionColumn
from bridge.models.m_data_group import MDataGroup
from bridge.models.m_function import MFunction
from bridge.models.m_process import MProcess
from bridge.models.m_unit import MUnit
from bridge.models.model_utils import TableColumn


class MData(MasterModel):
    class Columns(TableColumn):
        id = (1, RawDataTypeDB.INTEGER)
        process_id = (2, RawDataTypeDB.INTEGER)
        data_group_id = (3, RawDataTypeDB.INTEGER)
        data_type = (4, RawDataTypeDB.TEXT)
        unit_id = (5, RawDataTypeDB.INTEGER)
        config_equation_id = (6, RawDataTypeDB.INTEGER)
        data_factid = (7, RawDataTypeDB.TEXT)
        is_hide = (8, RawDataTypeDB.BOOLEAN)
        created_at = (97, RawDataTypeDB.DATETIME)
        updated_at = (98, RawDataTypeDB.DATETIME)

    def __init__(self, dict_proc=None):
        dict_proc = dict_proc if dict_proc else {}
        self.id = dict_proc.get(MData.Columns.id.name)
        self.process_id = dict_proc.get(MData.Columns.process_id.name)
        self.data_group_id = dict_proc.get(MData.Columns.data_group_id.name)
        self.data_type = dict_proc.get(MData.Columns.data_type.name)
        self.unit_id = dict_proc.get(MData.Columns.unit_id.name)
        self.config_equation_id = dict_proc.get(MData.Columns.config_equation_id.name)
        self.data_factid = dict_proc.get(MData.Columns.data_factid.name)
        self.is_hide = dict_proc.get(MData.Columns.is_hide.name)
        self.m_process: MProcess = None
        self.m_data_group: MDataGroup = None
        self.m_unit: MUnit = None
        self.cfg_process_function_column: CfgProcessFunctionColumn = None

    _table_name = 'm_data'
    primary_keys = [Columns.id]
    not_null_columns = [Columns.process_id, Columns.data_group_id]
    # message_cls = MsgMPart

    @classmethod
    def get_by_id(cls, db_instance, m_data_id: int, is_cascade=False):
        condition = {MData.Columns.id.name: m_data_id}
        _col, rows = cls.select_records(db_instance, condition, row_is_dict=True)
        if not rows:
            return None

        m_data = MData(rows[0])

        if is_cascade:
            data_group_id, unit_id = m_data.data_group_id, m_data.unit_id
            process_id, equation_id = m_data.process_id, m_data.config_equation_id

            m_process = MProcess.get_by_id(db_instance, process_id)
            m_data_groups = MDataGroup.get_in_ids(db_instance, [data_group_id], is_return_dict=False)
            m_units = MUnit.get_in_ids(db_instance, [unit_id], is_return_dict=False)
            m_equations = CfgProcessFunctionColumn.get_in_ids(db_instance, [equation_id], is_return_dict=False)

            m_data.m_process = m_process
            m_data.m_data_group = m_data_groups[0] if m_data_groups else None
            m_data.m_unit = m_units[0] if m_units else None
            m_data.cfg_process_function_column = m_equations[0] if m_equations else None

            if m_data.cfg_process_function_column:
                dict_m_function = MFunction.get_all(db_instance, is_return_dict=True)
                m_data.cfg_process_function_column.m_function = dict_m_function.get(
                    m_data.cfg_process_function_column.function_id,
                )

        return m_data

    @classmethod
    def get_all(cls, db_instance, is_cascade=False):
        _col, rows = cls.select_records(db_instance, row_is_dict=True)
        m_data_s = [MData(row) for row in rows]

        if is_cascade:
            cols, rows = MProcess.select_records(db_instance)
            dict_m_process = {row['id']: MProcess(row) for row in rows}

            cols, rows = MDataGroup.select_records(db_instance)
            dict_m_data_groups = {row['id']: MDataGroup(row) for row in rows}

            for m_data in m_data_s:
                m_data.m_process = dict_m_process[m_data.process_id]
                m_data.m_data_group = dict_m_data_groups[m_data.data_group_id]

        return m_data_s

    @classmethod
    def get_by_process_id(cls, db_instance, process_id: int, is_cascade=False, is_return_dict=False):
        condition = {MData.Columns.process_id.name: process_id}
        _col, rows = cls.select_records(db_instance, condition, row_is_dict=True)
        m_data_s = [MData(row) for row in rows]

        if m_data_s and is_cascade:
            data_group_ids, unit_ids, _, equation_ids = MData._get_unique_ids(m_data_s)

            m_process = MProcess.get_by_id(db_instance, process_id)
            dict_m_data_groups = MDataGroup.get_in_ids(db_instance, data_group_ids, is_return_dict=True)
            dict_m_units = MUnit.get_in_ids(db_instance, unit_ids, is_return_dict=True)
            dict_m_equation = CfgProcessFunctionColumn.get_in_ids(db_instance, equation_ids, is_return_dict=True)
            dict_m_function = MFunction.get_all(db_instance, is_return_dict=True)

            for m_data in m_data_s:
                m_data.m_process = m_process
                m_data.m_data_group = dict_m_data_groups[m_data.data_group_id]
                m_data.m_unit = dict_m_units[m_data.unit_id]
                m_data.cfg_process_function_column = dict_m_equation.get(m_data.config_equation_id)
                if m_data.cfg_process_function_column:
                    m_data.cfg_process_function_column['function_type'] = dict_m_function.get(
                        m_data.cfg_process_function_column.get('function_id'),
                    ).function_type

        if is_return_dict:
            return {m_data.id: m_data for m_data in m_data_s}
        return m_data_s

    @classmethod
    def get_in_process_ids(cls, db_instance, process_ids: List, is_cascade=False, is_return_dict=False):
        if process_ids is None or not len(process_ids):
            return []
        process_ids = [str(proc_id) for proc_id in process_ids]

        dict_condition = {MData.Columns.process_id.name: [(SqlComparisonOperator.IN, tuple(process_ids))]}
        _col, rows = cls.select_records(db_instance, dict_condition, row_is_dict=True, filter_deleted=False)
        m_data_s = [MData(row) for row in rows]
        if m_data_s and is_cascade:
            data_group_ids, unit_ids, _, equation_ids = MData._get_unique_ids(m_data_s)

            dict_m_process_s = MProcess.get_in_ids(db_instance, process_ids, is_return_dict=True)
            dict_m_data_groups = MDataGroup.get_in_ids(db_instance, data_group_ids, is_return_dict=True)
            dict_m_units = MUnit.get_in_ids(db_instance, unit_ids, is_return_dict=True)
            dict_m_equation = CfgProcessFunctionColumn.get_in_ids(db_instance, equation_ids, is_return_dict=True)
            dict_m_function = MFunction.get_all(db_instance, is_return_dict=True)

            for m_data in m_data_s:
                m_data.m_process = dict_m_process_s[m_data.process_id]
                m_data.m_data_group = dict_m_data_groups[m_data.data_group_id]
                m_data.m_unit = dict_m_units.get(m_data.unit_id)
                m_data.cfg_process_function_column = dict_m_equation.get(m_data.config_equation_id)
                if m_data.cfg_process_function_column:
                    m_data.cfg_process_function_column.m_function = dict_m_function.get(
                        m_data.cfg_process_function_column,
                    )

        if is_return_dict:
            return {m_data.id: m_data for m_data in m_data_s}
        return m_data_s

    @classmethod
    def get_in_data_group_ids(cls, db_instance, data_group_ids: List, is_cascade=False, is_return_dict=False):
        dict_condition = {MData.Columns.data_group_id.name: [(SqlComparisonOperator.IN, tuple(data_group_ids))]}
        _col, rows = cls.select_records(db_instance, dict_condition, row_is_dict=True)
        m_data_s = [MData(row) for row in rows]
        if m_data_s and is_cascade:
            _, unit_ids, process_ids, equation_ids = MData._get_unique_ids(m_data_s)

            dict_m_data_groups = MDataGroup.get_in_ids(db_instance, data_group_ids, is_return_dict=True)
            dict_m_units = MUnit.get_in_ids(db_instance, unit_ids, is_return_dict=True)
            dict_m_process_s = MProcess.get_in_ids(db_instance, process_ids, is_return_dict=True)
            dict_m_equation = CfgProcessFunctionColumn.get_in_ids(db_instance, equation_ids, is_return_dict=True)
            dict_m_function = MFunction.get_all(db_instance, is_return_dict=True)

            for m_data in m_data_s:
                m_data.m_process = dict_m_process_s[m_data.process_id]
                m_data.m_data_group = dict_m_data_groups[m_data.data_group_id]
                m_data.m_unit = dict_m_units[m_data.unit_id]
                m_data.cfg_process_function_column = dict_m_equation.get(m_data.config_equation_id)
                if m_data.cfg_process_function_column:
                    m_data.cfg_process_function_column.m_function = dict_m_function.get(
                        m_data.cfg_process_function_column,
                    )

        if is_return_dict:
            return {m_data.id: m_data for m_data in m_data_s}
        return m_data_s

    @classmethod
    def get_by_process_id_and_data_group_id(cls, db_instance, process_id: int, data_group_id: int):
        condition = {
            MData.Columns.process_id.name: process_id,
            MData.Columns.data_group_id.name: data_group_id,
        }
        _col, rows = cls.select_records(db_instance, condition, row_is_dict=True)
        m_data_s = [MData(row) for row in rows]
        return m_data_s[0]

    @classmethod
    def get_process_ids(cls, db_instance, data_ids: list[int]) -> list[int]:
        condition = {
            MData.Columns.id.name: [(SqlComparisonOperator.IN, tuple(data_ids))],
        }
        select_cols = [cls.Columns.process_id.name]
        _, rows = cls.select_records(db_instance, condition, select_cols=select_cols, row_is_dict=False)
        if rows:
            process_ids = list({process_id for process_id, *_ in rows})
            return process_ids
        return []

    @staticmethod
    def _get_unique_ids(m_data_s):
        return (
            get_list_attr(m_data_s, 'data_group_id'),
            get_list_attr(m_data_s, 'unit_id'),
            get_list_attr(m_data_s, 'process_id'),
            get_list_attr(m_data_s, 'config_equation_id'),
        )

    @classmethod
    def get_in_data_group_ids_and_process(
        cls,
        db_instance: PostgreSQL,
        process_id: int,
        data_group_ids: list[int],
        is_return_object: bool = False,
    ):
        condition = {
            MData.Columns.process_id.name: process_id,
            MData.Columns.data_group_id.name: [(SqlComparisonOperator.IN, tuple(data_group_ids))],
        }
        select_cols = [col.name for col in cls.Columns]
        _, rows = cls.select_records(db_instance, condition, select_cols=select_cols, row_is_dict=True)

        if is_return_object:
            return [cls(row) for row in rows]
        else:
            return rows

    @classmethod
    def hide_col_by_ids(cls, db_instance: PostgreSQL, data_ids: list[int], is_hide: bool = True):
        if not data_ids:
            return

        cls.update_by_conditions(
            db_instance,
            {cls.Columns.is_hide.name: is_hide},
            dic_conditions={
                MData.Columns.id.name: [(SqlComparisonOperator.IN, tuple(data_ids))],
            },
        )

    @classmethod
    def hide_col_by_process_id(cls, db_instance: PostgreSQL, process_id: int, is_hide: bool = True):
        if not process_id:
            return

        cls.update_by_conditions(
            db_instance,
            {cls.Columns.is_hide.name: is_hide},
            dic_conditions={
                MData.Columns.process_id.name: process_id,
            },
        )
