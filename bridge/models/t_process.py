from typing import Dict

from ap.common.constants import DataType
from bridge.models.bridge_station import AbstractProcess
from bridge.models.cfg_process import CfgProcess
from bridge.models.m_process import MProcess
from bridge.models.model_utils import TableColumn


class Process:
    class Columns(TableColumn):
        id = (1, DataType.INTEGER)
        name = (2, DataType.TEXT)
        major_master_id = (3, DataType.INTEGER)
        master_process_id = (4, DataType.INTEGER)  # if import yokomochi , this field is null
        config_process_id = (5, DataType.INTEGER)  # ref to cfg_process.id

    def __init__(self, dict_db_source: Dict):
        self.id = dict_db_source.get(Process.Columns.id.name)
        self.name = dict_db_source.get(Process.Columns.name.name)
        self.config_process_id = dict_db_source.get(Process.Columns.config_process_id.name)
        self.master_process_id = dict_db_source.get(Process.Columns.master_process_id.name)
        self.major_master_id = dict_db_source.get(Process.Columns.major_master_id.name)

    _table_name = 't_process'
    primary_keys = [Columns.id]

    @classmethod
    def get_by_process_name(cls, db_instance, process_name: int):
        dict_process_id = {Process.Columns.name.name: process_name}
        _col, rows = cls.select_records(db_instance, dic_conditions=dict_process_id)
        if not rows:
            return None
        return Process(rows[0])

    @classmethod
    def get_by_process_id(cls, db_instance, process_id: int):
        dict_process_id = {Process.Columns.id.name: process_id}
        _col, rows = cls.select_records(db_instance, dic_conditions=dict_process_id)
        if not rows:
            return None
        return Process(rows[0])

    @classmethod
    def get_by_config_process_id(cls, db_instance, cfg_proc_id: int):
        dict_proc_id = {Process.Columns.config_process_id.name: cfg_proc_id}
        _col, rows = cls.select_records(db_instance, dic_conditions=dict_proc_id)
        if not rows:
            return None
        return [Process(row) for row in rows]

    @classmethod
    def get_by_config_proc_id_proc_name(cls, db_instance, cfg_proc_id: int, mst_proc_id: int):
        dict_process_id = {
            Process.Columns.config_process_id.name: cfg_proc_id,
            Process.Columns.master_process_id.name: mst_proc_id,
        }
        _col, rows = cls.select_records(db_instance, dic_conditions=dict_process_id)
        if not rows:
            return None
        return Process(rows[0])

    # get proc or create new
    @classmethod
    def get_or_create_proc(cls, db_instance, cfg_process_id, process: AbstractProcess):
        proc = None

        if isinstance(process, CfgProcess):
            proc = cls.get_by_config_process_id(db_instance, cfg_process_id)
        if isinstance(process, MProcess):
            proc = cls.get_by_config_proc_id_proc_name(db_instance, cfg_process_id, process.get_id())
        if not proc:
            dict_proc = {
                Process.Columns.name.name: process.get_name(),
                Process.Columns.config_process_id.name: cfg_process_id,
            }
            if isinstance(process, MProcess):
                dict_proc[Process.Columns.master_process_id.name] = process.get_id()

            result_id = cls.insert_record(db_instance, dict_proc, is_return_id=True)

            if result_id:
                dict_proc[Process.Columns.id.name] = result_id
            proc = Process(dict_proc)
        return proc

    @classmethod
    def is_existing_t_process(cls, db_instance, cfg_process_id):
        pm = cls.get_parameter_marker()
        sql = f'SELECT 1 FROM {cls.get_table_name()} WHERE {cls.Columns.config_process_id} = {pm}'
        params = (cfg_process_id,)
        _col, _rows = db_instance.run_sql(sql, params=params, row_is_dict=False)
        return bool(_rows[0][0]) if _rows else False
