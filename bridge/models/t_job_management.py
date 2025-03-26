from typing import Dict

from ap.common.common_utils import get_current_timestamp
from ap.common.constants import DataType, JobStatus
from ap.common.pydn.dblib.db_common import OrderBy, SqlComparisonOperator
from bridge.models.bridge_station import OthersDBModel
from bridge.models.model_utils import TableColumn


class JobManagement(OthersDBModel):
    class Columns(TableColumn):
        id = (1, DataType.INTEGER)
        job_type = (2, DataType.TEXT)
        db_code = (3, DataType.TEXT)
        db_name = (4, DataType.TEXT)
        data_table_id = (15, DataType.INTEGER)
        process_id = (5, DataType.INTEGER)
        process_name = (6, DataType.TEXT)
        start_tm = (7, DataType.TEXT)
        end_tm = (8, DataType.TEXT)
        status = (9, DataType.TEXT)
        done_percent = (10, DataType.REAL)
        duration = (11, DataType.REAL)
        error_msg = (12, DataType.TEXT)
        created_at = (13, DataType.DATETIME)
        updated_at = (14, DataType.DATETIME)

    _table_name = 't_job_management'
    primary_keys = [Columns.id]

    def __init__(self, dict_proc: Dict = None):
        if not dict_proc:
            dict_proc = {}
        anchor_tm = get_current_timestamp()
        self.id = dict_proc.get(JobManagement.Columns.id.name)
        self.job_type = dict_proc.get(JobManagement.Columns.job_type.name)
        self.db_code = dict_proc.get(JobManagement.Columns.db_code.name)
        self.db_name = dict_proc.get(JobManagement.Columns.db_name.name)
        self.process_id = dict_proc.get(JobManagement.Columns.process_id.name)
        self.process_name = dict_proc.get(JobManagement.Columns.process_name.name)
        self.start_tm = dict_proc.get(JobManagement.Columns.start_tm.name, anchor_tm)
        self.end_tm = dict_proc.get(JobManagement.Columns.end_tm.name)
        self.status = dict_proc.get(JobManagement.Columns.status.name)
        self.done_percent = dict_proc.get(JobManagement.Columns.done_percent.name, 0.0)
        self.duration = dict_proc.get(JobManagement.Columns.duration.name, 0.0)
        self.error_msg = dict_proc.get(JobManagement.Columns.error_msg.name)
        self.created_at = dict_proc.get(JobManagement.Columns.created_at.name, anchor_tm)
        self.updated_at = dict_proc.get(JobManagement.Columns.updated_at.name, anchor_tm)

    @classmethod
    def get_by_proc_id(cls, db_instance, proc_id, is_return_dict=False):
        _, rows = cls.select_records(
            db_instance,
            dic_conditions={cls.Columns.process_id.name: proc_id},
            dic_order_by={cls.Columns.start_tm.name: OrderBy.ASC.name},
        )
        if not rows:
            return []
        return rows if is_return_dict else [JobManagement(row) for row in rows]

    @classmethod
    def check_new_jobs(cls, from_job_id, target_job_types):
        pass
        # out = cls.query.options(load_only(cls.id))
        # return out.filter(cls.id > from_job_id).filter(cls.job_type.in_(target_job_types)).first()

    @classmethod
    def get_error_jobs(cls, db_instance, is_return_dict=False):
        # temporarily, FAILED only, maybe put another status in future?
        _, rows = cls.select_records(
            db_instance,
            dic_conditions={cls.Columns.status.name: JobStatus.FAILED.name},
            dic_order_by={cls.Columns.start_tm.name: OrderBy.ASC.name},
        )
        return rows if is_return_dict else [JobManagement(row) for row in rows]

    @classmethod
    def get_new_jobs(cls, db_instance, process_id, from_job_id, is_return_dict=False):
        dic_conditions = {
            cls.Columns.process_id.name: process_id,
            cls.Columns.id.name: [(SqlComparisonOperator.GREATER_THAN_OR_EQ, from_job_id)],
        }

        dic_order_by = {cls.Columns.id.name: OrderBy.ASC.name}
        _, rows = cls.select_records(db_instance, dic_conditions=dic_conditions, dic_order_by=dic_order_by)
        return rows if is_return_dict else [JobManagement(row) for row in rows]

    @classmethod
    def get_last_job_id_by_job_type(cls, db_instance, job_type, data_table_id=None):
        dic_conditions = {cls.Columns.job_type.name: str(job_type)}

        if data_table_id:
            dic_conditions[cls.Columns.data_table_id.name] = data_table_id

        dic_order_by = {cls.Columns.id.name: OrderBy.DESC.name}

        _, rows = cls.select_records(db_instance, dic_conditions=dic_conditions, dic_order_by=dic_order_by, limit=1)
        if not rows:
            return None
        return JobManagement(rows)
