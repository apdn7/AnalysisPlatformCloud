from typing import List, Tuple

from ap.common.common_utils import get_current_timestamp
from ap.common.constants import DataType, JobStatus
from ap.common.pydn.dblib.db_common import OrderBy, SqlComparisonOperator
from bridge.models.bridge_station import OthersDBModel
from bridge.models.model_utils import TableColumn


class CsvImport(OthersDBModel):
    class Columns(TableColumn):
        id = (1, DataType.INTEGER)
        job_id = (2, DataType.INTEGER)

        data_table_id = (15, DataType.INTEGER)
        process_id = (3, DataType.INTEGER)
        file_name = (4, DataType.TEXT)

        start_tm = (5, DataType.TEXT)
        end_tm = (6, DataType.TEXT)
        imported_row = (7, DataType.INTEGER)
        status = (8, DataType.TEXT)
        error_msg = (9, DataType.TEXT)

        created_at = (10, DataType.DATETIME)
        updated_at = (11, DataType.DATETIME)

    _table_name = 't_csv_import'
    primary_keys = [Columns.id]

    def __init__(self, dict_proc=None):
        if not dict_proc:
            dict_proc = {}

        self.id = dict_proc.get(CsvImport.Columns.id.name)
        self.job_id = dict_proc.get(CsvImport.Columns.job_id.name)

        self.process_id = dict_proc.get(CsvImport.Columns.process_id.name)
        self.file_name = str(dict_proc.get(CsvImport.Columns.file_name.name))  # JobType

        self.start_tm = dict_proc.get(CsvImport.Columns.start_tm.name)
        self.end_tm = dict_proc.get(CsvImport.Columns.end_tm.name)
        self.status = dict_proc.get(CsvImport.Columns.status.name)
        self.imported_row = dict_proc.get(CsvImport.Columns.imported_row.name)
        self.error_msg = dict_proc.get(CsvImport.Columns.error_msg.name)

        self.created_at = dict_proc.get(CsvImport.Columns.created_at.name, get_current_timestamp())
        self.updated_at = dict_proc.get(CsvImport.Columns.updated_at.name, get_current_timestamp())

    @classmethod
    def get_last_import(cls, db_instance, process_id, is_first_id=False):
        dic_conditions = {
            cls.Columns.process_id.name: process_id,
            cls.Columns.status.name: JobStatus.DONE.name,
        }
        dic_order_by = {cls.Columns.id.name: OrderBy.ASC.name if is_first_id else OrderBy.DESC.name}
        sql, params = cls.select_records(db_instance, dic_conditions=dic_conditions, dic_order_by=dic_order_by, limit=1)
        _, rows = db_instance.run_sql(sql, params=params)
        if not rows:
            return None

        return CsvImport(rows[0])

    @classmethod
    def get_by_job_id(cls, db_instance, job_id):
        dic_conditions = {cls.Columns.job_id.name: job_id}

        _, rows = cls.select_records(db_instance, dic_conditions=dic_conditions)

        return rows if rows else None

    @classmethod
    def get_in_job_ids(cls, db_instance, job_ids: [List, Tuple]):
        dic_conditions = {cls.Columns.job_id.name: [(SqlComparisonOperator.IN, tuple(job_ids))]}

        _, rows = cls.select_records(db_instance, dic_conditions=dic_conditions)

        return rows if rows else []

    @classmethod
    def get_error_jobs(cls, db_instance, job_id, is_return_cols=False, row_is_dict=True):
        dic_conditions = {
            cls.Columns.job_id.name: job_id,
            cls.Columns.status.name: [(SqlComparisonOperator.NOT_EQUAL, JobStatus.DONE.name)],
        }

        cols, rows = cls.select_records(db_instance, dic_conditions=dic_conditions, row_is_dict=row_is_dict)

        if is_return_cols:
            return cols, rows
        return rows if rows else None

    @classmethod
    def get_by_process_id(cls, db_instance, process_id):
        dic_conditions = {cls.Columns.process_id.name: process_id}

        _, rows = cls.select_records(db_instance, dic_conditions=dic_conditions)

        return rows if rows else None

    @classmethod
    def get_by_job_id_and_proc_id(cls, db_instance, job_id, process_id):
        dic_conditions = {
            cls.Columns.job_id.name: job_id,
            cls.Columns.process_id.name: process_id,
        }

        dic_order_by = {cls.Columns.id.name: OrderBy.DESC.name}

        _, rows = cls.select_records(db_instance, dic_conditions=dic_conditions, dic_order_by=dic_order_by, limit=1)

        if not rows:
            return None
        return CsvImport(rows)

    @classmethod
    def get_latest_done_files(cls, db_instance, process_id):
        pm = cls.get_parameter_marker()
        sql = f'''SELECT file_name, max(start_tm) as start_tm, max(imported_row) as imported_row
            FROM {cls.get_table_name()}
            WHERE status IN ("DONE", "FAILED") AND process_id = {pm}
            GROUP BY file_name'''

        _, rows = db_instance.run_sql(sql, row_is_dict=True, params=(process_id,))
        return rows

    @classmethod
    def get_last_fatal_import(cls, db_instance, process_id):
        # todo:  get last job
        pm = cls.get_parameter_marker()
        sql = f'''SELECT file_name, max(start_tm) as start_tm, max(imported_row) as imported_row
            FROM {cls.get_table_name()}
            WHERE status IN ("FATAL", "PROCESSING") AND process_id = {pm}
            GROUP BY file_name'''

        _, rows = db_instance.run_sql(sql, row_is_dict=True, params=(process_id,))
        return rows
