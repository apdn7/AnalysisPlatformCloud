from ap.common.common_utils import get_current_timestamp
from ap.common.constants import DataType, JobStatus, JobType
from ap.common.pydn.dblib.db_common import AggregateFunction, OrderBy, SqlComparisonOperator
from bridge.models.bridge_station import OthersDBModel
from bridge.models.model_utils import TableColumn


class FactoryImport(OthersDBModel):
    def __init__(self, dict_proc=None):
        if not dict_proc:
            dict_proc = {}
        self.id = dict_proc.get(FactoryImport.Columns.id.name)
        self.job_id = dict_proc.get(FactoryImport.Columns.job_id.name)

        self.process_id = dict_proc.get(FactoryImport.Columns.process_id.name)
        self.import_type = str(dict_proc.get(FactoryImport.Columns.import_type.name))  # JobType
        self.import_from = dict_proc.get(FactoryImport.Columns.import_from.name)
        self.import_to = dict_proc.get(FactoryImport.Columns.import_to.name)

        self.cycle_start_tm = dict_proc.get(FactoryImport.Columns.cycle_start_tm.name)
        self.cycle_end_tm = dict_proc.get(FactoryImport.Columns.cycle_end_tm.name)
        self.is_duplicate_checked = dict_proc.get(FactoryImport.Columns.is_duplicate_checked.name)

        self.start_tm = dict_proc.get(FactoryImport.Columns.start_tm.name)
        self.end_tm = dict_proc.get(FactoryImport.Columns.end_tm.name)
        self.imported_row = dict_proc.get(FactoryImport.Columns.imported_row.name)
        self.imported_cycle_id = dict_proc.get(FactoryImport.Columns.imported_cycle_id.name)
        self.synced = dict_proc.get(FactoryImport.Columns.synced.name)

        self.status = str(dict_proc.get(FactoryImport.Columns.status.name))  # JobStatus
        self.error_msg = dict_proc.get(FactoryImport.Columns.error_msg.name)

        self.created_at = dict_proc.get(FactoryImport.Columns.created_at.name, get_current_timestamp())
        self.updated_at = dict_proc.get(FactoryImport.Columns.updated_at.name, get_current_timestamp())

    class Columns(TableColumn):
        id = (1, DataType.INTEGER)
        job_id = (2, DataType.INTEGER)

        process_id = (3, DataType.INTEGER)  # etl_table_id in case of master data import
        import_type = (4, DataType.TEXT)
        import_from = (5, DataType.TEXT)
        import_to = (6, DataType.TEXT)

        start_tm = (7, DataType.DATETIME)
        end_tm = (8, DataType.DATETIME)
        imported_row = (9, DataType.INTEGER)
        imported_cycle_id = (10, DataType.INTEGER)

        status = (11, DataType.TEXT)
        error_msg = (12, DataType.TEXT)

        created_at = (13, DataType.DATETIME)
        updated_at = (14, DataType.DATETIME)

        cycle_start_tm = (16, DataType.DATETIME)
        cycle_end_tm = (17, DataType.DATETIME)
        is_duplicate_checked = (18, DataType.BOOLEAN)
        synced = (19, DataType.BOOLEAN)

    _table_name = 't_factory_import'
    primary_keys = [Columns.id]

    @classmethod
    def get_by_proc_id(cls, db_instance, proc_id):
        _, rows = cls.select_records(
            db_instance,
            dic_conditions={cls.Columns.process_id.name: proc_id},
            dic_order_by={cls.Columns.start_tm.name: OrderBy.ASC.name},
        )
        if not rows:
            return []
        return [FactoryImport(row) for row in rows]

    @classmethod
    def get_last_import(cls, db_instance, process_id=None, import_type: JobType = None):
        # dic_conditions = {
        #     cls.Columns.import_type.name: [
        #         (SqlComparisonOperator.IN, tuple([JobType.EFA_IMPORT.name, JobType.V2_IMPORT.name]))],
        #     cls.Columns.status.name: [
        #         (SqlComparisonOperator.IN, tuple([JobStatus.FAILED.name, JobStatus.DONE.name]))],
        # }
        dic_conditions = {
            cls.Columns.status.name: [(SqlComparisonOperator.IN, (JobStatus.FAILED.name, JobStatus.DONE.name))],
        }

        if process_id:
            dic_conditions[cls.Columns.process_id.name] = process_id
        dic_conditions[cls.Columns.synced.name] = True
        if import_type:
            dic_conditions[cls.Columns.import_type.name] = str(import_type)

        dic_order_by = {cls.Columns.imported_cycle_id.name: OrderBy.DESC.name}
        col, rows = cls.select_records(db_instance, dic_conditions=dic_conditions, dic_order_by=dic_order_by, limit=1)
        if not rows:
            return None

        return FactoryImport(rows)

    @classmethod
    def get_first_import(cls, db_instance, process_id, import_type: JobType = None):
        dic_conditions = {
            cls.Columns.process_id.name: process_id,
            cls.Columns.status.name: [(SqlComparisonOperator.IN, (JobStatus.FAILED.name, JobStatus.DONE.name))],
        }
        if import_type:
            dic_conditions[cls.Columns.import_type.name] = str(import_type)

        dic_order_by = {cls.Columns.imported_cycle_id.name: OrderBy.ASC.name}
        _, rows = cls.select_records(db_instance, dic_conditions=dic_conditions, dic_order_by=dic_order_by, limit=1)
        if not rows:
            return None
        return FactoryImport(rows)

    @classmethod
    def insert_history_record(
        cls,
        db_instance,
        job_id,
        process_id,
        job_type,
        import_from,
        import_to,
        imported_row,
        import_status,
        start_tm=None,
        end_tm=None,
        error_msg=None,
    ):
        dict_t_factory_import = {
            FactoryImport.Columns.job_id.name: job_id,
            FactoryImport.Columns.process_id.name: process_id,
            FactoryImport.Columns.import_type.name: str(job_type),
            FactoryImport.Columns.import_from.name: import_from,
            FactoryImport.Columns.import_to.name: import_to,
            FactoryImport.Columns.start_tm.name: start_tm,
            FactoryImport.Columns.end_tm.name: end_tm,
            FactoryImport.Columns.imported_row.name: imported_row,
            FactoryImport.Columns.status.name: str(import_status),
            FactoryImport.Columns.error_msg.name: error_msg,
            FactoryImport.Columns.created_at.name: get_current_timestamp(),
            FactoryImport.Columns.updated_at.name: get_current_timestamp(),
        }
        cls.insert_record(db_instance, dict_t_factory_import, is_return_id=False)

    @classmethod
    def get_done_histories(cls, db_instance, process_id, imported_cycle_id, job_types=None):
        job_statuses = [JobStatus.DONE.name, JobStatus.FAILED.name]
        dic_conditions = {
            cls.Columns.process_id.name: process_id,
            cls.Columns.status.name: [(SqlComparisonOperator.IN, tuple(job_statuses))],
        }
        if job_types:
            dic_conditions[cls.Columns.import_type.name] = [(SqlComparisonOperator.IN, tuple(job_types))]

        dic_order_by = {cls.Columns.imported_cycle_id.name: OrderBy.ASC.name}
        _cycle_id = imported_cycle_id
        if _cycle_id:
            dic_conditions[cls.Columns.imported_cycle_id.name] = [(SqlComparisonOperator.GREATER_THAN, _cycle_id)]

        _, rows = cls.select_records(db_instance, dic_conditions=dic_conditions, dic_order_by=dic_order_by)
        rows = [FactoryImport(dict_record) for dict_record in rows]
        return rows

    @classmethod
    def get_latest_records_by_imported_cycle_id(cls, db_instance, process_id, imported_cycle_id):
        dic_conditions = {
            cls.Columns.process_id.name: process_id,
            cls.Columns.imported_cycle_id.name: [(SqlComparisonOperator.LESS_THAN_OR_EQ, imported_cycle_id)],
        }

        dic_order_by = {cls.Columns.job_id.name: OrderBy.DESC.name}

        _, row = cls.select_records(db_instance, dic_conditions=dic_conditions, dic_order_by=dic_order_by, limit=1)
        return FactoryImport(row)

    @classmethod
    def get_duplicate_check_targets(cls, db_instance):
        dic_conditions = {
            cls.Columns.import_type.name: [
                (
                    SqlComparisonOperator.IN,
                    (JobType.FACTORY_IMPORT.name, JobType.FACTORY_PAST_IMPORT.name),
                ),
            ],
            cls.Columns.status.name: [(SqlComparisonOperator.IN, (JobStatus.FAILED.name, JobStatus.DONE.name))],
            cls.Columns.is_duplicate_checked.name: False,
        }

        dic_order_by = {
            cls.Columns.process_id.name: OrderBy.ASC.name,
            cls.Columns.cycle_start_tm.name: OrderBy.ASC.name,
        }

        _, rows = cls.select_records(db_instance, dic_conditions=dic_conditions, dic_order_by=dic_order_by)
        rows = [FactoryImport(dict_record) for dict_record in rows]
        return rows

    @classmethod
    def get_max_cycle_id(cls, db_instance, process_id):
        dic_conditions = {cls.Columns.process_id.name: process_id}
        dict_aggregate_function = {
            cls.Columns.imported_cycle_id.name: (
                AggregateFunction.MAX.value,
                cls.Columns.imported_cycle_id.name,
            ),
        }
        _, rows = cls.select_records(
            db_instance,
            dic_conditions=dic_conditions,
            dict_aggregate_function=dict_aggregate_function,
            row_is_dict=False,
        )
        max_cycle_id = rows[0][0]
        return max_cycle_id if max_cycle_id else 0
