from ap.common.constants import DataType, JobType
from ap.common.pydn.dblib.db_common import SqlComparisonOperator
from ap.common.pydn.dblib.postgresql import PostgreSQL
from bridge.models.bridge_station import ConfigModel
from bridge.models.model_utils import TableColumn


class CfgPartitionTable(ConfigModel):
    PROGRESS_ORDER = [
        JobType.SCAN_MASTER.name,
        JobType.SCAN_DATA_TYPE.name,
        JobType.USER_APPROVED_MASTER.name,
        JobType.PULL_DB_DATA.name,
    ]  # no need TRANSACTION_CSV_IMPORT here

    class Columns(TableColumn):
        id = (1, DataType.INTEGER)
        data_table_id = (3, DataType.INTEGER)  # type: CfgDataTable.Columns.id
        table_name = (4, DataType.TEXT)
        partition_time = (5, DataType.TEXT)
        min_time = (6, DataType.DATETIME)
        max_time = (7, DataType.DATETIME)
        job_done = (6, DataType.TEXT)
        created_at = (98, DataType.DATETIME)
        updated_at = (99, DataType.DATETIME)

    def __init__(self, dict_partition):
        if not dict_partition:
            dict_partition = {}
        self.id = dict_partition.get(CfgPartitionTable.Columns.id.name)
        if self.id is None:
            del self.id
        self.data_table_id = dict_partition.get(CfgPartitionTable.Columns.data_table_id.name)
        self.table_name = dict_partition.get(CfgPartitionTable.Columns.table_name.name)
        self.partition_time = dict_partition.get(CfgPartitionTable.Columns.partition_time.name)
        self.min_time = dict_partition.get(CfgPartitionTable.Columns.min_time.name)
        self.max_time = dict_partition.get(CfgPartitionTable.Columns.max_time.name)
        self.job_done = dict_partition.get(CfgPartitionTable.Columns.job_done.name)
        self.created_at = dict_partition.get(CfgPartitionTable.Columns.created_at.name)
        self.updated_at = dict_partition.get(CfgPartitionTable.Columns.updated_at.name)

    _table_name = 'cfg_partition_table'
    primary_keys = [Columns.id]

    def is_no_min_max_date_time(self):
        # In case this table is empty (no have any records)
        return self.min_time is None and self.max_time is None

    @classmethod
    def get_partition_for_job(cls, db_instance: PostgreSQL, data_table_id: int, job_type: JobType):
        job_type = str(job_type)
        done_type = None
        if job_type in CfgPartitionTable.PROGRESS_ORDER:
            idx = CfgPartitionTable.PROGRESS_ORDER.index(job_type)
            done_type = CfgPartitionTable.PROGRESS_ORDER[idx - 1] if idx else None

        cfg_partition_tables = cls.get_most_recent_by_type(db_instance, data_table_id, done_type)

        return cfg_partition_tables

    @classmethod
    def get_most_recent_by_type(cls, db_instance: PostgreSQL, data_table_id: int, job_type: JobType = None):
        sql = f'''
SELECT *
FROM {cls.get_table_name()}
WHERE {cls.Columns.data_table_id.name} = {data_table_id}
      AND {cls.Columns.job_done.name} {f"= '{str(job_type)}'" if job_type else 'IS NULL'}
        '''
        cols, rows = db_instance.run_sql(sql, row_is_dict=True)
        return [cls(row) for row in rows]

    @classmethod
    def get_by_data_table_id(cls, db_instance, data_table_id):
        dic_conditions = {cls.Columns.data_table_id.name: data_table_id}
        col, rows = cls.select_records(
            db_instance,
            dic_conditions=dic_conditions,
            row_is_dict=True,
        )
        return rows

    @classmethod
    def delete_not_in_partition_times(cls, db_instance: PostgreSQL, data_table_id: int, partition_times: list):
        cls.delete_by_condition(
            db_instance,
            {
                cls.Columns.data_table_id.name: data_table_id,
                cls.Columns.partition_time.name: [(SqlComparisonOperator.NOT_IN, tuple(partition_times))],
            },
            mode=0,
        )
