from typing import List, Union

from sqlalchemy.orm import scoped_session

from ap import get_file_mode
from ap.common.common_utils import get_column_order
from ap.common.constants import DataGroupType, DataType, JobType
from ap.common.logger import logger
from ap.common.pydn.dblib.db_common import OrderBy
from ap.common.pydn.dblib.postgresql import PostgreSQL
from bridge.models.bridge_station import BridgeStationModel, ConfigModel
from bridge.models.cfg_data_table_column import CfgDataTableColumn
from bridge.models.m_data import MData
from bridge.models.m_data_group import MDataGroup
from bridge.models.mapping_factory_machine import MappingFactoryMachine
from bridge.models.model_utils import TableColumn
from bridge.models.r_factory_machine import RFactoryMachine
from bridge.services.utils import get_master_type


class CfgDataTable(ConfigModel):
    class Columns(TableColumn):
        id = (1, DataType.INTEGER)
        name = (2, DataType.TEXT)
        data_source_id = (3, DataType.INTEGER)  # type: CfgDataSource.Columns.id
        table_name = (4, DataType.TEXT)
        detail_master_type = (9, DataType.TEXT)
        comment = (5, DataType.TEXT)
        partition_from = (6, DataType.TEXT)
        partition_to = (7, DataType.TEXT)

        order = (8, DataType.INTEGER)
        skip_merge = (9, DataType.BOOLEAN)

        created_at = (98, DataType.DATETIME)
        updated_at = (99, DataType.DATETIME)

    def __init__(self, dict_proc=None, is_cascade: bool = False, db_instance: PostgreSQL = None):
        dict_proc = dict_proc if dict_proc else {}
        self.id = dict_proc.get(self.Columns.id.name)
        if self.id is None:
            del self.id
        self.name = dict_proc.get(self.Columns.name.name)
        self.data_source_id = dict_proc.get(self.Columns.data_source_id.name)
        self.table_name = dict_proc.get(self.Columns.table_name.name)
        self.detail_master_type = dict_proc.get(self.Columns.detail_master_type.name)
        self.comment = dict_proc.get(self.Columns.comment.name)
        self.partition_from = dict_proc.get(self.Columns.partition_from.name)
        self.partition_to = dict_proc.get(self.Columns.partition_to.name)
        self.order = dict_proc.get(self.Columns.order.name)
        self.skip_merge = dict_proc.get(self.Columns.skip_merge.name, False)
        self.created_at = dict_proc.get(self.Columns.created_at.name)
        self.updated_at = dict_proc.get(self.Columns.updated_at.name)
        self.data_source = None  # type: [CfgDataSourceDB, CfgDataSourceCSV]
        self.columns = []  # type:  List[CfgDataTableColumn]

        if not is_cascade:
            return

        @BridgeStationModel.use_db_instance(db_instance_argument_name='_db_instance')
        def _get_relation_data_(_self, _db_instance: PostgreSQL = None):
            from bridge.models.cfg_data_source import CfgDataSource

            _self.data_source = CfgDataSource(
                CfgDataSource.get_by_id(_db_instance, _self.data_source_id),
                is_cascade=is_cascade,
                db_instance=_db_instance,
            )
            _self.columns.extend(CfgDataTableColumn.get_by_data_table_id(_db_instance, _self.id))

        _get_relation_data_(self, _db_instance=db_instance)

    _table_name = 'cfg_data_table'
    primary_keys = [Columns.id]

    @classmethod
    def get_by_id(cls, db_instance, data_table_id: int, is_cascade: bool = False):
        dict_conditions = {CfgDataTable.Columns.id.name: data_table_id}
        _col, rows = cls.select_records(db_instance, dic_conditions=dict_conditions)
        if not rows:
            return None
        cfg_data_table = CfgDataTable(rows[0], is_cascade=is_cascade, db_instance=db_instance)
        if cfg_data_table.columns is None:
            cfg_data_table.columns = []

        if is_cascade:
            # parent process's columns has column type, meanwhile generated process column has no column type
            _by_data_group_id = [col.data_group_type for col in cfg_data_table.columns if col.data_group_type]
            if _by_data_group_id:
                dict_m_data_groups = MDataGroup.get_in_ids(db_instance, _by_data_group_id, is_return_dict=True)
                for col in cfg_data_table.columns:
                    # because GUI created process has no m_data, set m_data_group first.
                    col.m_data_group = dict_m_data_groups[col.data_group_type] if col.data_group_type else None

            m_data_s = MData.get_by_process_id(db_instance, data_table_id, is_cascade=True)
            for col in cfg_data_table.columns:
                temp_tuple = (m_data for m_data in m_data_s if m_data.m_data_group.get_sys_name() == col.english_name)
                col.m_data = next(temp_tuple, None)  # get first item or None
                if col.m_data and not col.m_data_group:
                    col.m_data_group = col.m_data.m_data_group
        return cfg_data_table

    @classmethod
    def get_last_id(cls, db_instance):
        select_cols = [cls.Columns.id.name]
        dic_order_by = {cls.Columns.id.name: OrderBy.ASC.name}
        _, rows = cls.select_records(db_instance, select_cols=select_cols, dic_order_by=dic_order_by, limit=1)
        if not rows:
            return None
        return rows['id']

    @classmethod
    def get_by_process_id(cls, db_instance: Union[PostgreSQL, scoped_session], process_id: int):
        sql = f'''
SELECT DISTINCT
    cds.{CfgDataTable.Columns.id.name}
    , cds.{CfgDataTable.Columns.name.name}
    , cds.{CfgDataTable.Columns.data_source_id.name}
    , cds.{CfgDataTable.Columns.partition_from.name}
    , cds.{CfgDataTable.Columns.partition_to.name}
    , cds.{CfgDataTable.Columns.table_name.name}
    , cds.{CfgDataTable.Columns.comment.name}
    , cds.{CfgDataTable.Columns.order.name}
    , cds.{CfgDataTable.Columns.created_at.name}
    , cds.{CfgDataTable.Columns.updated_at.name}
FROM {CfgDataTable.get_table_name()} cds
INNER JOIN {MappingFactoryMachine.get_table_name()} mdm ON
    mdm.{MappingFactoryMachine.Columns.data_table_id.name} = cds.{CfgDataTable.Columns.id.name}
INNER JOIN {RFactoryMachine.get_table_name()} rfm ON
    rfm.{RFactoryMachine.Columns.id.name} = mdm.{MappingFactoryMachine.Columns.factory_machine_id.name}
WHERE
    rfm.{RFactoryMachine.Columns.process_id.name} = {cls.get_parameter_marker()}
        '''
        if isinstance(db_instance, scoped_session):
            sql.replace(cls.get_parameter_marker(), ':1')
            params = {'1': process_id}
            rows = db_instance.execute(sql, params=params)
            cols = [
                CfgDataTable.Columns.id.name,
                CfgDataTable.Columns.name.name,
                CfgDataTable.Columns.data_source_id.name,
                CfgDataTable.Columns.partition_from.name,
                CfgDataTable.Columns.partition_to.name,
                CfgDataTable.Columns.table_name.name,
                CfgDataTable.Columns.comment.name,
                CfgDataTable.Columns.order.name,
                CfgDataTable.Columns.created_at.name,
                CfgDataTable.Columns.updated_at.name,
            ]
            rows = [dict(zip(cols, row)) for row in rows]
        else:
            params = [process_id]
            _, rows = db_instance.run_sql(sql, params=params)

        return rows

    def get_date_col(self, column_name_only=True):
        """
        get date column
        :param column_name_only:
        :return:
        """
        cols = [col for col in self.columns if col.data_group_type == DataGroupType.DATA_TIME.value]
        if cols:
            if column_name_only:
                return cols[0].column_name

            return cols[0]

        return None

    def get_columns(self, column_name_only=False) -> Union[List[str], List[CfgDataTableColumn]]:
        if not self.columns:
            logger.warning('No any column. Get cfg_process cascade or assign columns to this process')
        cols = [cfg_col.column_name for cfg_col in self.columns] if column_name_only else self.columns
        return cols

    def get_col_by_data_group_type(self, db_instance, data_group_type: DataGroupType):
        """
        get date column
        :param data_type:
        :param column_name_only:
        :return:
        """
        dic_conditions = {MDataGroup.Columns.data_group_type.name: data_group_type}
        select_cols = [MDataGroup.Columns.id.name]
        cols, m_data_group = MDataGroup.select_records(db_instance, dic_conditions, select_cols, limit=1)
        if not m_data_group:
            raise Exception(f'{data_group_type} not found in {MDataGroup._table_name}')
        cols = [col for col in self.columns if col.data_group_type == m_data_group[MDataGroup.Columns.id.name]]
        return cols[0] if cols else None

    def get_cols_by_data_type(self, data_type: DataType, column_name_only=True):
        """
        get date column
        :param data_type:
        :param column_name_only:
        :return:
        """
        if column_name_only:
            cols = [col.column_name for col in self.columns if col.data_type == data_type.name]
        else:
            cols = [col for col in self.columns if col.data_type == data_type.name]

        return cols

    def get_serials(self, column_name_only=True):
        columns = self.get_columns(False)  # type: List[CfgDataTableColumn]
        if column_name_only:
            cols = [cfg_col.column_name for cfg_col in columns if cfg_col.is_serial_no]  # type: List[str]
        else:
            cols = [cfg_col for cfg_col in columns if cfg_col.is_serial_no]  # type: List[CfgDataTableColumn]

        return cols

    def get_auto_increment_col(self, column_name_only=True):
        """
        get auto increment column
        :param column_name_only:
        :return:
        """
        cols = [col for col in self.columns if col.data_group_type == DataGroupType.AUTO_INCREMENTAL.value]
        if cols:
            if column_name_only:
                return cols[0].column_name

            return cols[0]

        return None

    def get_master_type(self):
        return get_master_type(
            self.data_source.master_type,
            table_name=self.table_name,
            column_names=[col.column_name for col in self.columns],
        )

    def is_has_serial_col(self):
        cols = self.columns
        return any(col.data_group_type == DataGroupType.DATA_SERIAL.value for col in cols)

    def get_sorted_columns(self):
        self.columns.sort(key=lambda c: (str(c.data_type), str(c.column_name)))
        self.columns.sort(key=lambda c: get_column_order(c.data_group_type))
        return self.columns

    def get_auto_increment_col_else_get_date(self, column_name_only=True):
        """
        get auto increment column
        :param column_name_only:
        :return:
        """
        return self.get_auto_increment_col(column_name_only) or self.get_date_col(column_name_only) or None

    def is_has_auto_increment_col(self):
        return bool(self.get_auto_increment_col_else_get_date())

    @classmethod
    def get_by_data_source_id(cls, db_instance: PostgreSQL, data_source_id: int, is_cascade: bool = False):
        """
        get all data tables by data source id
        :param db_instance: a database instance
        :param data_source_id: data source id
        :param is_cascade: is cascade
        :return:
        """
        dic_conditions = {CfgDataTable.Columns.data_source_id.name: data_source_id}
        _, rows = CfgDataTable.select_records(db_instance, dic_conditions)
        if not rows:
            return []

        return [CfgDataTable(row, db_instance=db_instance, is_cascade=is_cascade) for row in rows]

    def is_export_file(self):
        file_mode = get_file_mode()
        is_direct_import = self.data_source.is_direct_import
        return file_mode and not is_direct_import

    @BridgeStationModel.use_db_instance()
    def get_partition_for_job(self, job_type: JobType, many=None, db_instance: PostgreSQL = None):
        from bridge.models.cfg_partition_table import CfgPartitionTable

        job_type = str(job_type)
        done_type = None
        if job_type in CfgPartitionTable.PROGRESS_ORDER:
            idx = CfgPartitionTable.PROGRESS_ORDER.index(job_type)
            done_type = CfgPartitionTable.PROGRESS_ORDER[idx - 1] if idx else None  # nếu idx = 0 thì cũng None
        cfg_partition_table = CfgPartitionTable.get_most_recent_by_type(db_instance, self.id, done_type)
        return cfg_partition_table if many else next(cfg_partition_table, None)
