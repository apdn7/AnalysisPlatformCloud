import itertools
from typing import Optional, Union

import pandas as pd
import sqlalchemy as sa
from pandas import DataFrame

from ap import log_execution_time
from ap.api.setting_module.services.autolink import AUTO_LINK_ID, DATE, SERIAL, AutoLinkData
from ap.common.common_utils import convert_time, read_feather_file
from ap.common.constants import (
    INDEX_COL,
    JOB_ID,
    DataGroupType,
    JobStatus,
    JobType,
    RawDataTypeDB,
    TransactionForPurpose,
)
from ap.common.pydn.dblib.db_proxy_readonly import ReadOnlyDbProxy
from ap.common.pydn.dblib.postgresql import PostgreSQL
from ap.setting_module.models import CfgConstant, CfgDataTable
from ap.setting_module.services.background_process import JobInfo, send_processing_info
from bridge.models.bridge_station import BridgeStationModel
from bridge.models.cfg_process import CfgProcess
from bridge.models.m_data_group import get_primary_group
from bridge.models.mapping_factory_machine import MappingFactoryMachine as BSMappingFactoryMachine
from bridge.models.r_factory_machine import RFactoryMachine as BSRFactoryMachine
from bridge.models.t_auto_link import AutoLink
from bridge.models.transaction_model import TransactionData
from bridge.services.data_import import convert_csv_timezone_per_process, convert_datetime_format, get_import_files
from bridge.services.etl_services.etl_controller import ETLController
from bridge.services.etl_services.etl_csv_service import EtlCsvService
from bridge.services.etl_services.etl_db_long_service import DBLongService
from bridge.services.etl_services.etl_db_service import EtlDbService, get_n_save_partition_range_time_from_factory_db
from bridge.services.etl_services.etl_efa_service import EFAService, get_factory_master_data
from bridge.services.etl_services.etl_import import (
    check_latest_trans_data,
    convert_db_timezone,
    loop_join_master_data_for_import,
)
from bridge.services.etl_services.etl_service import ETLService
from bridge.services.etl_services.etl_software_workshop_services import SoftwareWorkshopService
from bridge.services.etl_services.etl_v2_history_service import V2HistoryService
from bridge.services.etl_services.etl_v2_measure_service import V2MeasureService
from bridge.services.etl_services.etl_v2_multi_history_service import V2MultiHistoryService
from bridge.services.etl_services.etl_v2_multi_measure_service import V2MultiMeasureService
from bridge.services.sql.utils import df_from_query


class PullForAutoLink:
    LIMIT_LATEST_FILE_COUNT = 3
    LIMIT_RECORD_COUNT = 100_000
    LIMIT_DAY_COUNT = 365

    class Log:
        LOG_PREFIX = '[PullForAutoLink]'

        @classmethod
        def log_execution_time(cls):
            return log_execution_time(prefix=cls.LOG_PREFIX)

    def __init__(self, auto_link_data: AutoLinkData):
        self.auto_link_data = auto_link_data
        self.processes: set[int] = set()
        self.data_table_ids: set[int] = set()

        self.dict_process_data_table_ids: dict[int, list[int]] = {}
        self.update_processes()

        self.pulled_df = pd.DataFrame(columns=[AUTO_LINK_ID, SERIAL, DATE])

    @BridgeStationModel.use_db_instance()
    def collect_params(self, db_instance: PostgreSQL = None):
        rows = BSRFactoryMachine.get_all_data_table_id_with_process_id(db_instance)
        for row in rows:
            process_id = row.get(BSRFactoryMachine.Columns.process_id.name)
            data_table_id = row.get(BSMappingFactoryMachine.Columns.data_table_id.name)
            if process_id not in self.processes:
                continue
            if process_id not in self.dict_process_data_table_ids:
                self.dict_process_data_table_ids[process_id] = []
            self.dict_process_data_table_ids[process_id].append(data_table_id)
            self.data_table_ids.add(data_table_id)

    def update_processes(self) -> None:
        """Update data table ids again to remove unneeded ones"""
        self.processes = {data.process_id for data in self.auto_link_data.data_process_for_update}
        data_table_ids_from_processes = set()
        for process in self.processes:
            data_table_ids = self.dict_process_data_table_ids.get(process) or []
            data_table_ids_from_processes.update(data_table_ids)
        self.data_table_ids = self.data_table_ids & data_table_ids_from_processes

    @Log.log_execution_time()
    @BridgeStationModel.use_db_instance()
    def get_data_from_source(self, db_instance: PostgreSQL = None) -> None:
        # TODO: Collect data for process
        # Step 1 - get record in table t_auto_link
        # Step 2
        #   - If total record is not enough, read in t_process_... table to get more records
        #   - Else go to step 4
        # Step 3
        #   - If total record is still not enough
        #       - If all files were read and pulled completely -> read in feather files to get more records
        #       - Else -> read unread latest files to get more records in real data source
        #   - Else do nothing

        self.collect_params(db_instance=db_instance)
        self.pull_data_table(db_instance=db_instance)
        self.update_auto_link_data(db_instance=db_instance)

        # update pulled dataframe into auto link data
        self.auto_link_data.update(self.pulled_df)

    @Log.log_execution_time()
    @BridgeStationModel.use_db_instance()
    def pull_data_table(self, db_instance: PostgreSQL = None):
        def handler_func(_self: PullForAutoLink, _job_info, _db_instance):
            yield 0

            # get job_id
            dic_job_id = {}
            yield dic_job_id
            _job_info.job_id = dic_job_id.get(JOB_ID)
            for idx, data_table_id in enumerate(_self.data_table_ids):
                CfgConstant.force_running_job()

                # TODO: Change all alchemy model to bridge model
                cfg_data_table: CfgDataTable = CfgDataTable.get_by_id(data_table_id)
                etl: Optional[ETLService] = ETLController.get_etl_service(cfg_data_table, db_instance=db_instance)

                if isinstance(
                    etl,
                    (EtlCsvService, V2HistoryService, V2MeasureService, V2MultiHistoryService, V2MultiMeasureService),
                ):
                    _self.pull_files(etl, _job_info.job_type, db_instance=_db_instance)
                elif isinstance(etl, (DBLongService, EFAService, EtlDbService, SoftwareWorkshopService)):
                    _self.pull_tables(etl, _job_info.job_type, db_instance=_db_instance)

                yield int((idx + 1) / len(_self.data_table_ids) * 100)

            job_info.status = JobStatus.DONE
            yield job_info

        job_info = JobInfo()
        job_type = JobType.PULL_FOR_AUTO_LINK
        job_info.job_type = job_type
        generator = handler_func(self, job_info, db_instance)
        send_processing_info(generator, job_type)

    @Log.log_execution_time()
    @BridgeStationModel.use_db_instance()
    def pull_files(
        self,
        etl: Optional[
            Union[
                EtlCsvService,
                V2MeasureService,
                V2MultiMeasureService,
                V2HistoryService,
                V2MultiHistoryService,
            ]
        ],
        job_type: JobType,
        db_instance: PostgreSQL = None,
    ):
        if etl is None:
            raise NotImplementedError

        for df, *_ in etl.get_transaction_data(
            for_purpose=TransactionForPurpose.FOR_AUTO_LINK,
            job_type=job_type,
            db_instance=db_instance,
        ):
            if df is None or not len(df):
                continue

            self.handle_auto_link_data(
                df,
                etl.cfg_data_table,
                db_instance=db_instance,
            )

    @Log.log_execution_time()
    @BridgeStationModel.use_db_instance()
    def pull_tables(
        self,
        etl: Optional[
            Union[
                EFAService,
                EtlDbService,
                DBLongService,
            ]
        ],
        job_type: JobType,
        db_instance: PostgreSQL = None,
    ):
        if etl is None:
            raise NotImplementedError

        if isinstance(etl, EFAService):
            convert_col, dict_config = get_factory_master_data(etl.cfg_data_table)
        else:
            convert_col, dict_config = None, None

        # get min and max time of partition
        get_n_save_partition_range_time_from_factory_db(etl.cfg_data_table)
        dic_tz_info = etl.get_time_zone_info()
        min_dt, max_dt, ori_min_dt, ori_max_dt = etl.cfg_data_table.get_min_max_time()
        start_dt = None
        with ReadOnlyDbProxy(etl.cfg_data_table.data_source) as factory_db_instance:
            CfgConstant.force_running_job()

            dates = check_latest_trans_data(
                etl.cfg_data_table,
                min_dt,
                max_dt,
                is_past=True,
                seconds=etl.factory_next_sql_range_seconds,
                filter_time=start_dt,
                ori_min_dt=ori_min_dt,
                ori_max_dt=ori_max_dt,
                job_type=job_type,
            )

            start_dt_str, end_dt_str, seconds, start_dt, end_dt, is_continue, is_break = dates

            # count data to determine best date time range have records
            while True:
                cnt = etl.count_transaction_data(factory_db_instance, start_dt_str, end_dt_str)

                print('COUNT DATA:', start_dt_str, end_dt_str, cnt)
                if not cnt:
                    # no table or no data
                    continue

                if cnt > PullForAutoLink.LIMIT_RECORD_COUNT:
                    # adjust time range
                    etl.calc_sql_range_days(cnt, start_dt=start_dt, end_dt=end_dt)
                    break

                dates = check_latest_trans_data(
                    etl.cfg_data_table,
                    min_dt,
                    max_dt,
                    is_past=True,
                    seconds=etl.factory_next_sql_range_seconds,
                    filter_time=start_dt,
                    ori_min_dt=ori_min_dt,
                    ori_max_dt=ori_max_dt,
                    job_type=job_type,
                )

                start_dt_str, _, _, start_dt, _, _, is_break = dates
                if is_break:
                    break

                diff_dt = convert_time(end_dt_str, return_string=False) - convert_time(
                    start_dt,
                    return_string=False,
                )

                if diff_dt.days > PullForAutoLink.LIMIT_DAY_COUNT:
                    break

            # ----------------
            data = etl.get_transaction_data(factory_db_instance, start_dt_str, end_dt_str)

            cols = next(data)
            rows = []
            for _rows in data:
                if _rows is None:
                    continue

                rows.extend(_rows)

            if rows:
                df = etl.gen_df_transaction(cols, rows, convert_col, dict_config)
                convert_db_timezone(df, etl, dic_tz_info)
                self.handle_auto_link_data(
                    df,
                    etl.cfg_data_table,
                    db_instance=db_instance,
                )

    @Log.log_execution_time()
    @BridgeStationModel.use_db_instance()
    def handle_auto_link_data(
        self,
        df: DataFrame,
        cfg_data_table: CfgDataTable,
        db_instance: PostgreSQL = None,
    ):
        """This function performs 2 main tasks
        1. Get master data and import master data
        2. Pull auto link data for each process
        """
        if df.empty:
            return None

        df.reset_index(drop=True, inplace=True)
        df[INDEX_COL] = df.index

        master_db_type = cfg_data_table.get_master_type()
        primary_group = get_primary_group(db_instance=db_instance)
        for process_id, df, *_ in loop_join_master_data_for_import(
            cfg_data_table,
            df,
            master_db_type,
            primary_group,
            db_instance=db_instance,
        ):
            CfgConstant.force_running_job()

            # In case all master data are new and not exist in DB, do nothing
            if df is None or df.empty:
                continue

            cfg_process = CfgProcess.get_by_process_id(db_instance, process_id, is_cascade_column=True)
            self._convert_datetime_(df, cfg_process.columns, cfg_process.datetime_format)
            datetime_column = (
                primary_group.DATA_TIME
                if cfg_data_table.get_date_col()
                else cfg_process.get_auto_increment_col(column_name_only=False).id
            )

            df_auto_link_data = df[[str(datetime_column), primary_group.DATA_SERIAL]].rename(
                columns={str(datetime_column): DATE, primary_group.DATA_SERIAL: SERIAL},
            )
            df_auto_link_data[DATE] = df_auto_link_data[DATE].dt.tz_localize(None)
            df_auto_link_data[AUTO_LINK_ID] = process_id
            df_auto_link_data = PullForAutoLink.convert_df(df_auto_link_data)
            self.pulled_df = pd.concat([self.pulled_df, df_auto_link_data])
            self.pulled_df = AutoLinkData.drop_duplicates(self.pulled_df)

    @BridgeStationModel.use_db_instance()
    def update_auto_link_data(self, db_instance: PostgreSQL = None):
        CfgConstant.force_running_job()

        if self.pulled_df.empty:
            return

        # Insert into t_auto_link table
        df_data = self.pulled_df.rename(
            columns={
                AUTO_LINK_ID: AutoLink.Columns.process_id.name,
                SERIAL: AutoLink.Columns.serial.name,
                DATE: AutoLink.Columns.date_time.name,
            },
        )
        AutoLink.update_data(df_data, db_instance=db_instance)

    @Log.log_execution_time()
    @BridgeStationModel.use_db_instance()
    def get_data_from_local(self, db_instance: PostgreSQL = None) -> None:
        self.get_data_from_t_auto_link(db_instance=db_instance)
        self.get_data_from_transaction_table(db_instance=db_instance)
        self.get_data_from_feather_file()
        self.update_processes()

    @staticmethod
    def convert_df(df: pd.DataFrame):
        df = df.replace({None: pd.NA}).dropna(subset=[SERIAL, DATE], how='any').convert_dtypes()
        df[SERIAL] = df[SERIAL].astype(str)
        return df

    @Log.log_execution_time()
    def get_data_from_t_auto_link(self, db_instance: PostgreSQL) -> None:
        for process_id in self.processes:
            data = self.auto_link_data.get(process_id)

            df = AutoLink.get_by_process_id(
                process_id=data.process_id,
                return_df=True,
                db_instance=db_instance,
                limit=data.records_needed,
            )
            if AutoLink.Columns.id.name in df:
                df = df.drop(columns=[AutoLink.Columns.id.name])
            df = df.rename(
                columns={
                    AutoLink.Columns.serial.name: SERIAL,
                    AutoLink.Columns.date_time.name: DATE,
                },
            )
            df = PullForAutoLink.convert_df(df)
            self.auto_link_data.update_per_process(data.process_id, df)

    @Log.log_execution_time()
    def get_data_from_transaction_table(self, db_instance: PostgreSQL) -> None:
        for process_id in self.processes:
            data = self.auto_link_data.get(process_id)

            transaction_data = TransactionData(process_id=data.process_id)
            if transaction_data.table_name not in db_instance.list_tables():
                continue

            if transaction_data.serial_column is None:
                raise ValueError('Only transaction table has serial columns can perform autolink')
            if transaction_data.getdate_column is None:
                raise ValueError('Only transaction table has getdate columns can perform autolink')
            table = transaction_data.table_model
            columns = [
                table.c.get(transaction_data.serial_column.bridge_column_name).label(SERIAL),
                table.c.get(transaction_data.getdate_column.bridge_column_name).label(DATE),
            ]
            query = sa.select(columns).select_from(table).limit(data.records_needed)
            df = df_from_query(query=query, db_instance=db_instance)
            df = PullForAutoLink.convert_df(df)
            self.auto_link_data.update_per_process(data.process_id, df)

    def _convert_datetime_(self, df: pd.DataFrame, cfg_process_columns, datetime_format):
        # Start - Convert datetime columns
        df_columns = [str(col) for col in df.columns]
        dic_data_types = {
            process_column.id: process_column.raw_data_type
            for process_column in cfg_process_columns
            if str(process_column.id) in df_columns
        }
        if DataGroupType.DATA_TIME.name in df.columns:
            dic_data_types.update({DataGroupType.DATA_TIME.name: RawDataTypeDB.DATETIME.value})
        df = convert_datetime_format(df, dic_data_types, datetime_format)
        # convert timezone
        for col_id, raw_data_type in dic_data_types.items():
            if raw_data_type == RawDataTypeDB.DATETIME.value:
                convert_csv_timezone_per_process(df, str(col_id), is_convert_dtype_string=False)
        # End - Convert datetime columns

    @Log.log_execution_time()
    def get_data_from_feather_file(self) -> None:
        for process_id in self.processes:
            data = self.auto_link_data.get(process_id)

            # TODO: rewrite this itertools chains
            files = [
                *itertools.chain.from_iterable(get_import_files(data.process_id, is_past=False).values()),
                *itertools.chain.from_iterable(get_import_files(data.process_id, is_past=True).values()),
            ]
            transaction_data = TransactionData(process_id=data.process_id)
            if transaction_data.serial_column is None:
                raise ValueError('Only transaction table has serial columns can perform autolink')
            if transaction_data.getdate_column is None:
                raise ValueError('Only transaction table has getdate columns can perform autolink')

            for file in files:
                if self.auto_link_data.enough_data_for_process(data.process_id):
                    break
                df = read_feather_file(file)
                if DataGroupType.DATA_TIME.name in df.columns:
                    for col in transaction_data.cfg_process_columns:
                        if col.column_name == DataGroupType.DATA_TIME.name:
                            df = df.rename(columns={DataGroupType.DATA_TIME.name: str(col.id)})

                self._convert_datetime_(
                    df,
                    transaction_data.cfg_process_columns,
                    transaction_data.cfg_process.datetime_format,
                )

                df = transaction_data.rename_columns_for_import(df)
                df = df[
                    [
                        transaction_data.serial_column.bridge_column_name,
                        transaction_data.getdate_column.bridge_column_name,
                    ]
                ]
                df = df.rename(
                    columns={
                        transaction_data.serial_column.bridge_column_name: SERIAL,
                        transaction_data.getdate_column.bridge_column_name: DATE,
                    },
                )
                df = PullForAutoLink.convert_df(df)
                self.auto_link_data.update_per_process(data.process_id, df)
