from typing import Union

import numpy as np
import pandas as pd
from pandas import DataFrame, Series

from ap.api.setting_module.services.common import get_datetime_val
from ap.api.setting_module.services.data_import import (
    add_transaction_import_job,
    convert_df_col_to_utc,
    validate_datetime,
)
from ap.common.common_utils import (
    DATE_FORMAT_STR_ONLY_DIGIT_SHORT,
    add_delta_to_datetime,
    add_seconds,
    convert_time,
    delete_file,
    get_basename,
    read_feather_file,
)
from ap.common.constants import (
    FEATHER_MAX_RECORD,
    INDEX_COL,
    JOB_ID,
    NEW_COLUMN_PROCESS_IDS_KEY,
    PROC_PART_ID_COL,
    SQL_FACTORY_LIMIT,
    CfgConstantType,
    DataGroupType,
    DBType,
    JobStatus,
    JobType,
    MasterDBType,
    RawDataTypeDB,
)
from ap.common.logger import log_execution_time, logger
from ap.common.pydn.dblib.db_proxy_readonly import ReadOnlyDbProxy
from ap.common.pydn.dblib.postgresql import PostgreSQL
from ap.common.scheduler import scheduler_app_context
from ap.common.timezone_utils import get_time_info
from ap.setting_module.models import CfgConstant, CfgDataTable, CfgDataTableColumn, MappingFactoryMachine
from ap.setting_module.services.background_process import JobInfo, send_processing_info
from bridge.models.bridge_station import BridgeStationModel
from bridge.models.m_data import MData
from bridge.models.m_data_group import PrimaryGroup, get_primary_group
from bridge.models.m_process import MProcess
from bridge.models.mapping_process_data import MappingProcessData
from bridge.models.transaction_model import TransactionData
from bridge.services.csv_management import add_scan_files_job
from bridge.services.data_import import (
    CSV_IDX,
    convert_datetime_format,
    gen_import_data_file,
    gen_transaction_data_unknown_master_file,
    generate_datetime_dummy,
    get_transaction_data_unknown_master_files,
    join_mapping_process_data,
    join_master_data_common,
    transform_horizon_columns_for_import,
)
from bridge.services.etl_services.etl_controller import ETLController
from bridge.services.etl_services.etl_csv_service import EtlCsvService
from bridge.services.etl_services.etl_db_service import (
    EtlDbService,
    get_auto_link_data_range,
    get_future_range,
    get_n_save_partition_range_time_from_factory_db,
    get_past_range,
)
from bridge.services.etl_services.etl_efa_service import EFAService, get_factory_master_data
from bridge.services.etl_services.etl_software_workshop_services import SoftwareWorkshopService
from bridge.services.etl_services.etl_v2_history_service import V2HistoryService
from bridge.services.etl_services.etl_v2_measure_service import V2MeasureService
from bridge.services.etl_services.etl_v2_multi_history_service import V2MultiHistoryService
from bridge.services.etl_services.etl_v2_multi_measure_service import V2MultiMeasureService
from bridge.services.master_data_import import scan_master
from bridge.services.scan_data_type import scan_data_type


def pull_db(
    factory_db_instance,
    job_type,
    etl: EtlDbService,
    start_dt: str,
    end_dt: str,
    dic_tz_info,
    convert_col,
    dict_config,
):
    yield 0
    # get job_id
    dic_job_id = {}
    yield dic_job_id

    job_info = JobInfo()
    job_info.job_id = dic_job_id.get(JOB_ID)
    job_info.job_type = job_type
    job_info.detail = f'{start_dt} - {end_dt}'

    data = etl.get_transaction_data(factory_db_instance, start_dt, end_dt)

    cols = next(data)
    rows = []
    for _rows in data:
        if _rows is None:
            continue

        rows.extend(_rows)
        job_info.calc_percent(len(rows), SQL_FACTORY_LIMIT)
        yield job_info.percent

    data_len = len(rows)
    if data_len:
        df = etl.gen_df_transaction(cols, rows, convert_col, dict_config)
        job_info.auto_increment_start_tm = start_dt
        job_info.auto_increment_end_tm = str(df[etl.auto_increment_col].max())
        convert_db_timezone(df, etl, dic_tz_info)

        start_dt_str = convert_time(start_dt, format_str=DATE_FORMAT_STR_ONLY_DIGIT_SHORT)
        end_dt_str = convert_time(end_dt, format_str=DATE_FORMAT_STR_ONLY_DIGIT_SHORT)
        binary_file_prefix = f'{start_dt_str}_{end_dt_str}'
        job_info = gen_master_and_pull_data_vertical_holding(
            df,
            etl.cfg_data_table,
            job_info,
            binary_file_suffix=binary_file_prefix,
        )
    job_info.status = JobStatus.DONE
    job_info.committed_count = data_len
    yield job_info

    yield 100


@BridgeStationModel.use_db_instance_generator()
def pull_csv(job_type, etl: EtlCsvService, job_info, ignore_add_job: bool = False, db_instance: PostgreSQL = None):
    yield 0
    # call scan file
    # ['登録日時', '工程名', 'ライン名']
    split_cols = CfgDataTableColumn.get_split_columns(etl.cfg_data_table.id)
    columns = CfgDataTableColumn.get_column_names_by_data_group_types(etl.cfg_data_table.id, split_cols)
    if not ignore_add_job:
        add_scan_files_job(data_table_id=etl.cfg_data_table.id, columns=columns)

    # get job_id
    dic_job_id = {}
    yield dic_job_id
    job_info.job_id = dic_job_id.get(JOB_ID)
    job_info.job_type = job_type
    data = etl.get_transaction_data(job_type=job_type, db_instance=db_instance)
    for df, target_files, percentage in data:
        job_info.percent = percentage

        if df is None or not len(df):
            continue

        # convert_csv_timezone(df, etl)  # No need to convert here, it will be converted in "import transaction" process
        if etl.cfg_data_table.is_has_auto_increment_col():
            auto_increment_col = (
                etl.auto_increment_col if etl.auto_increment_col in df else DataGroupType.DATA_TIME.name
            )
            datetime_series = df[auto_increment_col].astype(pd.StringDtype())
            job_info.auto_increment_start_tm = str(datetime_series.min())
            job_info.auto_increment_end_tm = str(datetime_series.max())
        else:
            job_info.auto_increment_start_tm = None
            job_info.auto_increment_end_tm = None

        # TODO: check why v2 not replace df column to standard (like EFA)
        # import data
        sort_files = sorted(target_files)
        start_file = get_basename(sort_files[0])
        end_file = get_basename(sort_files[-1])
        binary_file_suffix = f'{start_file}_{end_file}'
        job_info = gen_master_and_pull_data_vertical_holding(
            df,
            etl.cfg_data_table,
            job_info,
            binary_file_suffix=binary_file_suffix,
            db_instance=db_instance,
        )

        # save t_csv_import
        job_info.committed_count = len(df)
        job_info.target = target_files
        job_info.status = JobStatus.DONE
        yield job_info

    yield 100


def check_latest_trans_data(
    cfg_data_table: CfgDataTable,
    min_dt=None,
    max_dt=None,
    is_past=False,
    seconds=None,
    filter_time=None,
    ori_min_dt=None,
    ori_max_dt=None,
    job_type: JobType = None,
):
    is_break = False
    start_dt_str = ''
    end_dt_str = ''
    if not min_dt or not max_dt:
        min_dt, max_dt, ori_min_dt, ori_max_dt = cfg_data_table.get_min_max_time()

    if job_type is JobType.PULL_FOR_AUTO_LINK:
        start_dt, end_dt, seconds = get_auto_link_data_range(
            cfg_data_table.id,
            max_dt,
            seconds=seconds,
            filter_time=filter_time,
        )
        if start_dt < min_dt:
            is_break = True
    elif is_past:
        start_dt, end_dt, seconds, is_break = get_past_range(
            cfg_data_table.id,
            seconds=seconds,
            filter_time=filter_time,
        )
    else:
        start_dt, end_dt, seconds = get_future_range(
            cfg_data_table.id,
            min_dt,
            max_dt,
            seconds=seconds,
            filter_time=filter_time,
        )

    is_continue = True
    if start_dt == end_dt or start_dt > max_dt:
        is_break = True
        is_continue = False
        return start_dt_str, end_dt_str, seconds, start_dt, end_dt, is_continue, is_break

    if end_dt > max_dt:
        is_continue = False

    is_mssql_datetime = cfg_data_table.data_source.type == DBType.MSSQLSERVER.name
    # TODO: check if ori_max_dt have not format millisecond, start_dt, end_dt format millisecond
    start_dt_str = add_delta_to_datetime(
        ori_max_dt,
        time_delta=start_dt - max_dt,
        is_mssql_datetime=is_mssql_datetime,
    )
    end_dt_str = add_delta_to_datetime(ori_min_dt, time_delta=end_dt - min_dt, is_mssql_datetime=is_mssql_datetime)

    return start_dt_str, end_dt_str, seconds, start_dt, end_dt, is_continue, is_break


@scheduler_app_context
def pull_db_job(_job_id=None, _job_name=None, _db_id=None, _data_table_id=None, import_process_id=None):
    job_type = JobType[_job_name]
    # start job
    cfg_data_table = CfgDataTable.get_by_id(_data_table_id)
    if not cfg_data_table.partition_tables:
        return

    etl = ETLController.get_etl_service(cfg_data_table)
    if isinstance(etl, EFAService):
        convert_col, dict_config = get_factory_master_data(cfg_data_table)
    elif isinstance(etl, (EtlDbService, SoftwareWorkshopService)):
        convert_col, dict_config = None, None
    else:
        raise NotImplementedError

    # get min and max time of partition
    get_n_save_partition_range_time_from_factory_db(cfg_data_table)
    dic_tz_info = etl.get_time_zone_info()
    min_dt, max_dt, ori_min_dt, ori_max_dt = cfg_data_table.get_min_max_time()
    end_dt = None
    is_continue = True

    interval_sec = CfgConstant.get_value_by_type_first(CfgConstantType.POLLING_FREQUENCY.name, int)
    # convert_col, dict_config = get_factory_master_data(cfg_data_table)
    with ReadOnlyDbProxy(cfg_data_table.data_source) as factory_db_instance:
        while is_continue:
            CfgConstant.force_running_job()

            dates = check_latest_trans_data(
                cfg_data_table,
                min_dt,
                max_dt,
                seconds=etl.factory_next_sql_range_seconds,
                filter_time=end_dt,
                ori_min_dt=ori_min_dt,
                ori_max_dt=ori_max_dt,
            )

            start_dt_str, end_dt_str, seconds, start_dt, end_dt, is_continue, is_break = dates
            if is_break:
                break

            is_mssql_datetime = cfg_data_table.data_source.type == DBType.MSSQLSERVER.name

            # count data
            is_adjusted = False
            while True:
                cnt = etl.count_transaction_data(factory_db_instance, start_dt_str, end_dt_str)
                print('COUNT DATA:', start_dt_str, end_dt_str, cnt)
                if not cnt:
                    # no table or no data
                    break

                if cnt < SQL_FACTORY_LIMIT:
                    # adjust time range
                    etl.calc_sql_range_days(cnt, start_dt=start_dt, end_dt=end_dt)
                    break

                seconds = etl.calc_sql_range_days(cnt, start_dt=start_dt, end_dt=end_dt)
                end_dt = add_seconds(start_dt, seconds=seconds)
                end_dt_str = add_delta_to_datetime(
                    ori_max_dt,
                    time_delta=end_dt - max_dt,
                    is_mssql_datetime=is_mssql_datetime,
                )
                is_adjusted = True

            if not cnt:
                continue

            if is_adjusted:
                dates = check_latest_trans_data(
                    cfg_data_table,
                    min_dt,
                    max_dt,
                    seconds=seconds,
                    filter_time=start_dt,
                    ori_min_dt=ori_min_dt,
                    ori_max_dt=ori_max_dt,
                )

                start_dt_str, end_dt_str, seconds, start_dt, end_dt, is_continue, is_break = dates
                if is_break:
                    break

            # import data
            generator = pull_db(
                factory_db_instance,
                job_type,
                etl,
                start_dt_str,
                end_dt_str,
                dic_tz_info,
                convert_col,
                dict_config,
            )
            send_processing_info(generator, job_type, data_table_id=_data_table_id)

            # call import
            if import_process_id:
                add_transaction_import_job(import_process_id, interval_sec=interval_sec, run_now=True)


@scheduler_app_context
def pull_past_db_job(_job_id=None, _job_name=None, _db_id=None, _data_table_id=None):
    job_type = JobType[_job_name]
    # start job
    cfg_data_table = CfgDataTable.get_by_id(_data_table_id)
    if not cfg_data_table.partition_tables:
        return

    master_type = cfg_data_table.get_master_type()
    if MasterDBType.is_efa_group(master_type):
        etl = EFAService(cfg_data_table)
        convert_col, dict_config = get_factory_master_data(cfg_data_table)
    else:
        etl = EtlDbService(cfg_data_table)
        convert_col, dict_config = None, None

    # get min and max time of partition
    get_n_save_partition_range_time_from_factory_db(cfg_data_table)
    dic_tz_info = etl.get_time_zone_info()
    min_dt, max_dt, ori_min_dt, ori_max_dt = cfg_data_table.get_min_max_time()
    start_dt = None
    is_continue = True
    first_end_dt = None
    with ReadOnlyDbProxy(cfg_data_table.data_source) as factory_db_instance:
        while is_continue:
            CfgConstant.force_running_job()

            dates = check_latest_trans_data(
                cfg_data_table,
                min_dt,
                max_dt,
                is_past=True,
                seconds=etl.factory_next_sql_range_seconds,
                filter_time=start_dt,
                ori_min_dt=ori_min_dt,
                ori_max_dt=ori_max_dt,
            )

            start_dt_str, end_dt_str, seconds, start_dt, end_dt, is_continue, is_break = dates
            if is_break:
                break
            is_mssql_datetime = cfg_data_table.data_source.type == DBType.MSSQLSERVER.name
            if first_end_dt is None:
                first_end_dt = end_dt

            # count data
            is_adjusted = False
            while True:
                cnt = etl.count_transaction_data(factory_db_instance, start_dt_str, end_dt_str)
                if not cnt:
                    # no table or no data
                    break

                if cnt < SQL_FACTORY_LIMIT:
                    # adjust time range
                    etl.calc_sql_range_days(cnt, start_dt=start_dt, end_dt=end_dt)
                    break

                seconds = etl.calc_sql_range_days(cnt, start_dt=start_dt, end_dt=end_dt)
                start_dt = add_seconds(end_dt, seconds=-seconds)
                start_dt_str = add_delta_to_datetime(
                    ori_min_dt,
                    time_delta=start_dt - min_dt,
                    is_mssql_datetime=is_mssql_datetime,
                )
                is_adjusted = True

            if not cnt:
                continue

            if is_adjusted:
                dates = check_latest_trans_data(
                    cfg_data_table,
                    min_dt,
                    max_dt,
                    is_past=True,
                    seconds=seconds,
                    filter_time=end_dt,
                    ori_min_dt=ori_min_dt,
                    ori_max_dt=ori_max_dt,
                )

                start_dt_str, end_dt_str, seconds, start_dt, end_dt, is_continue, is_break = dates
                if is_break:
                    break

            # import data
            generator = pull_db(
                factory_db_instance,
                job_type,
                etl,
                start_dt_str,
                end_dt_str,
                dic_tz_info,
                convert_col,
                dict_config,
            )
            send_processing_info(generator, job_type, data_table_id=_data_table_id)
            diff_dt = convert_time(first_end_dt, return_string=False) - convert_time(start_dt, return_string=False)
            is_continue = diff_dt.days < 7


@scheduler_app_context
def pull_csv_job(_job_id=None, _job_name=None, _db_id=None, _data_table_id=None, import_process_id=None):
    # start job
    cfg_data_table = CfgDataTable.get_by_id(_data_table_id)
    etl = ETLController.get_etl_service(cfg_data_table)
    if etl is None:
        raise NotImplementedError

    job_info = JobInfo()
    job_type = JobType[_job_name]
    job_info.job_type = job_type
    generator = pull_csv(job_type, etl, job_info)
    send_processing_info(generator, job_type, data_table_id=_data_table_id)
    # call import
    if import_process_id:
        interval_sec = CfgConstant.get_value_by_type_first(CfgConstantType.POLLING_FREQUENCY.name, int)
        add_transaction_import_job(import_process_id, interval_sec=interval_sec, run_now=True)


@scheduler_app_context
def pull_past_csv_job(_job_id=None, _job_name=None, _db_id=None, _data_table_id=None):
    # start job
    cfg_data_table = CfgDataTable.get_by_id(_data_table_id)
    master_type = cfg_data_table.get_master_type()
    if master_type == MasterDBType.V2.name:
        etl = V2MeasureService(cfg_data_table)
    elif master_type == MasterDBType.V2_HISTORY.name:
        etl = V2HistoryService(cfg_data_table)
    elif master_type == MasterDBType.V2_MULTI.name:
        etl = V2MultiMeasureService(cfg_data_table)
    elif master_type == MasterDBType.V2_MULTI_HISTORY.name:
        etl = V2MultiHistoryService(cfg_data_table)
    else:
        etl = EtlCsvService(cfg_data_table)

    job_info = JobInfo()
    job_type = JobType[_job_name]
    job_info.job_type = job_type
    generator = pull_csv(job_type, etl, job_info)
    send_processing_info(generator, job_type, data_table_id=_data_table_id)


@scheduler_app_context
def pull_feather_file_job(_job_id=None, _job_name=None, _data_table_id=None):
    """
    Call job to process the feather files that contains unknown master data that was just registered on Mapping pages
    Args:
        _job_id (int): Job id of the
        _job_name (str): Name of the job
        _data_table_id (int): ID of the data table

    Returns:
        void
    """
    # start job
    job_info = JobInfo()
    job_info.job_id = _job_id
    job_info.job_type = JobType.PULL_FEATHER_DATA
    cfg_data_table = CfgDataTable.get_by_id(_data_table_id)
    files = get_transaction_data_unknown_master_files(_data_table_id)
    generator = handle_transaction_data_unknown_master(cfg_data_table, files, job_info)
    send_processing_info(generator, _job_name, data_table_id=_data_table_id)


def handle_transaction_data_unknown_master(
    cfg_data_table: CfgDataTable,
    files: list[str],
    job_info: JobInfo,
) -> Union[int, JobInfo]:
    """Do process the feather files that contains unknown master data that was just registered on Mapping pages

    Args:
        cfg_data_table (CfgDataTable): CfgDataTable instance
        files (list[str]): List of file path
        job_info (JobInfo): JobInfo instance

    Returns:
        JobInfo: Job information that contains status
    """
    yield 0
    for index, file_name in enumerate(files):
        job_info.percent = round((index + 1) * 100 / len(files), 2)
        df = read_feather_file(file_name)
        gen_master_and_pull_data_vertical_holding(df, cfg_data_table, job_info)
        delete_file(file_name)
        yield job_info

    yield 100


@log_execution_time()
def convert_db_timezone(df, etl: EtlDbService, dic_tz_info=None):
    cols = etl.get_all_dt_cols()
    for col in cols:
        # TODO : output error and duplicate info
        if col in df.columns:
            validate_datetime(df, col, is_strip=False, add_is_error_col=False)
            is_tz_inside, db_time_zone, time_offset = dic_tz_info[col]
            df[col] = convert_df_col_to_utc(df, col, is_tz_inside, db_time_zone, time_offset)
            df[col] = remove_timezone_inside(df[col], is_tz_inside)


@log_execution_time()
def convert_csv_timezone(df, etl):
    # convert datetime columns into correct format
    convert_datetime_format(df, {etl.get_date_col: RawDataTypeDB.DATETIME.value})

    cols = set(etl.datetime_cols + [etl.get_date_col, etl.auto_increment_col])
    for col in cols:
        if col in df.columns:
            datetime_val = get_datetime_val(df[col])
            is_timezone_inside, csv_timezone, utc_offset = get_time_info(datetime_val, None)
            df[col] = convert_df_col_to_utc(df, col, is_timezone_inside, csv_timezone, utc_offset)
            df[col] = remove_timezone_inside(df[col], is_timezone_inside)


def remove_timezone_inside(datetime_series: Series, is_tz_inside: bool):
    """
    Remove +-timezone in datetime column because DB auto add +9:00 timezone when it have timezone inside.

    :param datetime_series: Datetime column
    :param is_tz_inside: is timezone inside
    :return: series without timezone
    """

    if is_tz_inside:
        try:
            return datetime_series.dt.tz_convert(None)
        except Exception:
            pass

    return datetime_series


@BridgeStationModel.use_db_instance()
def gen_master_and_pull_data_vertical_holding(
    df_origin: DataFrame,
    cfg_data_table: CfgDataTable,
    job_info: JobInfo,
    binary_file_suffix: str = None,
    db_instance: PostgreSQL = None,
):
    """This function performs 2 main tasks
    1. Get master data and import master data
    2. Pull data for each process
    """
    # TODO : issue : index column changed, many processes in horizontal file.
    # Start - 1. Get master data and import master data
    if df_origin.empty:
        return None

    # reset index
    df_origin.reset_index(drop=True, inplace=True)
    df = df_origin.copy()
    df[INDEX_COL] = df.index
    mapped_idxs = []

    # Start - 2. Pull data for each process
    is_past = job_info.job_type not in (JobType.PULL_CSV_DATA, JobType.PULL_DB_DATA, JobType.PULL_FEATHER_DATA)

    master_db_type = cfg_data_table.get_master_type()
    primary_group = get_primary_group(db_instance=db_instance)
    for process_id, df, idxs in loop_join_master_data_for_import(
        cfg_data_table,
        df,
        master_db_type,
        primary_group,
        db_instance=db_instance,
    ):
        # In case all master data are new and not exist in DB
        if df is None or df.empty:
            continue

        gen_import_data_files(cfg_data_table, process_id, df, binary_file_suffix, is_past, db_instance=db_instance)
        mapped_idxs.extend(idxs)

    if len(mapped_idxs) == len(df_origin):
        # In case of 100% well known master data
        return job_info

    df_non_mapping: DataFrame = df_origin[~df_origin.index.isin(mapped_idxs)]
    return gen_master_and_pull_data_unknown_master_vertical_holding(
        df_non_mapping,
        cfg_data_table,
        job_info,
        binary_file_suffix,
        is_past,
        master_db_type,
        primary_group,
        db_instance=db_instance,
    )


@log_execution_time()
@BridgeStationModel.use_db_instance()
def gen_master_and_pull_data_unknown_master_vertical_holding(
    df_non_mapping: DataFrame,
    cfg_data_table: CfgDataTable,
    job_info: JobInfo,
    binary_file_suffix: str,
    is_past: bool,
    master_db_type: str,
    primary_group: PrimaryGroup,
    db_instance: PostgreSQL = None,
) -> JobInfo:
    """Handle dataframe that only contains data with unknown master data.

    1. Do Scan master
    2. Do Scan data type
    3. In case of through Mapping page, export to temporary feather files to handle later (in PULL_FEATHER_DATA Job).
       Otherwise, directly handle this data.

    Args:
        df_non_mapping (DataFrame): DataFrame containing unknown master data
        cfg_data_table (CfgDataTable): a CfgDataTable instance
        job_info (JobInfo): Job info
        binary_file_suffix (str):
        is_past (bool): It means that handle for further data or past data
        master_db_type (str): Type of master database
        primary_group (PrimaryGroup): Primary group
        db_instance (PostgreSQL): a PostgreSQL instance

    Returns:
        JobInfo: Job info
    """
    if df_non_mapping is None or df_non_mapping.empty:
        return job_info

    # ===== Start - Scan master and data type =====
    data_stream = (df_non_mapping, [], 99)
    new_master_dict = {}
    # Not scan master again for pull data from already feather files due to new master was exist in scan_... files
    if job_info.job_type != JobType.PULL_FEATHER_DATA:
        generator_import_factory = scan_master(
            cfg_data_table.id,
            data_stream=[data_stream],
            return_new_master_dict=new_master_dict,
            db_instance=db_instance,
            is_unknown_master=True,
        )
        *_, is_export_to_pickle_files = send_processing_info(
            generator_import_factory,
            JobType.SCAN_UNKNOWN_MASTER,
            data_table_id=cfg_data_table.id,
            is_check_disk=False,
            is_run_one_time=True,
        )

    if job_info.job_type == JobType.PULL_FEATHER_DATA or is_export_to_pickle_files:
        # Export data that contain unknown master to feather files, it will be pulled again
        # if new master data is registered
        gen_transaction_data_unknown_master_file(cfg_data_table.id, df_non_mapping, binary_file_suffix)
        return job_info

    if MData.get_table_name() in new_master_dict:  # in case new column is recognized
        generator = scan_data_type(cfg_data_table.id, data_steam=[data_stream], db_instance=db_instance)
        send_processing_info(
            generator,
            JobType.SCAN_UNKNOWN_DATA_TYPE,
            data_table_id=cfg_data_table.id,
            is_check_disk=False,
            is_run_one_time=True,
        )

        # Add new column into t_process_... table in case new column was recognized and registered in m_data table
        for process_id in new_master_dict[NEW_COLUMN_PROCESS_IDS_KEY]:
            transaction_data_obj = TransactionData(process_id, db_instance=db_instance)
            if transaction_data_obj.table_name in db_instance.list_tables():
                new_columns = transaction_data_obj.get_new_columns(db_instance)
                if new_columns:
                    dict_new_col_with_type = {column.bridge_column_name: column.data_type for column in new_columns}
                    transaction_data_obj.add_columns(db_instance, dict_new_col_with_type, auto_commit=False)
                    logger.info(
                        f'[NEW_MASTER] Add new columns in '
                        f'{transaction_data_obj.table_name} table: {dict_new_col_with_type}',
                    )
    # ===== End - Scan master and data type =====

    # ===== Start - Pull data for each process =====
    df_non_mapping[INDEX_COL] = df_non_mapping.index
    for process_id, df, _ in loop_join_master_data_for_import(
        cfg_data_table,
        df_non_mapping,
        master_db_type,
        primary_group,
        db_instance=db_instance,
    ):
        _binary_file_prefix = f'{binary_file_suffix}_NewMaster' if binary_file_suffix else 'NewMaster'
        gen_import_data_files(cfg_data_table, process_id, df, _binary_file_prefix, is_past, db_instance=db_instance)
    # ===== End - Pull data for each process =====

    return job_info


@log_execution_time()
@BridgeStationModel.use_db_instance_generator()
def loop_join_master_data_for_import(
    cfg_data_table: CfgDataTable,
    df: DataFrame,
    master_db_type,
    primary_group: PrimaryGroup,
    ignore_cols: list[str] = None,
    db_instance: PostgreSQL = None,
):
    unique_cols = get_transaction_unique_cols(cfg_data_table)
    process_id_col = MData.Columns.process_id.name
    data_id_col = MappingProcessData.Columns.data_id.name
    df, dic_df_horizons = transform_horizon_columns_for_import(cfg_data_table, df, ignore_cols=ignore_cols)
    df.set_index(INDEX_COL)
    df[MappingProcessData.Columns.data_table_id.name] = cfg_data_table.id

    is_v2_history = master_db_type == MasterDBType.V2_HISTORY.name
    is_v2_multi_history = master_db_type == MasterDBType.V2_MULTI_HISTORY.name
    join_data_type_cols = [
        primary_group.PROCESS_ID,
        primary_group.PROCESS_NAME,
        primary_group.DATA_ID,
        primary_group.DATA_NAME,
        MappingProcessData.Columns.data_table_id.name,
    ]

    df, *_ = join_master_data_common(
        df,
        db_instance,
        primary_group,
        is_v2_history=is_v2_history or is_v2_multi_history,
    )
    if df is None or df.empty:
        yield None, df, None
        return

    # remove deleted process ids
    exist_proc_ids = MProcess.get_existed_process_ids(db_instance=db_instance)
    df = df[df[process_id_col].isin(exist_proc_ids)]

    df_mapping_data = None
    if dic_df_horizons:
        for process_id, _df_process in df.groupby(process_id_col):
            df_process = _df_process
            if CSV_IDX in df.columns:
                df_process = generate_datetime_dummy(df_process, process_id)
            df_process_horizon = df_process[unique_cols]
            # for better performance
            drop_dupl_cols = [col for col in join_data_type_cols if col in df_process.columns]
            df_process = df_process.drop_duplicates(subset=drop_dupl_cols)
            data_ids = set()
            for quality_name, s in dic_df_horizons.items():
                if s.isnull().all():
                    continue

                # if quality_name == DataGroupType.FILE_NAME.name:  # FileName not in mapping table, no need to join
                #     df_process_horizon[quality_name] = s
                #     continue

                df_process[primary_group.DATA_NAME] = quality_name
                df_horizons, df_mapping_data = join_mapping_process_data(
                    db_instance,
                    df_process,
                    primary_group,
                    df_mapping_data,
                )
                if df_horizons is None or df_horizons.empty:
                    continue

                key = str(df_horizons[data_id_col].iloc[0])
                data_ids.add(key)
                df_process_horizon[key] = s

            df_process_horizon.dropna(subset=list(data_ids), how='all')
            idxs = df_process_horizon.index.tolist()
            yield process_id, df_process_horizon, idxs
    else:
        for process_id, _df_process in df.groupby(process_id_col):
            df_process = _df_process
            if CSV_IDX in df.columns:
                df_process = generate_datetime_dummy(df_process, process_id)
            df_process, *_ = join_mapping_process_data(db_instance, df_process, primary_group)
            idxs = df_process[INDEX_COL].to_list()
            df_process = (
                df_process.drop_duplicates(subset=unique_cols + [data_id_col])
                .pivot(index=unique_cols, columns=data_id_col, values=primary_group.DATA_VALUE)
                .rename({col: str(col) for col in df_process.columns})
                .reset_index()
            )
            yield process_id, df_process, idxs


@BridgeStationModel.use_db_instance()
def gen_import_data_files(
    cfg_data_table,
    process_id,
    df_process,
    binary_file_suffix,
    is_past,
    db_instance: PostgreSQL = None,
):
    df_process.drop_duplicates(inplace=True)
    chunk = len(df_process) // FEATHER_MAX_RECORD + 1
    for _df in np.array_split(df_process, chunk):
        gen_import_data_file(db_instance, cfg_data_table, process_id, is_past, _df, binary_file_suffix)


def drop_transaction_duplicates(df: DataFrame):
    cols = get_transaction_unique_cols()
    df = df.drop_duplicates(subset=cols)
    return df


def get_transaction_unique_cols(cfg_data_table):
    cols = [
        MappingFactoryMachine.factory_machine_id.name,
        PROC_PART_ID_COL,
    ]

    table_cols = cfg_data_table.columns
    for table_col in table_cols:
        if table_col.data_group_type == DataGroupType.DATA_SERIAL.value:
            cols.append(DataGroupType.DATA_SERIAL.name)

        if table_col.data_group_type == DataGroupType.DATA_TIME.value:
            cols.append(DataGroupType.DATA_TIME.name)

    return cols
