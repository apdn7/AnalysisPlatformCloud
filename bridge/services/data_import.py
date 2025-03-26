from __future__ import annotations

import os
import re
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Type, Union

import numpy as np
import pandas as pd
import pydantic
from apscheduler.triggers.date import DateTrigger
from dateutil import tz
from pandas import DataFrame
from pandas.core.dtypes.common import is_bool_dtype
from pytz import utc
from sqlalchemy.orm import scoped_session

from ap import dic_config, scheduler
from ap.api.setting_module.services.common import get_datetime_val
from ap.api.setting_module.services.process_data_count import save_proc_data_count
from ap.api.trace_data.services.proc_link import add_gen_proc_link_job, add_restructure_indexes_job
from ap.common.common_utils import (
    DATE_FORMAT_STR_ONLY_DIGIT,
    PostgresFormatStrings,
    check_exist,
    convert_int64_to_object,
    convert_nan_to_none,
    convert_time,
    convert_type_base_df,
    delete_file,
    get_basename,
    get_error_import_path,
    get_error_path,
    get_error_trace_path,
    get_files,
    get_import_transaction_future_path,
    get_import_transaction_past_path,
    get_ip_address,
    get_preview_data_file_folder,
    get_transaction_data_unknown_master_file_folder,
    is_boolean,
    is_int_16,
    is_int_32,
    is_int_64,
    make_dir_from_file_path,
    parse_int_value,
    read_feather_file,
    replace_dataframe_symbol,
    split_path_and_file_name,
    write_feather_file,
)
from ap.common.constants import (
    CATEGORY_ERROR_RESCHEDULE_TIME,
    CSV_HORIZONTAL_ROW_INDEX_COL,
    CSV_INDEX_COL,
    DATA_TYPE_ERROR_MSG,
    DATE_TYPE_REGEX,
    DATETIME,
    DATETIME_DUMMY,
    DEFAULT_NONE_VALUE,
    EMPTY_STRING,
    INDEX_COL,
    JOB_ID,
    LOCK,
    PAST_IMPORT_LIMIT_DATA_COUNT,
    PREVIEW_DATA_RECORDS,
    PROC_PART_ID_COL,
    PROCESS_QUEUE,
    TIME_TYPE_REGEX,
    AnnounceEvent,
    BooleanStringDefinition,
    CategoryErrorType,
    CRUDType,
    CsvDelimiter,
    DataGroupType,
    DataType,
    DBType,
    ErrorOutputMsg,
    ErrorType,
    FileExtension,
    JobStatus,
    JobType,
    ListenNotifyType,
    MasterDBType,
    RawDataTypeDB,
    ServerType,
    Suffixes,
)
from ap.common.datetime_format_utils import DateTimeFormatUtils
from ap.common.logger import log_execution_time, logger
from ap.common.pydn.dblib.db_proxy_readonly import ReadOnlyDbProxy
from ap.common.pydn.dblib.postgresql import PostgreSQL
from ap.common.scheduler import scheduler_app_context
from ap.common.services.normalization import normalize_df_multiple_columns
from ap.common.services.sse import background_announcer
from ap.common.timezone_utils import (
    add_days_from_utc,
    check_update_time_by_changed_tz,
    detect_timezone,
    gen_dummy_datetime,
    get_db_timezone,
    get_next_datetime_value,
    get_time_info,
)
from ap.setting_module.models import (
    CfgConstant,
    CfgDataTable,
    CfgDataTableColumn,
    CfgProcessColumn,
)
from ap.setting_module.models import SemiMaster as SemiMasterORM
from ap.setting_module.services.background_process import JobInfo, send_processing_info
from bridge.models.bridge_station import BridgeStationModel, TransactionModel
from bridge.models.cfg_data_source import CfgDataSource
from bridge.models.cfg_data_source_db import CfgDataSourceDB
from bridge.models.cfg_data_table import CfgDataTable as BSCfgDataTable
from bridge.models.cfg_process import gen_cfg_process
from bridge.models.cfg_process_column import gen_cfg_process_column
from bridge.models.m_data import MData
from bridge.models.m_data_group import MDataGroup as BSMDataGroup
from bridge.models.m_data_group import PrimaryGroup, get_primary_group
from bridge.models.m_process import MProcess
from bridge.models.m_unit import MUnit
from bridge.models.mapping_factory_machine import MappingFactoryMachine
from bridge.models.mapping_part import MappingPart
from bridge.models.mapping_process_data import MappingProcessData
from bridge.models.r_factory_machine import RFactoryMachine
from bridge.models.r_prod_part import RProdPart
from bridge.models.semi_master import SemiMaster
from bridge.models.t_proc_link_count import ProcLinkCount
from bridge.models.transaction_model import TransactionData
from bridge.redis_utils.db_changed import (
    ChangedType,
    publish_master_config_changed,
    publish_transaction_changed,
)
from bridge.services.transaction_data_import import (
    gen_transaction_partition_table,
    get_all_sensor_models,
)
from grpc_server.services.grpc_service_proxy import grpc_api

# index column in df

# file index col in df
FILE_IDX_COL = '__FILE_INDEX__'

# old_cycle_id
OLD_CYCLE_ID_COL = '__old_cycle_id__'

# N/A value lists
PANDAS_DEFAULT_NA = {
    '#N/A',
    '#N/A N/A',
    '#NA',
    '-1.#IND',
    '-1.#QNAN',
    '-NaN',
    '-nan',
    '1.#IND',
    '1.#QNAN',
    '<NA>',
    'N/A',
    'NA',
    'NULL',
    'NaN',
    'n/a',
    'nan',
    'null',
}
NA_VALUES = {'na', '-', '--', '---', '#NULL!', '#REF!', '#VALUE!', '#NUM!', '#NAME?', '0/0'}
INF_VALUES = {'Inf', 'Infinity', '1/0', '#DIV/0!'}
INF_NEG_VALUES = {'-Inf', '-Infinity', '-1/0'}

ALL_SYMBOLS = set(PANDAS_DEFAULT_NA | NA_VALUES | INF_VALUES | INF_NEG_VALUES)
SPECIAL_SYMBOLS = ALL_SYMBOLS - {'-'}
IS_ERROR_COL = '___ERR0R___'
DF_INDEX_COL = '___INDEX___'
CSV_IDX = '__CSV_IDX__'

MASTER_COL_TYPES = [
    DataGroupType.LINE_ID.value,
    DataGroupType.LINE_NAME.value,
    DataGroupType.PROCESS_ID.value,
    DataGroupType.PROCESS_NAME.value,
    DataGroupType.PART_NO.value,
    DataGroupType.EQUIP_ID.value,
    DataGroupType.EQUIP_NAME.value,
    DataGroupType.DATA_ID.value,
    DataGroupType.DATA_NAME.value,
]
DUPLICATE_MASTER_COL_TYPES = [
    DataGroupType.PROCESS_ID.value,
    DataGroupType.EQUIP_ID.value,
    DataGroupType.LINE_ID.value,
]  # show graph not show master PROCESS_ID, EQUIP_ID, LINE_ID

NOT_MASTER_COL_TYPES = [
    DataGroupType.DATA_SERIAL.value,
    DataGroupType.DATA_TIME.value,
    DataGroupType.AUTO_INCREMENTAL.value,
    DataGroupType.HORIZONTAL_DATA.value,
    DataGroupType.GENERATED.value,
    DataGroupType.DATA_TIME.value,
]


class CategoryDataError(pydantic.BaseModel):
    data_id: int
    error_type: CategoryErrorType

    @pydantic.field_serializer('error_type')
    def serialize_error_type(self, error_type: CategoryErrorType, _info):
        return error_type.name


class CategoryError(pydantic.BaseModel):
    process_id: int
    errors: list[CategoryDataError] = []

    def has_errors(self) -> bool:
        return len(self.errors) > 0

    def add_error(self, data_id: int, error_type: CategoryErrorType) -> None:
        self.errors.append(CategoryDataError(data_id=data_id, error_type=error_type))

    @property
    def announce_data(self) -> dict[str, Any]:
        return self.model_dump()

    def get_data_error(self, data_id: int) -> CategoryDataError | None:
        for error in self.errors:
            if error.data_id == data_id:
                return error
        return None


@log_execution_time()
def save_preview_data_file(
    db_instance: PostgreSQL,
    cfg_data_table_id: int,
    process_id: int,
    df_process: pd.DataFrame,
    binary_file_prefix: Optional[str] = None,
):
    increase_str = ''
    under_bar = ''  # df_sample_data = df_sample_data.replace({pd.NA: EMPTY_STRING})

    df_process = df_process.tail(PREVIEW_DATA_RECORDS)
    replace_dataframe_symbol(df_process)

    if INDEX_COL in df_process.columns:
        df_process.drop(INDEX_COL, axis=1, inplace=True)

    preview_data_file_names = get_preview_data_files(process_id)
    if preview_data_file_names:
        file_name = preview_data_file_names[-1]
        if is_file_created_after_process(
            db_instance=db_instance,
            file_name=file_name,
            process_id=process_id,
        ):
            df_preview_data = add_new_columns_from_other_df(
                df_old=pd.read_csv(file_name),
                df_new=df_process,
            )
            df_preview_data.to_csv(file_name)
            return

    sample_data_path = get_preview_data_file_folder(process_id)
    job_id = f'{process_id}_{cfg_data_table_id}'
    file_name = job_id if binary_file_prefix is None else f'{job_id}_{binary_file_prefix}'

    sample_data_file = os.path.join(sample_data_path, f'{file_name}{under_bar}{increase_str}.csv')
    make_dir_from_file_path(sample_data_file)
    df_process.to_csv(sample_data_file, index=False)


def add_new_columns_from_other_df(*, df_old: pd.DataFrame, df_new: pd.DataFrame) -> pd.DataFrame:
    new_columns = [col for col in df_new.columns if col not in df_old.columns]
    if not new_columns:
        return df_old
    return pd.concat([df_old, df_new[new_columns]], axis=1)


def is_file_created_after_process(
    *,
    db_instance: PostgreSQL,
    file_name: Optional[str, Path],
    process_id: int,
) -> bool:
    """
    Check if a file is created after process or not
    edge case: if process is None then the file is old
    :param db_instance:
    :param file_name:
    :param process_id:
    :return:
    """
    file = Path(file_name)
    if not file.exists():
        return False

    process = MProcess.get_by_id(db_instance=db_instance, id=process_id)
    if process is None:
        return False

    file_created_time = datetime.utcfromtimestamp(file.stat().st_ctime)  # noqa: DTZ004
    process_created_time: datetime = process.created_at
    return file_created_time > process_created_time


def get_transaction_file_folder(data_table_id, process_id, is_past=False):
    folder = get_import_transaction_past_path() if is_past else get_import_transaction_future_path()

    return os.path.join(folder, str(data_table_id), str(process_id))


def get_preview_data_files(process_id):
    files = []
    folder_path = get_preview_data_file_folder(process_id)
    if check_exist(folder_path):
        _files = get_files(folder_path, extension=['csv'])
        for _file in _files:
            files.append(_file)
    return files


def get_transaction_data_unknown_master_files(data_table_id: int) -> list[str]:
    """Collect all files that are transaction data with unknown master

    Args:
        data_table_id (int): the id of the data table

    Returns:
        list[str]: list of file full path
    """
    files: list[str] = []
    folder_path = get_transaction_data_unknown_master_file_folder(data_table_id)
    if check_exist(folder_path):
        _files = get_files(folder_path, extension=[FileExtension.Feather.value])
        for _file in _files:
            files.append(_file)
    return files


def get_import_files(process_id, data_table_id=None, is_past=False):
    data_table_ids = []
    if data_table_id:
        data_table_ids.append(data_table_id)
    else:
        cfg_data_tables = CfgDataTable.get_all_ids()
        data_table_ids = [cfg_data_table.id for cfg_data_table in cfg_data_tables]

    dict_file = {}
    for data_table_id in data_table_ids:
        files = []
        folder_path = get_transaction_file_folder(data_table_id, process_id, is_past)
        if check_exist(folder_path):
            files = get_files(folder_path, extension=['ftr', 'pck'])
            files = sorted(files) if files else []

        dict_file[data_table_id] = files

    return dict_file


@scheduler_app_context
def import_transaction_per_process_job(
    _job_id,
    _job_name,
    data_table_id,
    process_id,
    is_past=False,
    force_category_change: bool = False,
    register_by_file_request_id: str = None,
):
    # start job
    data_table_id_with_file_names = get_import_files(process_id, data_table_id, is_past)
    gen_proc_link_flag = None
    for data_table_id, file_names in data_table_id_with_file_names.items():
        CfgConstant.force_running_job()

        if not file_names:
            continue

        gen_proc_link_flag = True
        generator = import_trans_data_per_process_by_files(
            data_table_id,
            process_id,
            file_names,
            force_category_change,
            is_past=is_past,
            register_by_file_request_id=register_by_file_request_id,
        )
        send_processing_info(generator, _job_name, data_table_id=data_table_id, process_id=process_id)

    # call_pull_data_for_one_process(process_id)
    if gen_proc_link_flag:
        add_gen_proc_link_job(process_id)

    return True


def handle_category_error(category_error: CategoryError) -> None:
    # register jobs after 80 seconds, this time force change category
    trigger = DateTrigger(
        datetime.now().astimezone(utc) + timedelta(seconds=CATEGORY_ERROR_RESCHEDULE_TIME),
        timezone=utc,
    )
    for is_past in [True, False]:
        scheduler.modify_and_reschedule_job(
            id=JobType.transaction_import_job_id(category_error.process_id, is_past=is_past),
            trigger=trigger,
            force_category_change=True,
        )

    # notify front-end error
    background_announcer.announce(category_error.announce_data, AnnounceEvent.CATEGORY_ERROR.name)


def import_trans_data_per_process_by_files(
    data_table_id,
    process_id,
    file_names,
    force_category_change: bool = False,
    ignore_add_job: bool = False,
    is_past=False,
    register_by_file_request_id: str = None,
):
    yield 0
    # get job_id
    job_dict = {}
    yield job_dict
    job_id = job_dict.get(JOB_ID)

    # initialize job info
    job_info = JobInfo()
    job_info.job_id = job_id
    cfg_data_table = CfgDataTable.get_by_id(data_table_id)
    dummy_datetime_column = CfgProcessColumn.get_dummy_datetime_column(process_id)
    len_files = len(file_names)
    primary_group = get_primary_group()
    unique_keys = [
        primary_group.DATA_TIME,
        primary_group.DATA_SERIAL,
        MappingProcessData.Columns.data_id.name,
        MappingFactoryMachine.Columns.factory_machine_id.name,
        PROC_PART_ID_COL,
        RFactoryMachine.Columns.process_id.name,
    ]
    if cfg_data_table.is_has_serial_col:
        unique_keys.append(primary_group.DATA_SERIAL)

    job_info.percent = 10
    yield job_info.percent

    jump_percent = None
    is_first_file = True
    for file_num, file_name in enumerate(file_names, start=1):
        CfgConstant.force_running_job()

        job_info: JobInfo = import_trans_data_per_process(
            cfg_data_table,
            process_id,
            file_name,
            unique_keys,
            job_info,
            force_category_change,
            ignore_add_job=ignore_add_job,
            is_past=is_past,
        )

        if not jump_percent:
            jump_percent = file_num * 90 // len_files
        job_info.percent = job_info.percent + jump_percent
        job_info.status = JobStatus.DONE

        if register_by_file_request_id and is_first_file:
            data_register_data = {
                'RegisterByFileRequestID': register_by_file_request_id,
                'status': JobStatus.PROCESSING.name,
                'process_id': process_id,
                'is_first_imported': True,
                'use_dummy_datetime': bool(dummy_datetime_column),
            }
            background_announcer.announce(
                data_register_data,
                AnnounceEvent.DATA_REGISTER.name,
                f'{AnnounceEvent.DATA_REGISTER.name}_{process_id}',
            )
            is_first_file = False

        yield job_info

    if not ignore_add_job:
        publish_transaction_changed(process_id, ChangedType.TRANSACTION_IMPORT)

    yield 100


def import_trans_data_per_process(
    cfg_data_table: CfgDataTable,
    process_id,
    file_name,
    unique_keys,
    job_info,
    force_category_change: bool = False,
    ignore_add_job: bool = False,
    is_past: bool = False,
):
    """
    import columns: primary_group.DATA_SERIAL, primary_group.DATA_TIME, factory_machine_id_col, prod_part_id_col
                    DATA_NAME, DATA_VALUE

    :param cfg_data_table:
    :param process_id:
    :param file_name:
    :param unique_keys:
    :param job_info:
    :param is_past:
    :param force_category_change:
    :return:
    """
    with BridgeStationModel.get_db_proxy() as db_instance:
        transaction_data_obj = TransactionData(process_id, db_instance=db_instance)
        if transaction_data_obj.table_name not in db_instance.list_tables():
            transaction_data_obj.create_table(db_instance)

        if is_past and transaction_data_obj.data_count(db_instance) > PAST_IMPORT_LIMIT_DATA_COUNT:
            return job_info

        df = read_feather_file(file_name)
        if transaction_data_obj.getdate_column.column_name == DATETIME_DUMMY:
            df = generate_datetime_dummy_new(db_instance, df, transaction_data_obj)

        factory_machine_id_col = transaction_data_obj.factory_machine_id_col_name
        prod_part_id_col = transaction_data_obj.prod_part_id_col_name
        df = convert_int64_to_object(df, [factory_machine_id_col, prod_part_id_col])
        # data pre-processing
        cfg_process = transaction_data_obj.cfg_process
        process_columns = transaction_data_obj.cfg_process_columns
        if DataGroupType.DATA_TIME.name in df.columns:
            for col in process_columns:
                if col.column_name == DataGroupType.DATA_TIME.name:
                    df = df.rename(columns={DataGroupType.DATA_TIME.name: str(col.id)})

        df_columns = [str(col) for col in df.columns]
        dic_data_types = {
            process_column.id: process_column.raw_data_type
            for process_column in process_columns
            if str(process_column.id) in df_columns
        }
        is_csv_or_v2 = cfg_data_table.data_source.is_csv_or_v2()
        df, df_error = data_pre_processing(
            df,
            dic_data_types,
            is_csv_or_v2,
            datetime_format=cfg_process.datetime_format,
        )
        # make datetime main from date:main and time:main
        if transaction_data_obj.main_date_column and transaction_data_obj.main_time_column:
            merge_is_get_date_from_date_and_time(
                df,
                str(transaction_data_obj.getdate_column.id),
                str(transaction_data_obj.main_date_column.id),
                str(transaction_data_obj.main_time_column.id),
                is_csv_or_v2,
            )
        df, df_datetime_error = validate_datetime_data(df, str(transaction_data_obj.getdate_column.id))
        df_error = pd.concat([df_error, df_datetime_error])
        if not df_error.empty:
            dic_use_cols = {col.column_name: col.data_type for col in cfg_process.columns}
            df_error_trace = gen_error_output_df(dic_use_cols, df_error, df.head())
            write_error_trace(df_error_trace, cfg_process.name, file_path=file_name)
            write_error_import(df_error, cfg_process.name, file_path=file_name)
            logger.info(f'[CHECK_LOST_IMPORTED_DATA][{process_id}] Export error data type')

        # remove duplicate
        df_origin = df.copy()
        df = df.drop_duplicates(keep='last')
        df_duplicate: DataFrame = df_origin[~df_origin.index.isin(df.index)]
        if not df_duplicate.empty:
            now = datetime.now().strftime('%Y%m%d%H%M%S')
            duplicate_file_name = os.path.join(
                get_error_path(),
                cfg_process.name,
                f'{process_id}_{get_basename(file_name)}_{now}.csv',
            )
            make_dir_from_file_path(duplicate_file_name)
            df_duplicate.to_csv(duplicate_file_name, index=False)
            logger.info(f'[CHECK_LOST_IMPORTED_DATA][{process_id}][InFILE] Export duplicate data')

        # gen partition
        time_col = str(transaction_data_obj.getdate_column.id)
        gen_transaction_partition_table(db_instance, df, transaction_data_obj, time_col)

        # get data type
        # semi master
        from bridge.models.mapping_category_data import (
            gen_factor_value_not_mapping,
            transform_value_to_factor,
        )

        cat_counts = transaction_data_obj.get_count_by_category(db_instance)
        dic_cat_count = {str(_dic_cat['data_id']): _dic_cat for _dic_cat in cat_counts}

        category_errors = CategoryError(process_id=process_id)
        for _data_id, raw_data_type in dic_data_types.items():
            data_id = str(_data_id)
            if RawDataTypeDB.is_category_data_type(raw_data_type) and data_id in df.columns:
                error_type = CategoryErrorType.get_error_type_from_count(
                    unique_count=dic_cat_count[data_id]['unique_count'],
                    df_unique_count=df[data_id].nunique(),
                )
                if error_type is not None:
                    category_errors.add_error(data_id=_data_id, error_type=error_type)

        if not force_category_change and category_errors.has_errors():
            dic_config[PROCESS_QUEUE][ListenNotifyType.CATEGORY_ERROR.name][process_id] = category_errors
            raise ValueError('Category unique value exceed')

        lock = dic_config[PROCESS_QUEUE][LOCK]
        for _data_id, raw_data_type in dic_data_types.items():
            data_id = str(_data_id)
            if RawDataTypeDB.is_category_data_type(raw_data_type) and data_id in df.columns:
                # convert data from category to normal data
                dic_cat_detail = dic_cat_count[data_id]
                category_error = category_errors.get_data_error(data_id=_data_id)
                if category_error is None:
                    with lock:
                        gen_factor_value_not_mapping(db_instance, df[data_id], data_id, cfg_data_table.id)

                    df[data_id] = transform_value_to_factor(db_instance, _data_id, df[data_id], cfg_data_table.id)
                else:
                    if category_error.error_type is CategoryErrorType.OLD_UNIQUE_VALUE_EXCEED:
                        with lock:
                            transaction_data_obj.convert_category_to_normal_transaction_data(
                                db_instance,
                                dic_cat_detail['data_id'],
                                dic_cat_detail['col_name'],
                                dic_cat_detail['group_id'],
                            )
                    elif category_error.error_type is CategoryErrorType.NEW_UNIQUE_VALUE_EXCEED:
                        with lock:
                            transaction_data_obj.convert_category_to_normal_transaction_data_first_time(
                                db_instance,
                                dic_cat_detail['data_id'],
                                dic_cat_detail['col_name'],
                            )
                    else:
                        raise NotImplementedError(f'Not implemented yet for {category_error.error_type}')

                    if not ignore_add_job:
                        add_restructure_indexes_job(process_id)

        start_tm, end_tm = df[time_col].min(), df[time_col].max()
        inserted_count, inserted_id, insert_df = transaction_data_obj.import_data(db_instance, df, cfg_data_table)
        save_proc_data_count(
            db_instance,
            insert_df,
            process_id,
            transaction_data_obj.getdate_column.bridge_column_name,
            job_info.job_id,
        )

        # # create indexes again
        # link_keys = transaction_data_obj.add_default_indexes_column()
        # transaction_data_obj.create_index(db_instance, new_link_key_indexes=link_keys)

    # delete file name
    delete_file(file_name)

    job_info = gen_import_trans_history(
        cfg_data_table,
        process_id,
        job_info,
        inserted_id,
        inserted_count,
        start_tm,
        end_tm,
        target_files=None,
    )
    job_info.detail = f'{start_tm} - {end_tm} (UTC)'
    return job_info


def merge_is_get_date_from_date_and_time(df, get_date_col, date_main_col, time_main_col, is_csv_or_v2):
    from ap.api.setting_module.services.data_import import convert_df_col_to_utc
    from bridge.services.etl_services.etl_import import remove_timezone_inside

    series_x = df[date_main_col]
    series_y = df[time_main_col]
    is_x_string = not pd.api.types.is_datetime64_any_dtype(series_x)
    is_y_string = not pd.api.types.is_datetime64_any_dtype(series_y)

    result_format = f'{PostgresFormatStrings.DATE.value}{PostgresFormatStrings.TIME.value}'

    # extract date format
    if not is_x_string:
        series_x = series_x.dt.strftime(PostgresFormatStrings.DATE.value)

    # extract time format
    if not is_y_string:
        series_y = series_y.dt.strftime(PostgresFormatStrings.TIME.value)

    get_date_series = pd.to_datetime(
        series_x + series_y,
        format=result_format,
        exact=True,
        errors='coerce',
    )
    df[get_date_col] = get_date_series
    # convert csv timezone
    if is_csv_or_v2:  # TODO: Confirm convert db timezone
        datetime_val = get_datetime_val(df[get_date_col])
        is_timezone_inside, csv_timezone, utc_offset = get_time_info(datetime_val, None)
        df[get_date_col] = convert_df_col_to_utc(df, get_date_col, is_timezone_inside, csv_timezone, utc_offset)
        df[get_date_col] = remove_timezone_inside(df[get_date_col], is_timezone_inside)


def validate_datetime_data(df: DataFrame, datetime_column: str) -> Tuple[DataFrame, DataFrame]:
    df_null_datetime: DataFrame = df[df[datetime_column].isnull()]
    df = df[~df.index.isin(df_null_datetime.index)]
    return df, df_null_datetime


def merge_existing_data(df_group, df_db_origin, df_cycle_pairs):
    if df_db_origin is None or df_db_origin.empty:
        return df_group

    # constants
    cycle_id_col = TransactionModel.Columns.cycle_id.name
    df_merge = df_group.merge(df_cycle_pairs, how='left', on=cycle_id_col)
    df_merge = df_merge.set_index(OLD_CYCLE_ID_COL)
    df_merge = df_merge.combine_first(df_db_origin.set_index(cycle_id_col))
    df_merge.dropna(axis=1, how='all', inplace=True)
    df_merge.dropna(subset=[cycle_id_col], inplace=True)
    df_merge.drop_duplicates(subset=[cycle_id_col], inplace=True)

    return df_merge


def insert_semi_master(db_instance: Union[PostgreSQL, scoped_session], df, select_cols: list[str] = None):
    if df.empty:
        return

    if not select_cols:
        select_cols = [col for col in SemiMaster.Columns.get_column_names() if col in df.columns]

    df = df[select_cols]

    if isinstance(db_instance, scoped_session):
        SemiMasterORM.insert_records(select_cols, df.values.tolist(), db_instance)
    else:
        rows = df[select_cols].values.tolist()
        db_instance.bulk_insert(SemiMaster.get_table_name(), select_cols, rows)


@log_execution_time()
def convert_v2_history_to_vertical_holding(df):
    df[DataGroupType.SUB_PART_NO.name] = df[DataGroupType.SUB_PART_NO.name].str[2:8]

    index_cols = [
        col
        for col in df.columns
        if col
        not in [
            DataGroupType.SUB_PART_NO.name,
            DataGroupType.SUB_TRAY_NO.name,
            DataGroupType.SUB_LOT_NO.name,
            DataGroupType.SUB_SERIAL.name,
        ]
    ]
    pivot_columns = [DataGroupType.SUB_PART_NO.name]
    pivot_values = [
        DataGroupType.SUB_TRAY_NO.name,
        DataGroupType.SUB_LOT_NO.name,
        DataGroupType.SUB_SERIAL.name,
    ]

    # Fix bug that columns do not exist in df
    for col in pivot_values:
        if col not in df:
            df[col] = DEFAULT_NONE_VALUE

    df_pivot = df.pivot(index=index_cols, columns=pivot_columns, values=pivot_values)

    df_pivot.dropna(axis=1, how='all', inplace=True)
    df_pivot.columns = df_pivot.columns.to_flat_index()
    dict_rename = {(main_col, part_no): f'{main_col}_{part_no}' for main_col, part_no in df_pivot.columns}
    df_pivot.rename(columns=dict_rename, inplace=True)
    df_pivot = df_pivot.reset_index(drop=False)
    # => Convert to logical columns - END
    # => From logical columns (horizontal type), convert to vertical type with below structure
    # logical column names are stored in 'DATA_ID' column
    # logical column value are stored in 'DATA_VALUE' column
    sub_cols = [col for col in df_pivot.columns.tolist() if col not in index_cols]
    df_pivot = df_pivot.melt(
        id_vars=index_cols,
        value_vars=sub_cols,
        var_name=DataGroupType.DATA_ID.name,
        value_name=DataGroupType.DATA_VALUE.name,
    )

    df_pivot[DataGroupType.DATA_NAME.name] = df_pivot[DataGroupType.DATA_ID.name]
    df_pivot[DataGroupType.DATA_VALUE.name] = df_pivot[DataGroupType.DATA_VALUE.name]
    # df_pivot['DATA_ID'] = df_pivot[DataGroupType.SUB_PART_NO.name]

    df_pivot = df_pivot[~df_pivot[DataGroupType.DATA_VALUE.name].isna()]
    df_pivot.reset_index(drop=True, inplace=True)
    return df_pivot.convert_dtypes()


@log_execution_time()
def data_pre_processing(
    df,
    dic_data_types,
    is_csv_or_v2: bool,
    datetime_format: Optional[str] = None,
) -> Tuple[DataFrame, DataFrame]:
    """
    This is the function that preprocesses the data in a dataFrame

    It will check all the values in the dataFrame to see if they are valid.
    If a value of the wrong data type exists, the row containing that value
    is invalid and will be removed from the input dataFrame.

    Parameters
    ----------
    df : dataFrame
        In case of CSV import, there are rows of many files in dataFrame that store all data rows.
    dic_data_types : dic_data_types
        dictionary of all using columns that mean columns will be check in this function.
    datetime_format : Optional[str]
        a datetime format string of process.

    Returns
    -------
    DataFrame of all invalid rows.
    """

    na_values = PANDAS_DEFAULT_NA | NA_VALUES

    # string parse
    cols = get_string_cols(df)

    # normalization
    normalize_df_multiple_columns(df, cols)

    df = convert_datetime_format(df, dic_data_types, datetime_format)
    # convert timezone
    for col_id, raw_data_type in dic_data_types.items():
        # TODO: handle converting timezone for data from database
        if raw_data_type == RawDataTypeDB.DATETIME.value and is_csv_or_v2:
            convert_csv_timezone_per_process(df, str(col_id))

    # parse data type
    df, df_error = validate_data_type(df, dic_data_types, na_values=na_values)  # type: DataFrame, DataFrame

    return df, df_error


def convert_csv_timezone_per_process(df, col, is_convert_dtype_string: bool = True):
    from ap.api.setting_module.services.data_import import convert_df_col_to_utc
    from bridge.services.etl_services.etl_import import remove_timezone_inside

    datetime_val = get_datetime_val(df[col])
    is_timezone_inside, csv_timezone, utc_offset = get_time_info(datetime_val, None)
    df[col] = convert_df_col_to_utc(df, col, is_timezone_inside, csv_timezone, utc_offset)
    df[col] = remove_timezone_inside(df[col], True)
    if is_convert_dtype_string:
        df[col] = df[col].dt.strftime(PostgresFormatStrings.DATETIME.value).astype(pd.StringDtype())


def convert_db_timezone_per_process(df, dic_tz_info, col):
    from ap.api.setting_module.services.data_import import convert_df_col_to_utc
    from bridge.services.etl_services.etl_import import remove_timezone_inside

    validate_datetime(df, col, is_strip=False, add_is_error_col=False)
    is_tz_inside, db_time_zone, time_offset = dic_tz_info[col]
    df[col] = convert_df_col_to_utc(df, col, is_tz_inside, db_time_zone, time_offset)
    df[col] = remove_timezone_inside(df[col], is_tz_inside)


def handle_db_timezone(cfg_data_table: CfgDataTable, get_date_col, df):
    with ReadOnlyDbProxy(cfg_data_table.data_source) as db_instance:
        dt_input = df[get_date_col][0]
        # get timezone offset
        db_timezone = get_db_timezone(db_instance)
        # get tzoffset_str
        tzoffset_str = None
        if tzoffset_str:
            # use os time zone
            db_timezone = None
        else:
            detected_timezone = detect_timezone(dt_input)
            # if there is time offset in datetime value, do not force time.
            if detected_timezone is None:
                # check and update if you use os time zone flag changed
                # if tz offset in val date, do not need to force
                check_update_time_by_changed_tz(cfg_data_table)
        if cfg_data_table.data_source.db_detail.use_os_timezone:
            # use os time zone
            db_timezone = None

        is_tz_inside, db_time_zone, time_offset = get_time_info(dt_input, db_timezone)
        return is_tz_inside, db_time_zone, time_offset


# write to error/trace folder
@log_execution_time()
def write_error_trace(df_error: DataFrame, proc_name, file_path=None):
    if df_error.empty:
        return

    time_str = convert_time(datetime.now(), format_str=DATE_FORMAT_STR_ONLY_DIGIT)[4:-3]
    ip_address = get_ip_address()
    ip_address = f'_{ip_address}' if ip_address else ''
    base_name = f'_{get_basename(file_path)}' if file_path else ''
    file_name = f'{proc_name}{base_name}_{time_str}{ip_address}.txt'
    full_path = os.path.join(get_error_trace_path(), file_name)
    make_dir_from_file_path(full_path)

    df_error.to_csv(full_path, sep=CsvDelimiter.TSV.value, header=False, index=False)


@log_execution_time()
def write_error_import(
    df_error: DataFrame,
    proc_name,
    file_path=None,
    error_file_delimiter=CsvDelimiter.CSV,
    cfg_csv_folder=None,
    file_extension=None,
):
    if df_error.empty:
        return

    if not file_extension:
        file_extension = error_file_delimiter.name.lower()

    if file_path:
        csv_folder, csv_file_name = split_path_and_file_name(file_path)
        file_name = f'{csv_file_name}.csv'
        # cfg_csv_folder : config folder
        # csv_folder : actual folder
        folders = csv_folder.split('\\')[len(cfg_csv_folder.split('\\')) :] if cfg_csv_folder else []
    else:
        time_str = convert_time(format_str=DATE_FORMAT_STR_ONLY_DIGIT)[4:-3]
        file_name = f'{time_str}.{file_extension}'
        folders = []

    full_path = os.path.join(get_error_import_path(), proc_name, *folders, file_name)
    make_dir_from_file_path(full_path)
    df_error.to_csv(full_path, sep=error_file_delimiter.value, index=False)


def return_inf_vals(data_type):
    if data_type == DataType.REAL.name:
        return np.nan, float('-inf'), float('inf')
    elif data_type == DataType.INTEGER.name:
        return DEFAULT_NONE_VALUE, DEFAULT_NONE_VALUE, DEFAULT_NONE_VALUE

    return None, '-inf', 'inf'


@log_execution_time('[DATA VALIDATION]')
def validate_data(df: DataFrame, dic_use_cols, na_vals, exclude_cols=None):
    """
    validate data type, NaN values...
    :param df:
    :param dic_use_cols:
    :param na_vals:
    :param exclude_cols:
    :return:
    """

    if IS_ERROR_COL not in df.columns:
        df[IS_ERROR_COL] = 0

    if exclude_cols is None:
        exclude_cols = []

    exclude_cols.append(IS_ERROR_COL)

    # string + object + category
    float_cols = df.select_dtypes(include=['float']).columns.tolist()
    int_cols = df.select_dtypes(include=['integer']).columns.tolist()
    for col_name in df.columns:
        if col_name in exclude_cols:
            continue

        if col_name not in dic_use_cols:
            continue

        # do nothing with int column
        if col_name in int_cols:
            # in case data type of column is {DataType.INTEGER}, it will be verified int32
            # if user's choice column is {DataType.TEXT}, skip verify step
            if dic_use_cols[col_name] == DataType.INTEGER.name:
                determine_invalid_int32_in_df(df, col_name)
            continue

        # data type that user chose
        user_data_type = dic_use_cols[col_name]

        # do nothing with float column
        if col_name in float_cols and user_data_type != DataType.INTEGER.name:
            continue

        # convert inf , -inf to Nan
        nan, inf_neg_val, inf_val = return_inf_vals(user_data_type)
        if col_name in float_cols and user_data_type == DataType.INTEGER.name:
            df.loc[df[col_name].isin([float('inf'), float('-inf')]), col_name] = nan
            non_na_vals = df[col_name].dropna()
            if len(non_na_vals):
                df.loc[non_na_vals.index, col_name] = df.loc[non_na_vals.index, col_name].astype(int)

            continue

        # strip quotes and spaces
        dtype_name = df[col_name].dtype.name
        if user_data_type in [DataType.INTEGER.name, DataType.REAL.name]:
            vals = df[col_name].copy()

            # convert numeric values
            numerics = pd.to_numeric(vals, errors='coerce')
            df[col_name] = numerics

            # strip quote space then convert non numeric values
            non_num_idxs = numerics.isna()
            non_numerics = vals.loc[non_num_idxs].dropna()
            if len(non_numerics):
                non_num_idxs = non_numerics.index
                non_numerics = non_numerics.astype(str).str.strip("'").str.strip()

                # convert non numeric again
                numerics = pd.to_numeric(non_numerics, errors='coerce')
                df.loc[non_num_idxs, col_name] = numerics

                # set error for non numeric values
                non_num_idxs = numerics.isna()
                for idx, is_true in non_num_idxs.items():
                    if not is_true:
                        continue

                    if vals.at[idx] in na_vals:
                        df.at[idx, col_name] = nan
                    elif vals.at[idx] in INF_VALUES:
                        df.at[idx, col_name] = inf_val
                    elif vals.at[idx] in INF_NEG_VALUES:
                        df.at[idx, col_name] = inf_neg_val
                    else:
                        df.at[idx, IS_ERROR_COL] = 1

                try:
                    if len(non_num_idxs):
                        pd.to_numeric(df.loc[non_num_idxs.index, col_name], errors='raise')
                except Exception as ex:
                    logger.exception(ex)

            # replace Inf --> None
            if user_data_type == DataType.INTEGER.name:
                df.loc[df[col_name].isin([float('inf'), float('-inf')]), col_name] = nan
                determine_invalid_int32_in_df(df, col_name)

        elif user_data_type == DataType.TEXT.name:
            idxs = df[col_name].dropna().index
            if dtype_name == 'object':
                df.loc[idxs, col_name] = df.loc[idxs, col_name].astype(str).str.strip("'").str.strip()
            elif dtype_name == 'string':
                df.loc[idxs, col_name] = df.loc[idxs, col_name].str.strip("'").str.strip()
            elif dtype_name == 'boolean':
                df[col_name] = df[col_name].astype(str)
            else:
                # convert to string before insert to database
                df.loc[idxs, col_name] = df.loc[idxs, col_name].astype(str)
                continue

            if len(idxs):
                conditions = [
                    df[col_name].isin(na_vals),
                    df[col_name].isin(INF_VALUES),
                    df[col_name].isin(INF_NEG_VALUES),
                ]
                return_vals = [nan, inf_val, inf_neg_val]

                df[col_name] = np.select(conditions, return_vals, df[col_name])


def set_error_msg_job_info(job_info: JobInfo, err_cnt, err_msgs=None):
    """
    save error msg to job info
    :param job_info:
    :param err_msgs:
    :param err_cnt:
    :return:
    """
    default_msg = ErrorOutputMsg[str(ErrorType.DataError)].value
    if not err_msgs:
        msg = default_msg
    elif isinstance(err_msgs, (list, tuple)):
        msg = ','.join(err_msgs)
    else:
        msg = err_msgs

    if job_info.err_msg:
        job_info.err_msg += msg
    else:
        job_info.err_msg = msg

    job_info.data_type_error_cnt += err_cnt
    return job_info


def gen_import_job_info(
    job_info,
    save_res,
    auto_increment_start_tm=None,
    auto_increment_end_tm=None,
    err_msgs=None,
    err_cnt=None,
    cycle_start_tm=None,
    cycle_end_tm=None,
    imported_cycle=None,
):
    # auto incremental
    job_info.auto_increment_start_tm = auto_increment_start_tm
    job_info.auto_increment_end_tm = auto_increment_end_tm

    # cycle time
    job_info.cycle_start_tm = cycle_start_tm
    job_info.cycle_end_tm = cycle_end_tm
    job_info.imported_cycle_id = imported_cycle

    # add new errors
    if err_msgs:
        if isinstance(err_msgs, str):
            err_msgs = [err_msgs]

        job_info.err_msg = ', '.join(err_msgs)

        # limit 1000 character
        job_info.err_msg = job_info.err_msg[0:1000]

    if err_cnt:
        job_info.data_type_error_cnt += err_cnt

    if isinstance(save_res, Exception):
        job_info.exception = save_res
        job_info.status = JobStatus.FATAL
    elif job_info.err_msg:
        job_info.status = JobStatus.FAILED
    else:
        job_info.status = JobStatus.DONE

    if isinstance(save_res, int):
        job_info.row_count = save_res
        job_info.committed_count = save_res

    return job_info


def get_string_cols(df: DataFrame):
    cols = df.select_dtypes(include=['string', 'object']).columns.tolist()
    return cols


@log_execution_time()
def validate_datetime(df: DataFrame, col_name, is_strip=True, add_is_error_col=True, null_is_error=True):
    dtype_name = df[col_name].dtype.name
    is_number = False
    if dtype_name == 'object':
        df[col_name] = df[col_name].astype(str)
    elif 'int' in dtype_name.lower():
        df[col_name] = df[col_name].astype(str)
        is_number = True
    elif 'float' in dtype_name.lower():
        df[col_name] = df[col_name].astype(int)
        df[col_name] = df[col_name].astype(str)
        is_number = True
    elif dtype_name != 'string':
        return

    if IS_ERROR_COL not in df.columns:
        df[IS_ERROR_COL] = 0

    if is_strip and not is_number:
        df[col_name] = df[col_name].str.strip("'").str.strip()

    # convert to datetime value
    idxs = None
    if not null_is_error:
        idxs = df[col_name].notna()

    df[col_name] = pd.to_datetime(df[col_name], errors='coerce')  # failed records -> pd.NaT
    # df[IS_ERROR_COL] = np.where(pd.isna(df[col_name]), 1, df[IS_ERROR_COL])
    # mark error records
    if add_is_error_col:
        init_is_error_col(df)

        if null_is_error or idxs is None:
            df[IS_ERROR_COL] = np.where(pd.isna(df[col_name]), 1, df[IS_ERROR_COL])
        else:
            df_temp = df.loc[idxs, [col_name, IS_ERROR_COL]]
            # df.loc[idxs, IS_ERROR_COL] = np.where(pd.isna(df.loc[idxs, date_col]), 1, df.loc[idxs, IS_ERROR_COL])
            df_temp[IS_ERROR_COL] = np.where(pd.isna(df_temp[col_name]), 1, df_temp[IS_ERROR_COL])
            df.loc[idxs, IS_ERROR_COL] = df_temp


def init_is_error_col(df: DataFrame):
    if IS_ERROR_COL not in df.columns:
        df[IS_ERROR_COL] = 0


@log_execution_time('[CONVERT DATE TIME TO UTC')
def convert_df_col_to_utc(df, get_date_col, is_timezone_inside, db_time_zone, utc_time_offset):
    if DATETIME not in df[get_date_col].dtype.name:
        # create datetime column in df
        # if data has tz info, convert to utc
        df[get_date_col] = pd.to_datetime(df[get_date_col], errors='coerce', utc=is_timezone_inside)

    if not db_time_zone:
        db_time_zone = tz.tzlocal()

    local_dt = df[df[get_date_col].notnull()][get_date_col]
    # return if there is utc
    if not utc_time_offset:
        # utc_offset = 0
        return local_dt

    if not local_dt.dt.tz:
        # utc_time_offset = 0: current UTC
        # cast to local before convert to utc
        local_dt = local_dt.dt.tz_localize(tz=db_time_zone, ambiguous='infer')
    return local_dt.dt.tz_convert(tz.tzutc())


def get_object_cols(df: DataFrame):
    return [col for col in df.columns if df[col].dtype.name.lower() == 'object']


def determine_invalid_int32_in_df(df: DataFrame, col_name: str):
    # df[IS_ERROR_COL] = np.where(~df[col_name].isnull() & is_int_64(df[col_name]), 1, df[IS_ERROR_COL])
    df[IS_ERROR_COL] = np.where(df[col_name].isnull() | is_int_32(df[col_name]), df[IS_ERROR_COL], 1)


@log_execution_time()
def write_duplicate_to_file(
    df_duplicate: DataFrame,
    process_name,
    job_id,
    data_source_name=None,
    csv_file_name=None,
    ip_address=None,
):
    """
    Export duplicate data to text file with below setting:

    - File name format: ProcessName_DataSourceName_CsvName_ErrorType(Duplicate)_JobID_mmddhhmmss.sss_IP.txt
    - Directory: ~/error/import/

    In case of empty input data, no export file

    :param df_duplicate: dataFrame contain all duplicate records that want to export to file
    :param process_name: process name
    :param job_id: job id of importing csv
    :param data_source_name: data source name that process belongs to
    :param csv_file_name: csv file name that contain duplicate records
    :param ip_address: ip address of Edge Server that send request to import csv files
    """
    if not len(df_duplicate):
        return df_duplicate

    # prepare necessary params
    time_str = convert_time(format_str=DATE_FORMAT_STR_ONLY_DIGIT)[4:-3]
    base_name = split_path_and_file_name(csv_file_name)[1] if csv_file_name else ''
    # make folder
    file_name_elements = [
        process_name,
        data_source_name,
        base_name,
        'Duplicate',
        str(job_id),
        time_str,
        ip_address,
    ]
    file_name = '_'.join([element for element in file_name_elements if element])
    export_file_name = f'{file_name}.txt'
    full_path = os.path.join(get_error_import_path(), export_file_name)
    make_dir_from_file_path(full_path)

    # export to file
    df_duplicate.to_csv(full_path, sep=CsvDelimiter.TSV.value, index=False)


def get_transaction_by_range_time(db_instance, process_id, start_tm, end_tm, cycle_ids=None):
    sensor_models = get_all_sensor_models(ignore_t_master_data=True)
    dic_transaction_data = defaultdict(dict)
    dic_col_groups = {
        rec['col_group_id']: rec['data_type'] for rec in []
    }  # ColumnGroup.get_column_groups_by_process_id(db_instance, process_id)}
    for model_cls in sensor_models:
        cols, rows = model_cls.get_records_by_range_time(db_instance, process_id, start_tm, end_tm)
        if not rows:
            continue

        df = pd.DataFrame(columns=cols, data=rows)
        if cycle_ids:
            df = df[df[model_cls.Columns.cycle_id.name].isin(cycle_ids)]

        for col_group_id, df_sub in df.groupby(model_cls.Columns.col_group_id.name):
            data_type = dic_col_groups[col_group_id]
            dic_transaction_data[data_type][col_group_id] = df_sub

    return dic_transaction_data


def gen_duplicate_output_df(dic_use_cols, df_error, csv_file_name=None, table_name=None, error_msgs=None):
    # db_name: if factory db -> db name
    #                           else if csv -> file name
    columns = df_error.columns.tolist()

    # error data
    new_row = columns
    df_db = add_row_to_df(df_error, columns, new_row)

    new_row = (f'column name/duplicate data (total: {len(df_error)} rows)',)
    df_db = add_row_to_df(df_db, columns, new_row)

    new_row = ('',)
    df_db = add_row_to_df(df_db, columns, new_row)

    new_row = [dic_use_cols[col_name] for col_name in columns if col_name in dic_use_cols]
    df_db = add_row_to_df(df_db, columns, new_row)

    new_row = ('data type',)
    df_db = add_row_to_df(df_db, columns, new_row)

    new_row = ('',)
    df_db = add_row_to_df(df_db, columns, new_row)

    if csv_file_name:
        new_row = ('Data File', csv_file_name)
        df_db = add_row_to_df(df_db, columns, new_row)

    if table_name:
        new_row = ('Table name', table_name)
        df_db = add_row_to_df(df_db, columns, new_row)

    new_row = ('',)
    df_db = add_row_to_df(df_db, columns, new_row)

    new_row = ('',)
    df_db = add_row_to_df(df_db, columns, new_row)

    error_msg = '|'.join(error_msgs) if isinstance(error_msgs, (list, tuple)) else error_msgs

    new_row = ('Error Type', error_msg or str(ErrorOutputMsg.DuplicateError))
    df_db = add_row_to_df(df_db, columns, new_row)

    return df_db


def gen_error_output_df(
    dic_use_cols,
    df_error,
    db_latest_records: [Dict, DataFrame],
    csv_file_name=None,
    table_name=None,
    error_msgs=None,
):
    if isinstance(db_latest_records, Dict):
        df_db = pd.DataFrame(db_latest_records)
    elif isinstance(db_latest_records, DataFrame) and len(db_latest_records):
        df_db = db_latest_records.copy()
    else:
        df_db = pd.DataFrame(columns=list(dic_use_cols))

    if INDEX_COL in df_db.columns:
        df_db.drop(INDEX_COL, axis=1, inplace=True)

    db_len = len(df_db)
    df_db = df_db.append(df_error, ignore_index=True)
    columns = df_db.columns.tolist()

    # error data
    new_row = columns
    df_db = add_row_to_df(df_db, columns, new_row, db_len)

    new_row = ('column name/sample data (first 10 & last 10)',)
    df_db = add_row_to_df(df_db, columns, new_row, db_len)

    if csv_file_name:
        new_row = ('Data File', csv_file_name)
        df_db = add_row_to_df(df_db, columns, new_row, db_len)

    if table_name:
        new_row = ('Table name', table_name)
        df_db = add_row_to_df(df_db, columns, new_row, db_len)

    new_row = ('',)
    df_db = add_row_to_df(df_db, columns, new_row, db_len)

    # data in db
    new_row = columns
    df_db = add_row_to_df(df_db, columns, new_row)

    new_row = ('column name/sample data (latest 5)',)
    df_db = add_row_to_df(df_db, columns, new_row)

    new_row = [dic_use_cols[col_name] for col_name in columns if col_name in dic_use_cols]
    df_db = add_row_to_df(df_db, columns, new_row)

    new_row = ('data type',)
    df_db = add_row_to_df(df_db, columns, new_row)

    new_row = ('',)
    df_db = add_row_to_df(df_db, columns, new_row)

    new_row = ('',)
    df_db = add_row_to_df(df_db, columns, new_row)

    error_msg = '|'.join(error_msgs) if isinstance(error_msgs, (list, tuple)) else error_msgs

    new_row = ('Error Type', error_msg or DATA_TYPE_ERROR_MSG)
    df_db = add_row_to_df(df_db, columns, new_row)

    return df_db


def add_row_to_df(df, columns, new_row, pos=0):
    df_temp = pd.DataFrame({columns[i]: new_row[i] for i in range(len(new_row))}, index=[pos])
    df = pd.concat([df.iloc[0:pos], df_temp, df.iloc[pos:]]).reset_index(drop=True)

    return df


@grpc_api()
@log_execution_time()
def check_db_con(db_type, host, port, dbname, schema, username, password):
    # 戻り値の初期化
    result = False

    parsed_int_port = parse_int_value(port)
    if parsed_int_port is None and db_type.lower() != DBType.SQLITE.name.lower():
        return result

    # 　オブジェクトを初期化する
    dic_data_source_db = {
        'host': host,
        'port': parsed_int_port,
        'dbname': dbname,
        'schema': schema,
        'username': username,
        'password': password,
    }
    data_source_db = CfgDataSourceDB(dic_data_source_db)

    # ローカルホストの場合はホスト構成を交換する
    if data_source_db.host == data_source_db.localhost_alias.get(ServerType.EdgeServer):
        data_source_db.host = data_source_db.localhost_alias.get(ServerType.BridgeStationGrpc)

    dic_data_source = {'db_detail': data_source_db, 'type': db_type.upper() if db_type else db_type}
    data_source = CfgDataSource(dic_data_source)
    data_source_db.cfg_data_source = data_source

    # コネクションをチェックする
    with ReadOnlyDbProxy(data_source_db) as factory_db_instance:
        result = factory_db_instance.is_connected

    return result


@log_execution_time()
def join_master_data_common(
    df: DataFrame,
    db_instance: PostgreSQL,
    primary_groups: PrimaryGroup,
    df_mapping_factory_machine=None,
    df_mapping_part=None,
    df_r_prod_part=None,
    df_r_factory=None,
    is_v2_history: bool = False,
):
    unique_col = '__CHECK_UNIQUE__'
    df[unique_col] = df.index

    dic_mapping_factory = {
        MappingFactoryMachine.Columns.t_location_name.name: primary_groups.LOCATION_NAME,
        MappingFactoryMachine.Columns.t_location_abbr.name: primary_groups.LOCATION_ABBR,
        MappingFactoryMachine.Columns.t_factory_id.name: primary_groups.FACTORY_ID,
        MappingFactoryMachine.Columns.t_factory_name.name: primary_groups.FACTORY_NAME,
        MappingFactoryMachine.Columns.t_factory_abbr.name: primary_groups.FACTORY_ABBR,
        MappingFactoryMachine.Columns.t_plant_id.name: primary_groups.PLANT_ID,
        MappingFactoryMachine.Columns.t_plant_name.name: primary_groups.PLANT_NAME,
        MappingFactoryMachine.Columns.t_plant_abbr.name: primary_groups.PLANT_ABBR,
        MappingFactoryMachine.Columns.t_dept_id.name: primary_groups.DEPT_ID,
        MappingFactoryMachine.Columns.t_dept_name.name: primary_groups.DEPT_NAME,
        MappingFactoryMachine.Columns.t_dept_abbr.name: primary_groups.DEPT_ABBR,
        MappingFactoryMachine.Columns.t_sect_id.name: primary_groups.SECT_ID,
        MappingFactoryMachine.Columns.t_sect_name.name: primary_groups.SECT_NAME,
        MappingFactoryMachine.Columns.t_sect_abbr.name: primary_groups.SECT_ABBR,
        MappingFactoryMachine.Columns.t_line_id.name: primary_groups.LINE_ID,
        MappingFactoryMachine.Columns.t_line_no.name: primary_groups.LINE_NO,
        MappingFactoryMachine.Columns.t_line_name.name: primary_groups.LINE_NAME,
        MappingFactoryMachine.Columns.t_outsource.name: primary_groups.OUTSOURCE,
        MappingFactoryMachine.Columns.t_equip_id.name: primary_groups.EQUIP_ID,
        MappingFactoryMachine.Columns.t_equip_name.name: primary_groups.EQUIP_NAME,
        MappingFactoryMachine.Columns.t_equip_product_no.name: primary_groups.EQUIP_PRODUCT_NO,
        MappingFactoryMachine.Columns.t_equip_product_date.name: primary_groups.EQUIP_PRODUCT_DATE,
        MappingFactoryMachine.Columns.t_equip_no.name: primary_groups.EQUIP_NO,
        MappingFactoryMachine.Columns.t_station_no.name: primary_groups.STATION_NO,
        MappingFactoryMachine.Columns.t_prod_family_id.name: primary_groups.PROD_FAMILY_ID,
        MappingFactoryMachine.Columns.t_prod_family_name.name: primary_groups.PROD_FAMILY_NAME,
        MappingFactoryMachine.Columns.t_prod_family_abbr.name: primary_groups.PROD_FAMILY_ABBR,
        MappingFactoryMachine.Columns.t_prod_id.name: primary_groups.PROD_ID,
        MappingFactoryMachine.Columns.t_prod_name.name: primary_groups.PROD_NAME,
        MappingFactoryMachine.Columns.t_prod_abbr.name: primary_groups.PROD_ABBR,
        MappingFactoryMachine.Columns.t_process_id.name: primary_groups.PROCESS_ID,
        MappingFactoryMachine.Columns.t_process_name.name: primary_groups.PROCESS_NAME,
        MappingFactoryMachine.Columns.t_process_abbr.name: primary_groups.PROCESS_ABBR,
        MappingFactoryMachine.Columns.data_table_id.name: MappingFactoryMachine.Columns.data_table_id.name,
    }
    dict_index_cols = {key: val for key, val in dic_mapping_factory.items() if val in df.columns}
    if not dict_index_cols:
        return (None,)

    # ↓=========== MAPPING FACTORY MACHINE ===========↓
    # get factory_machine_id from mapping_factory_machine
    mapping_factory_index_cols = list(dict_index_cols)
    idx_cols = list(dict_index_cols.values())
    if df_mapping_factory_machine is None:
        select_cols = mapping_factory_index_cols + [MappingFactoryMachine.Columns.factory_machine_id.name]
        df_mapping_factory_machine = MappingFactoryMachine.get_all_as_df(db_instance, select_cols=select_cols)
        if df_mapping_factory_machine is None or df_mapping_factory_machine.empty:
            return (None,)

        df_mapping_factory_machine.drop_duplicates(subset=mapping_factory_index_cols, inplace=True)
        df_mapping_factory_machine.rename(columns=dict(zip(mapping_factory_index_cols, idx_cols)), inplace=True)
    convert_type_base_df(df, df_mapping_factory_machine, idx_cols)
    df = pd.merge(df, df_mapping_factory_machine, how='inner', on=idx_cols)
    # ↓=========== MAPPING FACTORY MACHINE ===========↓

    dic_mapping_part = {
        MappingPart.Columns.t_part_no.name: primary_groups.PART_NO,
        MappingPart.Columns.t_part_name.name: primary_groups.PART_NAME,
        MappingPart.Columns.t_part_abbr.name: primary_groups.PART_ABBR,
        MappingPart.Columns.t_part_no_full.name: primary_groups.PART_NO_FULL,
        MappingPart.Columns.data_table_id.name: MappingPart.Columns.data_table_id.name,
    }
    dict_index_cols = {key: val for key, val in dic_mapping_part.items() if val in df.columns}
    if not dict_index_cols:
        return (None,)

    # ↓=========== MAPPING PART ===========↓
    # get prod_part_id from mapping_part
    mapping_part_index_cols = list(dict_index_cols)
    idx_cols = list(dict_index_cols.values())
    if df_mapping_part is None:
        select_cols = mapping_part_index_cols + [MappingPart.Columns.part_id.name]
        df_mapping_part = MappingPart.get_all_as_df(db_instance, select_cols=select_cols)
        if df_mapping_part is None or df_mapping_part.empty:
            return (None,)

        df_mapping_part.drop_duplicates(subset=mapping_part_index_cols, inplace=True)
        df_mapping_part.rename(columns=dict(zip(mapping_part_index_cols, idx_cols)), inplace=True)
    convert_type_base_df(df, df_mapping_part, idx_cols)
    df = pd.merge(df, df_mapping_part, how='inner', on=idx_cols)
    # ↑=========== MAPPING PART ===========↑

    # ↓=========== MAPPING SUB PART ===========↓
    # check exist sub part
    if is_v2_history and DataGroupType.SUB_PART_NO.name in df.columns:
        df_sub_part = df[~df[DataGroupType.SUB_PART_NO.name].isnull()]
        df_mapped_sub_part = pd.merge(
            df_sub_part,
            df_mapping_part,
            how='inner',
            left_on=[DataGroupType.SUB_PART_NO.name],
            right_on=[primary_groups.PART_NO],
        )
        df_unmap_sub_part = df_sub_part[~df_sub_part[unique_col].isin(df_mapped_sub_part[unique_col])]
        if not df_unmap_sub_part.empty:
            df = df[~df[unique_col].isin(df_unmap_sub_part[unique_col])]
    # ↑=========== MAPPING SUB PART ===========↑

    # ↓=========== RELATION PRODUCT PART ===========↓
    # get part_id from r_prod_part
    r_prod_part_index_cols = [RProdPart.Columns.part_id.name]
    if df_r_prod_part is None:
        select_cols = r_prod_part_index_cols + [RProdPart.Columns.id.name]
        df_r_prod_part = RProdPart.get_all_as_df(db_instance, select_cols=select_cols)
        df_r_prod_part.drop_duplicates(subset=r_prod_part_index_cols, inplace=True)
    convert_type_base_df(df, df_r_prod_part, r_prod_part_index_cols)
    df = pd.merge(df, df_r_prod_part, how='inner', on=r_prod_part_index_cols)
    # ↑=========== RELATION PRODUCT PART ===========↑

    # ↓=========== RELATION FACTORY MACHINE ===========↓
    # get process_id from r_factory_machine
    r_factory_machine_index_col = MappingFactoryMachine.Columns.factory_machine_id.name
    if df_r_factory is None:
        select_cols = [RFactoryMachine.Columns.id.name, RFactoryMachine.Columns.process_id.name]
        df_r_factory = RFactoryMachine.get_all_as_df(db_instance, select_cols=select_cols)
        df_r_factory.drop_duplicates(subset=r_factory_machine_index_col, inplace=True)
    convert_type_base_df(df, df_r_factory, [r_factory_machine_index_col])
    df = pd.merge(df, df_r_factory, how='inner', on=r_factory_machine_index_col)
    # ↑=========== RELATION FACTORY MACHINE ===========↑

    df.drop_duplicates(subset=[unique_col], inplace=True)
    df.set_index(unique_col, inplace=True)

    return df, df_mapping_factory_machine, df_mapping_part, df_r_prod_part, df_r_factory


@log_execution_time()
def join_mapping_process_data(db_instance: PostgreSQL, df, primary_groups: PrimaryGroup, df_mapping_data=None):
    dict_index_cols = {
        key: val
        for key, val in MappingProcessData.mapping_process_for_join(primary_groups).items()
        if val in df.columns
    }
    if not dict_index_cols:
        return None, None

    mapping_process_index_cols = list(dict_index_cols)
    idx_cols = list(dict_index_cols.values())
    if df_mapping_data is None:
        select_cols = mapping_process_index_cols + [MappingProcessData.Columns.data_id.name]
        df_mapping_data = MappingProcessData.get_all_as_df(db_instance, select_cols=select_cols)
        if df_mapping_data is None or df_mapping_data.empty:
            return None, None

        df_mapping_data.drop_duplicates(subset=mapping_process_index_cols, inplace=True)
        df_mapping_data.rename(columns=dict(zip(mapping_process_index_cols, idx_cols)), inplace=True)

    convert_type_base_df(df, df_mapping_data, idx_cols)
    df = pd.merge(df, df_mapping_data, how='inner', on=idx_cols)

    return df, df_mapping_data


@log_execution_time()
def join_master_data_type(
    df: DataFrame,
    db_instance: PostgreSQL,
    primary_groups: PrimaryGroup,
    df_mapping_data=None,
    df_m_data=None,
):
    unique_col = '__CHECK_UNIQUE__'
    df[unique_col] = df.index

    df, df_mapping_data = join_mapping_process_data(db_instance, df, primary_groups, df_mapping_data)

    if df_m_data is None:
        select_cols = [MData.Columns.id.name, MData.Columns.data_type.name]
        df_m_data = MData.get_all_as_df(db_instance, select_cols=select_cols)
        df_m_data.fillna(EMPTY_STRING, inplace=True)
        df_m_data.drop_duplicates(subset=MappingProcessData.Columns.data_id.name, inplace=True)
        df_m_data.set_index(MappingProcessData.Columns.data_id.name, inplace=True)
    replace_dataframe_symbol(df, MappingProcessData.Columns.data_id.name)
    df = pd.merge(
        df,
        df_m_data,
        how='inner',
        left_on=MappingProcessData.Columns.data_id.name,
        right_index=True,
    )

    df.drop_duplicates(subset=[unique_col], inplace=True)
    df.drop(unique_col, axis=1, inplace=True)

    return df, df_mapping_data, df_m_data


@log_execution_time()
def join_master_data(
    df: DataFrame,
    db_instance: PostgreSQL,
    primary_groups: PrimaryGroup,
    df_mapping_data=None,
    df_mapping_factory_machine=None,
    df_mapping_part=None,
    df_r_prod_part=None,
    df_m_data=None,
):
    unique_col = '__CHECK_UNIQUE__'
    # df.reset_index(drop=True, inplace=True)
    df[unique_col] = df.index

    dict_index_cols = {
        key: val
        for key, val in MappingProcessData.mapping_process_for_join(primary_groups).items()
        if val in df.columns
    }
    if not dict_index_cols:
        return (None,)

    mapping_process_index_cols = list(dict_index_cols)
    idx_cols = list(dict_index_cols.values())
    if df_mapping_data is None:
        select_cols = mapping_process_index_cols + [MappingProcessData.Columns.data_id.name]
        df_mapping_data = MappingProcessData.get_all_as_df(db_instance, select_cols=select_cols)
    # TODO: fill na
    # df_mapping_data.replace({pd.NA: EMPTY_STRING}, inplace=True)
    if df_mapping_data is None or df_mapping_data.empty:
        return (None,)

    df_mapping_data.fillna(EMPTY_STRING, inplace=True)
    df_mapping_data.drop_duplicates(subset=mapping_process_index_cols, inplace=True)
    df_mapping_data.set_index(mapping_process_index_cols, inplace=True)
    # df.set_index(idx_cols, inplace=True)
    # df.index.set_names(mapping_process_index_cols, inplace=True)
    # df = df.join(df_mapping_data, how='inner')
    replace_dataframe_symbol(df, idx_cols)
    df = pd.merge(df, df_mapping_data, how='inner', left_on=idx_cols, right_index=True)
    # df = df.compute(scheduler='threads')

    dic_mapping_factory = {
        MappingFactoryMachine.Columns.t_line_id.name: primary_groups.LINE_ID,
        MappingFactoryMachine.Columns.t_line_name.name: primary_groups.LINE_NAME,
        MappingFactoryMachine.Columns.t_equip_id.name: primary_groups.EQUIP_ID,
        MappingFactoryMachine.Columns.t_equip_name.name: primary_groups.EQUIP_NAME,
        MappingFactoryMachine.Columns.t_dept_id.name: primary_groups.DEPT_ID,
        MappingFactoryMachine.Columns.t_dept_name.name: primary_groups.DEPT_NAME,
        MappingFactoryMachine.Columns.t_process_id.name: primary_groups.PROCESS_ID,
        MappingFactoryMachine.Columns.t_process_name.name: primary_groups.PROCESS_NAME,
        MappingFactoryMachine.Columns.t_factory_id.name: primary_groups.FACTORY_ID,
        MappingFactoryMachine.Columns.t_factory_name.name: primary_groups.FACTORY_NAME,
        MappingFactoryMachine.Columns.t_plant_id.name: primary_groups.PLANT_ID,
        MappingFactoryMachine.Columns.data_table_id.name: MappingFactoryMachine.Columns.data_table_id.name,
    }
    dict_index_cols = {key: val for key, val in dic_mapping_factory.items() if val in df.columns}
    if not dict_index_cols:
        return (None,)

    mapping_factory_index_cols = list(dict_index_cols)
    idx_cols = list(dict_index_cols.values())
    if df_mapping_factory_machine is None:
        select_cols = mapping_factory_index_cols + [MappingFactoryMachine.Columns.factory_machine_id.name]
        df_mapping_factory_machine = MappingFactoryMachine.get_all_as_df(db_instance, select_cols=select_cols)
        # df_mapping_factory_machine.replace({pd.NA: EMPTY_STRING}, inplace=True)
        if df_mapping_factory_machine is None or df_mapping_factory_machine.empty:
            return (None,)

        # df.set_index(idx_cols, inplace=True)
        # df.index.set_names(mapping_factory_index_cols, inplace=True)
        # df = df.join(df_mapping_factory_machine, how='inner')
        df_mapping_factory_machine.fillna(EMPTY_STRING, inplace=True)
        df_mapping_factory_machine.drop_duplicates(subset=mapping_factory_index_cols, inplace=True)
        df_mapping_factory_machine.set_index(mapping_factory_index_cols, inplace=True)
    replace_dataframe_symbol(df, idx_cols)
    df = pd.merge(df, df_mapping_factory_machine, how='inner', left_on=idx_cols, right_index=True)
    # df = df.compute(scheduler='threads')

    dic_mapping_part = {
        MappingPart.Columns.t_part_no.name: primary_groups.PART_NO,
        MappingPart.Columns.data_table_id.name: MappingPart.Columns.data_table_id.name,
    }
    # mapping_part_index_cols = [MappingPart.Columns.t_part_no.name]
    dict_index_cols = {key: val for key, val in dic_mapping_part.items() if val in df.columns}
    if not dict_index_cols:
        return (None,)

    mapping_part_index_cols = list(dict_index_cols)
    idx_cols = list(dict_index_cols.values())
    if df_mapping_part is None:
        select_cols = mapping_part_index_cols + [MappingPart.Columns.part_id.name]
        df_mapping_part = MappingPart.get_all_as_df(db_instance, select_cols=select_cols)
        # df_mapping_part.replace({pd.NA: EMPTY_STRING}, inplace=True)
        if df_mapping_part is None or df_mapping_part.empty:
            return (None,)

        df_mapping_part.fillna(EMPTY_STRING, inplace=True)
        df_mapping_part.drop_duplicates(subset=mapping_part_index_cols, inplace=True)
        df_mapping_part.set_index(mapping_part_index_cols, inplace=True)
    # df.set_index(idx_cols, inplace=True)
    # df.index.set_names(mapping_part_index_cols, inplace=True)
    # df = df.join(df_mapping_part, how='inner')
    replace_dataframe_symbol(df, idx_cols)
    df = pd.merge(df, df_mapping_part, how='inner', left_on=idx_cols, right_index=True)
    # df = df.compute(scheduler='threads')

    r_prod_part_index_cols = [RProdPart.Columns.part_id.name]
    if df_r_prod_part is None:
        select_cols = r_prod_part_index_cols + [RProdPart.Columns.id.name]
        df_r_prod_part = RProdPart.get_all_as_df(db_instance, select_cols=select_cols)
        # df_r_prod_part.replace({pd.NA: EMPTY_STRING}, inplace=True)
        df_r_prod_part.fillna(EMPTY_STRING, inplace=True)
        df_r_prod_part.drop_duplicates(subset=r_prod_part_index_cols, inplace=True)
        df_r_prod_part.set_index(r_prod_part_index_cols, inplace=True)

    # df.set_index(r_prod_part_index_cols, inplace=True)
    # df = df.join(df_r_prod_part, how='inner')
    replace_dataframe_symbol(df, r_prod_part_index_cols)
    df = pd.merge(df, df_r_prod_part, how='inner', left_on=r_prod_part_index_cols, right_index=True)
    # df = df.compute(scheduler='threads')

    if df_m_data is None:
        select_cols = [MData.Columns.id.name] + [
            MData.Columns.process_id.name,
            MData.Columns.data_type.name,
        ]
        df_m_data = MData.get_all_as_df(db_instance, select_cols=select_cols)
        # df_m_data.replace({pd.NA: EMPTY_STRING}, inplace=True)
        df_m_data.fillna(EMPTY_STRING, inplace=True)
        df_m_data.drop_duplicates(subset=MappingProcessData.Columns.data_id.name, inplace=True)
        df_m_data.set_index(MappingProcessData.Columns.data_id.name, inplace=True)
    # df.set_index(MappingProcessData.Columns.data_id.name, drop=False, inplace=True)
    # df = df.join(df_m_data, how='inner')
    replace_dataframe_symbol(df, MappingProcessData.Columns.data_id.name)
    df = pd.merge(
        df,
        df_m_data,
        how='inner',
        left_on=MappingProcessData.Columns.data_id.name,
        right_index=True,
    )
    # df = df.compute(scheduler='threads')

    df.drop_duplicates(subset=[unique_col], inplace=True)
    df.drop(unique_col, axis=1, inplace=True)

    return (
        df,
        df_mapping_data,
        df_mapping_factory_machine,
        df_mapping_part,
        df_r_prod_part,
        df_m_data,
    )


def gen_import_trans_history(
    cfg_data_table,
    process_id,
    job_info,
    imported_cycle,
    imported_count,
    cycle_start_tm,
    cycle_end_tm,
    target_files=None,
):
    df_error_cnt = 0  # todo pre process

    job_info.data_table_id = cfg_data_table.id
    job_info.target = target_files
    job_info.data_table_id = job_info.data_table_id
    job_info.process_id = process_id

    job_info = gen_import_job_info(
        job_info,
        imported_count,
        err_cnt=df_error_cnt,
        cycle_start_tm=cycle_start_tm,
        cycle_end_tm=cycle_end_tm,
        imported_cycle=imported_cycle,
    )

    return job_info


@log_execution_time()
def generate_config_process(
    db_instance: PostgreSQL,
    cfg_data_table: Union[CfgDataTable, BSCfgDataTable],
    existed_processes_ids: list[int],
):
    """
    Generate cfg_process & cfg_process_column data.

    **Attention**: The generated data is base on m_data table
    :param db_instance: instance of PostgreSQL
    :param cfg_data_table: instance of CfgDataTable
    :param existed_processes_ids: list of existed process ids
    :return: void
    """

    if existed_processes_ids:
        df_m_process = MProcess.get_all_as_df(db_instance)
        df_m_process = df_m_process[df_m_process[MData.Columns.process_id.name].isin(existed_processes_ids)]
        if not cfg_data_table.is_has_auto_increment_col():
            from bridge.services.master_data_import import gen_m_data_manual

            df_m_data = MData.get_all_as_df(db_instance)
            unit_id = MUnit.get_empty_unit_id(db_instance)
            for process_id in existed_processes_ids:
                df_m_data = df_m_data[df_m_data[MData.Columns.process_id.name] == process_id]
                if not (df_m_data[MData.Columns.data_type.name] == RawDataTypeDB.DATETIME.value).any():
                    gen_m_data_manual(
                        RawDataTypeDB.DATETIME.value,
                        unit_id,
                        DATETIME_DUMMY,
                        DATETIME_DUMMY,
                        DATETIME_DUMMY,
                        process_id,
                        data_group_type=DataGroupType.GENERATED.value,
                        db_instance=db_instance,
                    )

        gen_cfg_process(db_instance, df_m_process)
        gen_cfg_process_column(db_instance, cfg_data_table, df_m_process, existed_processes_ids)


@log_execution_time()
def insert_by_df_ignore_duplicate(
    db_instance: PostgreSQL,
    df: DataFrame,
    model_cls: Type[BridgeStationModel],
):
    """
    df: DataFrame with columns of model_cls, include id columns. (id, or PK columns for duplicate check)

    :param db_instance: an instance of database
    :param df: data frame
    :param model_cls: class of BridgeStationModel
    :return: length of inserted records
    """
    # Insert by df and ignore if duplicate by primary key
    #  Same with write_master_data, but this method is more simple
    table_name = model_cls.get_table_name()

    foreign_id_col = model_cls.get_foreign_id_column_name()
    df_unique = get_df_unique(df, model_cls, [foreign_id_col])

    # get all
    df_existing_unique = model_cls.get_all_as_df(db_instance)
    existing_flag_col = '__EXISTING_FLA9__'
    df_existing_unique[existing_flag_col] = True

    #
    if df_existing_unique.empty:
        df_insert = df_unique
    else:
        df_unique = df_unique.merge(df_existing_unique, how='left', on=[foreign_id_col], suffixes=Suffixes.KEEP_LEFT)
        df_insert = df_unique[df_unique[existing_flag_col].isna()]

    if not df_insert.empty:
        df_insert['id'] = df_insert[foreign_id_col]
        cols = [col for col in model_cls.Columns.get_column_names() if col in df_insert.columns]
        rows = convert_nan_to_none(df_insert[cols], convert_to_list=True)

        db_instance.bulk_insert(table_name, cols, rows)
        publish_master_config_changed(table_name, crud_type=CRUDType.INSERT.name)
    return len(df_insert)


def get_df_unique(df, model_cls, config_bs_unique_key):
    # if column is in unique rule but not found in df. set to default none.
    for col in config_bs_unique_key:
        if col not in df.columns:
            # tạm thời chỉ có trường hợp string thôi nên không set type cũng được nhưng nếu lúc str lúc int thì phức tạp
            df[col] = None  # too type ?

    # drop duplicate by unique rule
    df_unique = df.drop_duplicates(subset=config_bs_unique_key)[config_bs_unique_key]

    # copy model_cls' columns
    for col in model_cls.Columns.get_column_names():
        if col in df.columns and col not in df_unique.columns:
            df_unique[col] = df[col]

    return df_unique


def insert_proc_link_count(db_instance, job_id, proc_id, inserted_count):
    proc_link_count_rec = {
        ProcLinkCount.Columns.job_id.name: job_id,
        ProcLinkCount.Columns.process_id.name: proc_id,
        ProcLinkCount.Columns.matched_count.name: inserted_count,
    }
    ProcLinkCount.insert_record(db_instance, proc_link_count_rec)

    # Unused -> Todo Remove


def gen_import_data_file(
    db_instance: PostgreSQL,
    cfg_data_table,
    process_id: int,
    is_past: bool,
    df_process: DataFrame,
    binary_file_suffix: str,
):
    if df_process is None or len(df_process) == 0:
        return False

    job_id = convert_time(format_str=DATE_FORMAT_STR_ONLY_DIGIT)
    folder_path = get_transaction_file_folder(cfg_data_table.id, process_id, is_past)
    extension = FileExtension.Feather.value
    file_name = job_id if binary_file_suffix is None else f'{job_id}_{binary_file_suffix}'

    output_file = gen_import_file_name(folder_path, file_name, extension)
    if DataGroupType.DATA_SERIAL.name in df_process:
        serial_col_id = None
        dict_m_data = MData.get_by_process_id(db_instance, process_id, is_cascade=True, is_return_dict=True)
        for data_id, m_data in dict_m_data.items():
            if m_data.m_data_group.data_group_type == DataGroupType.DATA_SERIAL.value:
                serial_col_id = data_id
                break

        if serial_col_id is not None:
            rename_dict = {DataGroupType.DATA_SERIAL.name: str(serial_col_id)}
            df_process = df_process.rename(columns=rename_dict)
    write_feather_file(df_process, output_file)

    return True


def gen_transaction_data_unknown_master_file(data_table_id: int, df: DataFrame, binary_file_suffix: str):
    """
    Export a feather file that contains all the data from the dataframe
    Args:
        data_table_id (int): the id of the data table
        df (DataFrame): the dataframe
        binary_file_suffix (str): the prefix of the file
    """
    folder_path = get_transaction_data_unknown_master_file_folder(data_table_id)
    extension = FileExtension.Feather.value
    file_name = binary_file_suffix
    output_file = gen_import_file_name(folder_path, file_name, extension)
    write_feather_file(df, output_file)


def gen_import_file_name(folder_path, file_name, extension):
    increase_str = ''
    under_bar = ''
    while True:
        output_file = os.path.join(folder_path, f'{file_name}{under_bar}{increase_str}.{extension}')
        if check_exist(output_file):
            under_bar = '_'
            increase_str = str(int(increase_str) + 1) if increase_str else str(1)
        else:
            break
    return output_file


def get_horizon_cols(cfg_data_table):
    cfg_data_table_columns = cfg_data_table.get_sorted_columns()
    source_column_names, bridge_column_names, _ = get_pair_source_col_bridge_col(cfg_data_table_columns)

    horizon_cols = []
    for source_col, bridge_col in zip(source_column_names, bridge_column_names):
        if bridge_col == DataGroupType.HORIZONTAL_DATA.name:
            horizon_cols.append(source_col)


@log_execution_time()
def transform_horizon_columns_for_import(
    cfg_data_table: CfgDataTable,
    df_original: DataFrame,
    only_horizon_col=None,
    ignore_cols=None,
):
    cfg_data_table_columns = cfg_data_table.get_sorted_columns()
    source_column_names, bridge_column_names, _ = get_pair_source_col_bridge_col(cfg_data_table_columns)

    dic_name_replace = {}
    horizon_cols = []
    for source_col, bridge_col in zip(source_column_names, bridge_column_names):
        if bridge_col in [
            DataGroupType.HORIZONTAL_DATA.name,
            DataGroupType.AUTO_INCREMENTAL.name,
        ] and not MasterDBType.is_v2_group(cfg_data_table.get_master_type()):
            horizon_cols.append(source_col)
        else:
            dic_name_replace[source_col] = bridge_col

    if ignore_cols:
        ignore_cols = set([dic_name_replace.get(col) for col in ignore_cols if dic_name_replace.get(col)] + ignore_cols)
    else:
        ignore_cols = set()

    dic_df_horizons = {}
    dic_nan_replace = dict.fromkeys(PANDAS_DEFAULT_NA | NA_VALUES, DEFAULT_NONE_VALUE)
    for horizon_col in horizon_cols:
        if only_horizon_col and only_horizon_col != horizon_col:
            continue

        # in case of dataframe not exist horizon_col in this data chunk, do nothing
        if horizon_col in ignore_cols or horizon_col not in df_original:
            continue

        if is_bool_dtype(df_original[horizon_col]):
            col_value_series = df_original[horizon_col]
        else:
            col_value_series = df_original[horizon_col].replace(dic_nan_replace)

        dic_df_horizons[horizon_col] = col_value_series

    df = pd.DataFrame()
    not_horizon_cols = list(set(df_original.columns) - set(horizon_cols) - ignore_cols)
    if not_horizon_cols:
        df[not_horizon_cols] = df_original[not_horizon_cols]

    # cate_cols = [col for col in not_horizon_cols if col != 'datetime']  # TODO: variable 'datetime'
    # df[cate_cols] = df[cate_cols].astype('category')

    if dic_name_replace:
        df.rename(columns=dic_name_replace, inplace=True)

    # sort by time to split smaller dataset
    # df[INDEX_COL] = df.index
    if DataGroupType.AUTO_INCREMENTAL.name in df.columns:
        df.sort_values([DataGroupType.AUTO_INCREMENTAL.name], inplace=True)
    elif DataGroupType.DATA_TIME.name in df.columns:
        df.sort_values([DataGroupType.DATA_TIME.name], inplace=True)

    return df, dic_df_horizons


@BridgeStationModel.use_db_instance()
def get_pair_source_col_bridge_col(cfg_data_table_columns: List[CfgDataTableColumn], db_instance: PostgreSQL = None):
    """
    bridge_column_names: m_data_group's data_name_sys
    source_column_names: cfg_data_table's column_name
    :param cfg_data_table_columns:
    :param db_instance:
    :return:
    """
    source_column_names = [col.column_name for col in cfg_data_table_columns if col.data_group_type]
    m_data_groups = BSMDataGroup.get_data_group_in_group_types(
        db_instance,
        [col.data_group_type for col in cfg_data_table_columns],
    )
    m_data_groups = {group.data_group_type: group.get_sys_name() for group in m_data_groups}
    bridge_column_names = [
        m_data_groups.get(col.data_group_type) for col in cfg_data_table_columns if col.data_group_type
    ]
    data_group_types = [col.data_group_type for col in cfg_data_table_columns if col.data_group_type]

    return source_column_names, bridge_column_names, data_group_types


@log_execution_time()
def validate_data_type(df: DataFrame, dic_data_types: dict, na_values: set[str] = None):
    df = df.reset_index(drop=True)
    data_ids = []
    df_error = pd.DataFrame()
    for _data_id, data_type in dic_data_types.items():
        data_id = str(_data_id)
        if data_id not in df.columns:
            continue

        data_ids.append(data_id)
        s = df[data_id]

        ori_data_type = s.dtype.name
        if na_values:
            s_notnull = s[s.notnull()]
            if ori_data_type not in [
                pd.Int64Dtype.name,
                pd.Float64Dtype.name,
                pd.BooleanDtype.name,
            ]:
                s_notnull = s_notnull.astype(str).replace(dict.fromkeys(na_values, DEFAULT_NONE_VALUE))
                s_notnull = s_notnull.astype(ori_data_type)
                s[s.notnull()] = s_notnull

        # change boolean to string column name
        # handle case when importing boolean column as float
        if pd.api.types.is_bool_dtype(s):
            s = s.astype(pd.StringDtype()).str.lower()
        if data_type == RawDataTypeDB.REAL.value:
            s = pd.to_numeric(s, errors='coerce')
        elif data_type in (
            RawDataTypeDB.INTEGER.value,
            RawDataTypeDB.SMALL_INT.value,
        ):
            s = s[s.notnull()]
            s = pd.to_numeric(s, errors='coerce')
            s = s[s.notnull()]
            if data_type == RawDataTypeDB.SMALL_INT.value:
                s = s[((np.mod(s, 1) == 0) & is_int_16(s))]
                s = s.astype(pd.Int16Dtype())
            elif data_type == RawDataTypeDB.INTEGER.value:
                s = s[((np.mod(s, 1) == 0) & is_int_32(s))]
                s = s.astype(pd.Int32Dtype())
        elif data_type == RawDataTypeDB.BIG_INT.value:
            s[~s.astype(str).str.isnumeric()] = pd.NA  # replace value not number to pd.NA
            s = s[s.notnull()]
            s = pd.to_numeric(s, errors='coerce')
            s = s[s.notnull()]
            s = s[(np.mod(s, 1) == 0) & (is_int_64(s))]
            s = s.astype(pd.Int64Dtype())
        elif data_type == RawDataTypeDB.BOOLEAN.value:
            if ori_data_type == pd.BooleanDtype():
                continue

            s = s[s.notnull()]
            s = s.astype(pd.StringDtype())
            s = s.replace(
                {
                    BooleanStringDefinition.true.name: BooleanStringDefinition.true.value,
                    BooleanStringDefinition.false.name: BooleanStringDefinition.false.value,
                },
            )
            s = pd.to_numeric(s, errors='coerce')
            s = s[s.notnull()]
            s = s[((np.mod(s, 1) == 0) & is_boolean(s))]
        elif data_type == RawDataTypeDB.DATETIME.value:
            s = pd.to_datetime(s, errors='coerce')
            s = s[s.notnull()]
        else:
            continue

        df[data_id] = s.astype(df[data_id].dtypes.name)

    if data_ids:
        df_error = df[df[data_ids].isnull().all(axis=1)]
        df = df.dropna(subset=data_ids, how='all')

    df.reset_index(drop=True, inplace=True)
    return df, df_error


def generate_datetime_dummy(df_process, process_id):
    join_by = [CSV_INDEX_COL, CSV_HORIZONTAL_ROW_INDEX_COL]
    latest_record = None
    # max_time = MasterDataModel.get_max_date_time_by_process_id(process_id)
    with BridgeStationModel.get_db_proxy() as db_instance:
        transaction_data_obj = TransactionData(process_id)
        if transaction_data_obj.table_name not in db_instance.list_tables():
            transaction_data_obj.create_table(db_instance)
        max_time = transaction_data_obj.get_max_date_time_by_process_id(db_instance)
        if max_time:
            latest_record = add_days_from_utc(max_time, 1, True)
    dummy_datetime_from = latest_record
    df_process.drop(columns=DataGroupType.DATA_TIME.name, inplace=True)
    df_groups = df_process.groupby(by=[CSV_INDEX_COL, MData.Columns.process_id.name])
    datatime_cols = pd.Series([])
    for _, _sub_df in df_groups:
        # remove data_time
        if DataGroupType.DATA_TIME.name in df_process.columns:
            df_process.drop(columns=DataGroupType.DATA_TIME.name, inplace=True)
        row_indexes = _sub_df[CSV_HORIZONTAL_ROW_INDEX_COL].unique().tolist()
        group_id = _sub_df[CSV_INDEX_COL].unique().tolist() * len(row_indexes)
        sub_df = pd.DataFrame({CSV_INDEX_COL: group_id, CSV_HORIZONTAL_ROW_INDEX_COL: row_indexes})
        sub_df = gen_dummy_datetime(sub_df, dummy_datetime_from)
        sub_df.rename(columns={DATETIME_DUMMY: DataGroupType.DATA_TIME.name}, inplace=True)
        # merge has data_time
        df_process = pd.merge(df_process, sub_df, how='left', left_on=join_by, right_on=join_by)
        if not datatime_cols.size:
            datatime_cols = df_process[DataGroupType.DATA_TIME.name]
        datatime_cols = datatime_cols.combine_first(df_process[DataGroupType.DATA_TIME.name])
        dummy_datetime_from = get_next_datetime_value(sub_df.shape[0], dummy_datetime_from)

    df_process[DataGroupType.DATA_TIME.name] = datatime_cols
    return df_process


def generate_datetime_dummy_new(db_instance, df_process, transaction_data_obj):
    latest_record = None
    max_time = transaction_data_obj.get_max_date_time_by_process_id(db_instance=db_instance)
    if max_time:
        latest_record = add_days_from_utc(max_time, 1, True)
    dummy_datetime_from = latest_record
    gen_dummy_datetime(df_process, dummy_datetime_from)
    df_process.rename(columns={DATETIME_DUMMY: str(transaction_data_obj.getdate_column.id)}, inplace=True)

    return df_process


@log_execution_time()
def datetime_transform(datetime_series):
    # MM-DD | MM月DD日 | MM/DD -> current year -MM-DD 00:00:00
    regex1 = r'^(?P<m>\d{1,2})(-|\/|月)(?P<d>\d{1,2})日?$'
    # YYYY/MM/DD | YYYY-MM-DD | YYYY年MM月DD日 | YY-MM-DD | YY/MM/DD | YY年MM月DD日-> YYYY-MM-DD 00:00:00
    regex2 = r'^(?P<y>\d{4}|\d{1,2})(-|\/|年)(?P<m>\d{1,2})(-|\/|月)(?P<d>\d{1,2})日?$'
    # YYYY年MM月DD日hh時mm分ss秒 -> YYYY-MM-DD hh:mm:ss
    regex3 = r'^(?P<y>\d{4})年(?P<m>\d{1,2})月(?P<d>\d{1,2})日(?P<h>\d{1,2})時(?P<min>\d{1,2})分(?P<s>\d{1,2})秒$'

    current_year = datetime.now().strftime('%Y')

    def without_year_datetime(m: re.match) -> str:
        return f"{current_year}-{m.group('m')}-{m.group('d')} 00:00:00"

    def full_datetime(m: re.match) -> str:
        if len(m.group('y')) == 4:
            return f"{m.group('y')}-{m.group('m')}-{m.group('d')} 00:00:00"
        # if there is 2 digit of year, convert to full year
        return f"{current_year[0:2]}{m.group('y')}-{m.group('m')}-{m.group('d')} 00:00:00"

    def actual_datetime(m: re.match) -> str:
        return f"{m.group('y')}-{m.group('m')}-{m.group('d')} {m.group('h')}:{m.group('min')}:{m.group('s')}"

    # convert special datetime string to iso-format
    datetime_series = datetime_series.str.replace(regex1, without_year_datetime, regex=True)
    datetime_series = datetime_series.str.replace(regex2, full_datetime, regex=True)
    datetime_series = datetime_series.str.replace(regex3, actual_datetime, regex=True)

    return datetime_series


@log_execution_time()
def date_transform(date_series):
    """
    Convert date series to standard date format

    Support input formats:

    - YYYY/MM/DD
    - YYYY-MM-DD
    - YYYY年MM月DD日

    Args:
        date_series (Series): a series of time

    Returns:
        A series of date with standard format YYYY-MM-DD
    """
    separate_char = '-'
    begin_part_of_year = datetime.now().year.__str__()[:2]
    formatted_date_series = date_series.str.replace(
        DATE_TYPE_REGEX,
        lambda m: (
            f'{m.group("year") if len(m.group("year")) == 4 else begin_part_of_year + m.group("year")}'
            f'{separate_char}'
            f'{m.group("month").rjust(2, "0")}'
            f'{separate_char}'
            f'{m.group("day").rjust(2, "0")}'
        ),
        regex=True,
    )
    # check date match format 2022-20-20 -> NaT
    formatted_date_series = pd.to_datetime(formatted_date_series, errors='coerce')
    formatted_date_series = formatted_date_series.dt.strftime(PostgresFormatStrings.DATE.value).astype('string')
    return formatted_date_series


@log_execution_time()
def time_transform(time_series):
    """
    Convert time series to standard time format

    Support input formats:

    - HH:mm:ss
    - HH-mm-ss
    - HH.mm.ss
    - HH mm ss
    - HH時mm分ss秒

    Args:
        time_series (Series): a series of time

    Returns:
        A series of time with standard format HH:MM:SS
    """
    separate_char = ':'
    time_series = time_series.astype(pd.StringDtype())
    formatted_time_series = time_series.str.replace(
        TIME_TYPE_REGEX,
        lambda m: (
            f'{m.group("hour").rjust(2, "0")}'
            f'{separate_char}'
            f'{m.group("minute").rjust(2, "0")}'
            f'{separate_char}'
            f'{m.group("second").rjust(2, "0")}'
            if m.group('hour') and m.group('minute') and m.group('second')
            else m.group(0)  # keep origin value if not match regex
        ),
        regex=True,
    )
    formatted_time_series = pd.to_datetime(formatted_time_series, errors='coerce')
    formatted_time_series = formatted_time_series.dt.strftime(PostgresFormatStrings.TIME.value).astype(pd.StringDtype())
    return formatted_time_series


@log_execution_time()
def convert_db_timezone(df, col, dic_tz_info=None):
    from bridge.services.etl_services.etl_import import remove_timezone_inside

    if col in df.columns:
        validate_datetime(df, col, is_strip=False, add_is_error_col=False)
        is_tz_inside, db_time_zone, time_offset = dic_tz_info[col]
        df[col] = convert_df_col_to_utc(df, col, is_tz_inside, db_time_zone, time_offset)
        df[col] = remove_timezone_inside(df[col], is_tz_inside)


def get_time_zone_info(cfg_data_table: CfgDataTable):
    from bridge.services.etl_services.etl_db_service import handle_time_zone

    cols = [cfg_data_table.get_date_col()]
    return {col: handle_time_zone(cfg_data_table, col) for col in cols}


@log_execution_time()
def convert_csv_timezone(df, col):
    from bridge.services.etl_services.etl_import import remove_timezone_inside

    # convert datetime columns into correct format
    convert_datetime_format(df, {col: RawDataTypeDB.DATETIME.value})
    if col in df.columns:
        datetime_val = get_datetime_val(df[col])
        is_timezone_inside, csv_timezone, utc_offset = get_time_info(datetime_val, None)
        df[col] = convert_df_col_to_utc(df, col, is_timezone_inside, csv_timezone, utc_offset)
        df[col] = remove_timezone_inside(df[col], is_timezone_inside)


@log_execution_time()
def convert_datetime_format(df, dic_data_types, datetime_format: Optional[str] = None):
    datetime_format_obj = DateTimeFormatUtils.get_datetime_format(datetime_format)
    for _col, raw_data_type in dic_data_types.items():
        col = str(_col)
        if col not in df:
            continue
        if raw_data_type == RawDataTypeDB.DATETIME.value:
            # Convert datetime base on datetime format
            if datetime_format_obj.datetime_format:
                datetime_series = pd.to_datetime(
                    df[col],
                    errors='coerce',
                    format=datetime_format_obj.datetime_format,
                )
                non_na_datetime_series = datetime_series[datetime_series.notnull()]
                df[col] = non_na_datetime_series.dt.strftime(PostgresFormatStrings.DATETIME.value).astype(
                    pd.StringDtype(),
                )
                continue

            if pd.api.types.is_datetime64_dtype(df[col]):
                df[col] = df[col].dt.strftime(PostgresFormatStrings.DATETIME.value).astype(pd.StringDtype())
                continue

            if pd.api.types.is_object_dtype(df[col]):
                df[col] = df[col].astype(pd.StringDtype())
            elif not pd.api.types.is_string_dtype(df[col]):
                continue

            df[col] = datetime_transform(df[col])
        elif raw_data_type == RawDataTypeDB.DATE.value:
            # Convert date base on date format
            if datetime_format_obj.date_format:
                date_series = pd.to_datetime(
                    df[col],
                    errors='coerce',
                    format=datetime_format_obj.date_format,
                )
                non_na_date_series = date_series[date_series.notnull()]
                df[col] = non_na_date_series.dt.strftime(PostgresFormatStrings.DATE.value).astype(pd.StringDtype())
                continue

            if pd.api.types.is_datetime64_dtype(df[col]):
                df[col] = df[col].dt.strftime(PostgresFormatStrings.DATE.value).astype(pd.StringDtype())
                continue

            date_series = pd.to_datetime(df[col], errors='coerce')
            date_series.update(date_series[date_series.notnull()].dt.strftime(PostgresFormatStrings.DATE.value))
            unknown_series = df[date_series.isnull()][col].astype(pd.StringDtype())
            date_series.update(unknown_series)
            date_series = date_series.astype(pd.StringDtype())
            df[col] = date_transform(date_series).replace({pd.NaT: DEFAULT_NONE_VALUE})
        elif raw_data_type == RawDataTypeDB.TIME.value:
            # Convert time base on time format
            if datetime_format_obj.time_format:
                time_series = pd.to_datetime(
                    df[col],
                    errors='coerce',
                    format=datetime_format_obj.time_format,
                )
                non_na_time_series = time_series[time_series.notnull()]
                df[col] = non_na_time_series.dt.strftime(PostgresFormatStrings.TIME.value).astype(pd.StringDtype())
                continue

            if pd.api.types.is_datetime64_dtype(df[col]):
                df[col] = df[col].dt.strftime(PostgresFormatStrings.TIME.value).astype(pd.StringDtype())
                continue

            time_series = pd.to_datetime(df[col], errors='coerce')
            time_series.update(time_series[time_series.notnull()].dt.strftime(PostgresFormatStrings.TIME.value))
            unknown_series = df[time_series.isnull()][col].astype(pd.StringDtype())
            time_series.update(unknown_series)
            time_series = time_transform(time_series).replace({pd.NaT: DEFAULT_NONE_VALUE})
            df[col] = time_series

    return df
