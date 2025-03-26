import os.path
import traceback
from datetime import datetime
from threading import Lock
from typing import List

import numpy as np
import pandas as pd
from apscheduler.triggers.date import DateTrigger
from apscheduler.triggers.interval import IntervalTrigger
from dateutil import tz
from pandas import DataFrame
from pytz import utc

from ap import scheduler
from ap.common.common_utils import (
    DATE_FORMAT_STR,
    DATE_FORMAT_STR_ONLY_DIGIT,
    TXT_FILE_TYPE,
    convert_time,
    get_basename,
    get_csv_delimiter,
    get_current_timestamp,
    get_error_cast_path,
    get_error_duplicate_path,
    get_error_import_path,
    get_error_trace_path,
    make_dir_from_file_path,
    split_path_to_list,
)
from ap.common.constants import (
    CAST_DATA_TYPE_ERROR_MSG,
    DATA_TYPE_DUPLICATE_MSG,
    DATA_TYPE_ERROR_MSG,
    DATETIME,
    DEFAULT_NONE_VALUE,
    EFA_HEADER_FLAG,
    WR_HEADER_NAMES,
    WR_VALUES,
    CfgConstantType,
    CsvDelimiter,
    DataType,
    EFAColumn,
    JobStatus,
    JobType,
)
from ap.common.disk_usage import get_ip_address
from ap.common.logger import log_execution_time, logger
from ap.common.pydn.dblib.db_common import PARAM_SYMBOL
from ap.common.pydn.dblib.db_proxy_readonly import ReadOnlyDbProxy
from ap.common.scheduler import add_job_to_scheduler
from ap.common.services import csv_header_wrapr as chw
from ap.common.services.csv_content import read_data
from ap.common.services.jp_to_romaji_utils import to_romaji
from ap.common.services.normalization import normalize_df, normalize_str
from ap.common.timezone_utils import calc_offset_between_two_tz, gen_sql, get_db_timezone
from ap.setting_module.models import (
    CfgConstant,
    CfgDataTable,
    CfgProcess,
    CfgProcessColumn,
    MProcess,
    make_session,
)
from bridge.services.clear_transaction_data import run_clean_trans_tables_job
from bridge.services.data_import import import_transaction_per_process_job

lock = Lock()
# csv_import : max id of cycles
# ( because of csv import performance, we make a deposit/a guess of cycle id number
# to avoid conflict of other csv import thread/job  )
csv_import_cycle_max_id = None

# index column in df
INDEX_COL = '__INDEX__'
CYCLE_TIME_COL = '__time__'

# file index col in df
FILE_IDX_COL = '__FILE_INDEX__'

# max insert record per job
RECORD_PER_COMMIT = 10_000

# range of time per sql

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
PREVIEW_ALLOWED_EXCEPTIONS = {'-', '--', '---'}
INF_VALUES = {'Inf', 'Infinity', '1/0', '#DIV/0!', float('inf')}
INF_NEG_VALUES = {'-Inf', '-Infinity', '-1/0', float('-inf')}

ALL_SYMBOLS = set(PANDAS_DEFAULT_NA | NA_VALUES | INF_VALUES | INF_NEG_VALUES)
# let app can show preview and import all na column, as string
NORMAL_NULL_VALUES = {'NA', 'na', 'null'}
SPECIAL_SYMBOLS = ALL_SYMBOLS - NORMAL_NULL_VALUES - PREVIEW_ALLOWED_EXCEPTIONS
IS_ERROR_COL = '___ERR0R___'
ERR_COLS_NAME = '___ERR0R_C0LS___'


@log_execution_time()
def write_error_trace(df_error: DataFrame, proc_name, file_path=None, ip_address=None):
    if not len(df_error):
        return df_error

    time_str = convert_time(datetime.now(), format_str=DATE_FORMAT_STR_ONLY_DIGIT)[4:-3]
    ip_address = get_ip_address()
    ip_address = f'_{ip_address}' if ip_address else ''

    base_name = f'_{get_basename(file_path)}' if file_path else ''

    file_name = f'{proc_name}{base_name}_{time_str}{ip_address}{TXT_FILE_TYPE}'
    full_path = os.path.join(get_error_trace_path(), file_name)
    make_dir_from_file_path(full_path)

    df_error.to_csv(full_path, sep=CsvDelimiter.TSV.value, header=None, index=False)

    return df_error


@log_execution_time()
def write_duplicate_import(df: DataFrame, file_name_elements: List):
    if not len(df):
        return df

    file_name = '_'.join([element for element in file_name_elements if element])
    export_file_name = f'{file_name}{TXT_FILE_TYPE}'
    full_path = os.path.join(get_error_duplicate_path(), export_file_name)
    # make folder
    make_dir_from_file_path(full_path)

    df.to_csv(full_path, sep=CsvDelimiter.TSV.value, header=None, index=False)

    return df


@log_execution_time()
def write_error_import(
    df_error: DataFrame,
    proc_name,
    file_path=None,
    error_file_delimiter=CsvDelimiter.CSV.value,
    csv_directory=None,
):
    if not len(df_error):
        return df_error

    if csv_directory:
        file_paths = split_path_to_list(file_path)
        csv_directories = split_path_to_list(csv_directory)
        file_name = file_paths[-1]
        folders = file_paths[len(csv_directories) : -1]
    else:
        time_str = convert_time(format_str=DATE_FORMAT_STR_ONLY_DIGIT)[4:-3]
        file_name = proc_name + '_' + time_str + TXT_FILE_TYPE
        folders = []

    full_path = os.path.join(get_error_import_path(), proc_name, *folders, file_name)
    full_path = full_path.replace('.zip', '')
    make_dir_from_file_path(full_path)

    df_error.to_csv(full_path, sep=error_file_delimiter, index=False)

    return df_error


@log_execution_time(prefix='CAST_DATA_TYPE')
def write_error_cast_data_types(process: CfgProcess, failed_column_data: dict[CfgProcessColumn, list[object]]):
    """
    Export to csv file that contain all failed convert data for all
    :param process: a process object
    :param failed_column_data: a list of columns that failed convert data type
    """
    if not failed_column_data:
        return

    # Create file path & folder
    time_str = convert_time(datetime.now(), format_str=DATE_FORMAT_STR_ONLY_DIGIT)[4:-3]
    ip_address = get_ip_address()
    ip_address = f'_{ip_address}' if ip_address else ''
    file_name = f'{process.name}_{time_str}{ip_address}{TXT_FILE_TYPE}'
    full_path = os.path.join(get_error_cast_path(), file_name)
    make_dir_from_file_path(full_path)

    df = pd.DataFrame()
    for column, data in failed_column_data.items():
        column_name = column.bridge_column_name
        df[column_name] = pd.Series(data, dtype=np.object.__name__)
    df = gen_error_cast_output_df(process, df)

    # write data to file
    df.to_csv(full_path, sep=CsvDelimiter.TSV.value, header=None, index=False)

    return full_path


def gen_error_cast_output_df(process: CfgProcess, df_error: DataFrame) -> DataFrame:
    """
    Generate a dataframe with title & error data
    :param process: a process object
    :param df_error: a dataframe containing error data
    :return: a dataframe with title & error data
    """

    df_output = df_error.copy()
    new_row = df_error.columns.tolist()
    columns = df_error.columns.tolist()
    if len(columns) == 1:
        columns.append('')
        new_row.append('')
        df_output[''] = ''

    df_output = add_row_to_df(df_output, columns, new_row)

    if len(columns) > 1:
        columns = columns[:2]

    new_row = ('', '')
    df_output = add_row_to_df(df_output, columns, new_row)

    new_row = ('Table Name', process.table_name)
    df_output = add_row_to_df(df_output, columns, new_row)

    new_row = ('Process Name', process.name)
    df_output = add_row_to_df(df_output, columns, new_row)

    new_row = ('', '')
    df_output = add_row_to_df(df_output, columns, new_row)

    new_row = ('', '')
    df_output = add_row_to_df(df_output, columns, new_row)

    error_type = CAST_DATA_TYPE_ERROR_MSG
    # error_type += '(!: Target column)'
    new_row = ('Error Type', error_type)
    df_output = add_row_to_df(df_output, columns, new_row)

    return df_output


def gen_error_output_df(csv_file_name, dic_cols, df_error, df_db, error_msgs=None):
    db_len = len(df_db)
    df_db = df_db.append(df_error, ignore_index=True)
    columns = df_db.columns.tolist()

    # error data
    new_row = columns
    df_db = add_row_to_df(df_db, columns, new_row, db_len, rename_err_cols=True)

    new_row = ('column name/sample data (first 10 & last 10)',)
    df_db = add_row_to_df(df_db, columns, new_row, db_len)

    new_row = ('Data File', csv_file_name)
    df_db = add_row_to_df(df_db, columns, new_row, db_len)

    new_row = ('',)
    df_db = add_row_to_df(df_db, columns, new_row, db_len)

    # data in db
    new_row = columns
    selected_columns = list(dic_cols.keys())
    df_db = add_row_to_df(df_db, columns, new_row, selected_columns=selected_columns, mark_not_set_cols=True)

    new_row = ('column name/sample data (latest 5)',)
    df_db = add_row_to_df(df_db, columns, new_row)

    new_row = [DataType(dic_cols[col_name].type).name for col_name in columns if col_name in dic_cols]
    df_db = add_row_to_df(df_db, columns, new_row)

    new_row = ('data type',)
    df_db = add_row_to_df(df_db, columns, new_row)

    new_row = ('Database',)
    df_db = add_row_to_df(df_db, columns, new_row)

    new_row = ('',)
    df_db = add_row_to_df(df_db, columns, new_row)

    new_row = ('',)
    df_db = add_row_to_df(df_db, columns, new_row)

    error_msg = '|'.join(error_msgs) if isinstance(error_msgs, (list, tuple)) else error_msgs

    error_type = error_msg or DATA_TYPE_ERROR_MSG
    error_type += '(!: Target column)'
    new_row = ('Error Type', error_type)
    df_db = add_row_to_df(df_db, columns, new_row)

    if ERR_COLS_NAME in df_db.columns:
        df_db.drop(columns=ERR_COLS_NAME, inplace=True)

    return df_db


@log_execution_time()
def gen_duplicate_output_df(dic_use_cols, df_duplicate, csv_file_name=None, table_name=None, error_msgs=None):
    # db_name: if factory db -> db name
    #                           else if csv -> file name
    columns = df_duplicate.columns.tolist()

    # duplicate data
    new_row = columns
    df_output = add_row_to_df(df_duplicate, columns, new_row)

    new_row = (f'column name/duplicate data (total: {len(df_duplicate)} rows)',)
    df_output = add_row_to_df(df_output, columns, new_row)

    new_row = ('',)
    df_output = add_row_to_df(df_output, columns, new_row)

    new_row = [dic_use_cols[col_name].predict_type for col_name in columns if col_name in dic_use_cols]
    df_output = add_row_to_df(df_output, columns, new_row)

    new_row = ('data type',)
    df_output = add_row_to_df(df_output, columns, new_row)

    new_row = ('',)
    df_output = add_row_to_df(df_output, columns, new_row)

    if csv_file_name:
        new_row = ('Data File', csv_file_name)
        df_output = add_row_to_df(df_output, columns, new_row)

    if table_name:
        new_row = ('Table name', table_name)
        df_output = add_row_to_df(df_output, columns, new_row)

    new_row = ('',)
    df_output = add_row_to_df(df_output, columns, new_row)

    new_row = ('',)
    df_output = add_row_to_df(df_output, columns, new_row)

    error_msg = '|'.join(error_msgs) if isinstance(error_msgs, (list, tuple)) else error_msgs

    new_row = ('Error Type', error_msg or DATA_TYPE_DUPLICATE_MSG)
    df_output = add_row_to_df(df_output, columns, new_row)

    return df_output


def add_row_to_df(df, columns, new_row, pos=0, rename_err_cols=False, selected_columns=[], mark_not_set_cols=False):
    df_temp = pd.DataFrame({columns[i]: new_row[i] for i in range(len(new_row))}, index=[pos])

    error_cols = {}
    if ERR_COLS_NAME in df.columns:
        df = df.astype('string')
        for i in range(len(df)):
            for col_name in df.columns:
                if (
                    not pd.isna(df.iloc[i][ERR_COLS_NAME])
                    and col_name in df.iloc[i][ERR_COLS_NAME]
                    and col_name not in [ERR_COLS_NAME, IS_ERROR_COL]
                ):
                    df.loc[i, col_name] = '{}*****'.format(df.iloc[i][col_name])
                    error_cols[col_name] = '!{}'.format(col_name)
            df.loc[i, ERR_COLS_NAME] = None

    # add ! to head of error columns
    if rename_err_cols:
        for col_val in error_cols:
            df_temp[col_val] = error_cols[col_val]

    # add bracket to unselected columns
    if mark_not_set_cols and len(selected_columns):
        for col_val in columns:
            if col_val not in selected_columns:
                df_temp[col_val] = '({})'.format(col_val)

    df = pd.concat([df.iloc[0:pos], df_temp, df.iloc[pos:]]).reset_index(drop=True)

    return df


@log_execution_time()
def get_new_adding_columns(proc, dic_use_cols):
    proc_id = proc.id

    # exist sensors
    created_at = get_current_timestamp()
    dic_exist_sensor = {s.column_name: s for s in proc.sensors}
    missing_sensors = []
    for col_name, cfg_col in dic_use_cols.items():
        data_type = cfg_col.data_type
        # already exist
        if dic_exist_sensor.get(col_name):
            continue

        data_type_obj = DataType[data_type]
        if data_type_obj is DataType.DATETIME:
            data_type_obj = DataType.TEXT

        sensor = {
            'process_id': proc_id,
            'column_name': col_name,
            'type': data_type_obj.value,
            'created_at': created_at,
            'name_en': to_romaji(col_name),
        }
        missing_sensors.append(sensor)

    return missing_sensors


def csv_data_with_headers(csv_file_name, data_src):
    efa_header_exists = CfgConstant.get_efa_header_flag(data_src.id)
    read_directly_ok = True
    if efa_header_exists:
        try:
            # csv delimiter
            csv_delimiter = get_csv_delimiter(data_src.delimiter)

            # read file directly to get Line, Machine, Process
            csv_reader = read_data(csv_file_name, limit=5, delimiter=csv_delimiter, do_normalize=False)
            next(csv_reader)

            row_line = next(csv_reader)  # 2nd row
            line = normalize_str(row_line[1])  # 2nd cell

            row_process = next(csv_reader)  # 3rd row
            process = normalize_str(row_process[1])  # 2nd cell

            row_machine = next(csv_reader)  # 4th row
            machine = normalize_str(row_machine[1])  # 2nd cell

            etl_headers = {
                WR_HEADER_NAMES: [
                    EFAColumn.Line.name,
                    EFAColumn.Process.name,
                    EFAColumn.Machine.name,
                ],
                WR_VALUES: [line, process, machine],
            }
            return etl_headers[WR_HEADER_NAMES], etl_headers[WR_VALUES]
        except Exception:
            read_directly_ok = False
            traceback.print_exc()

    # if there is no flag in DB or failed to read file directly -> call R script + save flag
    if not efa_header_exists or not read_directly_ok:
        csv_inst, _ = chw.get_file_info_py(csv_file_name)
        if isinstance(csv_inst, Exception):
            return csv_inst

        if csv_inst is None:
            return [], []

        etl_headers = chw.get_etl_headers(csv_inst)

        # save flag to db if header exists
        efa_header_exists = chw.get_efa_header_flag(csv_inst)
        if efa_header_exists:
            with make_session() as session:
                CfgConstant.create_or_update_by_type(
                    session,
                    const_type=CfgConstantType.EFA_HEADER_EXISTS.name,
                    const_name=data_src.id,
                    const_value=EFA_HEADER_FLAG,
                )

        return etl_headers[WR_HEADER_NAMES], etl_headers[WR_VALUES]


@log_execution_time()
def strip_special_symbol(data, is_dict=False):
    # TODO: convert to dataframe than filter is faster , but care about generation purpose ,
    #  we just need to read some rows

    def iter_func(x):
        return x.values() if is_dict else x

    for row in data:
        is_ng = False
        if not row:
            continue
        for val in iter_func(row):
            if str(val).lower() in SPECIAL_SYMBOLS:
                is_ng = True
                break

        if not is_ng:
            yield row


@log_execution_time()
def set_cycle_ids_to_df(df: DataFrame, start_cycle_id):
    """
    reset new cycle id to save to db
    :param df:
    :param start_cycle_id:
    :return:
    """
    df.reset_index(drop=True, inplace=True)
    df.index = df.index + start_cycle_id
    return df


@log_execution_time()
def gen_import_job_info(job_info, save_res, start_time=None, end_time=None, imported_count=0, err_cnt=0, err_msgs=None):
    # start time
    if job_info.auto_increment_end_tm:
        job_info.auto_increment_start_tm = job_info.auto_increment_end_tm
    else:
        job_info.auto_increment_start_tm = start_time

    # end time
    job_info.auto_increment_end_tm = end_time

    if isinstance(save_res, Exception):
        job_info.exception = save_res
        job_info.status = JobStatus.FATAL
    else:
        if imported_count:
            job_info.row_count = imported_count
            job_info.committed_count = imported_count
        else:
            job_info.row_count = save_res
            job_info.committed_count = save_res

        if job_info.err_msg or err_cnt > 0 or err_msgs:
            job_info.status = JobStatus.FAILED
        else:
            job_info.status = JobStatus.DONE

    # set msg
    if job_info.status == JobStatus.FAILED:
        if not job_info.err_msg and not err_msgs:
            msg = DATA_TYPE_ERROR_MSG
            job_info.data_type_error_cnt += err_cnt
        elif isinstance(err_msgs, (list, tuple)):
            msg = ','.join(err_msgs)
        else:
            msg = err_msgs

        if job_info.err_msg and msg:
            job_info.err_msg += msg
        else:
            job_info.err_msg = msg

    return job_info


@log_execution_time()
def validate_data(df: DataFrame, dic_use_cols, na_vals, exclude_cols=None):
    """
    validate data type, NaN values...
    :param df:
    :param dic_use_cols:
    :param na_vals:
    :param exclude_cols:
    :return:
    """

    init_is_error_col(df)

    if exclude_cols is None:
        exclude_cols = []

    exclude_cols.append(IS_ERROR_COL)
    exclude_cols.append(ERR_COLS_NAME)

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
            continue

        # data type that user chose
        user_data_type = dic_use_cols[col_name].data_type

        # do nothing with float column
        if col_name in float_cols and user_data_type != DataType.INTEGER.name:
            continue

        # convert inf , -inf to Nan
        nan, inf_neg_val, inf_val = return_inf_vals(user_data_type)
        if col_name in float_cols and user_data_type == DataType.INTEGER.name:
            df.loc[df[col_name].isin([float('inf'), float('-inf')]), col_name] = nan
            non_na_vals = df[col_name].dropna()
            if len(non_na_vals):
                df.loc[non_na_vals.index, col_name] = df.loc[non_na_vals.index, col_name].astype('Int64')

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
                        df.at[idx, ERR_COLS_NAME] = df[ERR_COLS_NAME].at[idx] + '{},'.format(col_name)

                try:
                    if len(non_num_idxs):
                        pd.to_numeric(df.loc[non_num_idxs.index, col_name], errors='raise')
                except Exception as ex:
                    logger.exception(ex)

            # replace Inf --> None
            if user_data_type == DataType.INTEGER.name:
                df.loc[df[col_name].isin([float('inf'), float('-inf')]), col_name] = nan

        elif user_data_type == DataType.TEXT.name:
            if dtype_name == 'boolean':
                df[col_name] = df[col_name].replace({True: 'True', False: 'False'})
            else:
                idxs = df[col_name].dropna().index
                if dtype_name == 'object':
                    df.loc[idxs, col_name] = df.loc[idxs, col_name].astype(str).str.strip("'").str.strip()
                elif dtype_name == 'string':
                    df.loc[idxs, col_name] = df.loc[idxs, col_name].str.strip("'").str.strip()
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
    df.head()


@log_execution_time()
def add_new_col_to_df(df: DataFrame, col_name, value):
    """
    add new value as a new column in dataframe , but avoid duplicate column name.
    :param df:
    :param col_name:
    :param value:
    :return:
    """
    columns = list(df.columns)
    # avoid duplicate column name
    while col_name in columns:
        col_name = '_' + col_name

    df[col_name] = value

    return col_name


def return_inf_vals(data_type):
    if data_type == DataType.REAL.name:
        return np.nan, float('-inf'), float('inf')
    elif data_type == DataType.INTEGER.name:
        return DEFAULT_NONE_VALUE, DEFAULT_NONE_VALUE, DEFAULT_NONE_VALUE

    return None, '-inf', 'inf'


@log_execution_time()
def data_pre_processing(df, orig_df, dic_use_cols, na_values=None, exclude_cols=None, get_date_col=None):
    if exclude_cols is None:
        exclude_cols = []
    if na_values is None:
        na_values = PANDAS_DEFAULT_NA | NA_VALUES

    # string parse
    cols = get_object_cols(df)
    # keep None value in object column, instead of convert to "None"
    df[cols] = df[cols].fillna(np.nan)
    df[cols] = df[cols].astype(str)
    cols += get_string_cols(df)

    # normalization
    for col in cols:
        df[col] = normalize_df(df, col)

    # parse data type
    validate_data(df, dic_use_cols, na_values, exclude_cols)

    # If there are all invalid values in one row, the row will be invalid and not be imported to database
    # Otherwise, invalid values will be set nan in the row and be imported normally
    if get_date_col:
        # datetime_col as string, but value is 'nan' -> could not filter by isnull
        datetime_series = pd.to_datetime(df[get_date_col])
        is_error_row_series = df.eval(f'{IS_ERROR_COL} == 1') & (
            datetime_series.isnull() | df[set(df.columns) - set(exclude_cols)].isnull().all(axis=1)
        )
    else:
        is_error_row_series = df.eval(f'{IS_ERROR_COL} == 1') & df[set(df.columns) - set(exclude_cols)].isnull().all(
            axis=1,
        )
    df_error = orig_df.loc[is_error_row_series]
    df_error[ERR_COLS_NAME] = df.loc[is_error_row_series][ERR_COLS_NAME]

    # remove status column ( no need anymore )
    df.drop(df[is_error_row_series].index, inplace=True)
    df.drop(IS_ERROR_COL, axis=1, inplace=True)
    df.drop(ERR_COLS_NAME, axis=1, inplace=True)

    return df_error


def get_string_cols(df: DataFrame):
    return [col for col in df.columns if df[col].dtype.name.lower() == 'string']


def get_object_cols(df: DataFrame):
    return [col for col in df.columns if df[col].dtype.name.lower() == 'object']


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


@log_execution_time()
def convert_df_datetime_to_str(df: DataFrame, get_date_col):
    return df[df[get_date_col].notnull()][get_date_col].dt.strftime(DATE_FORMAT_STR)


@log_execution_time()
def validate_datetime(df: DataFrame, date_col, is_strip=True, add_is_error_col=True, null_is_error=True):
    dtype_name = df[date_col].dtype.name
    if dtype_name == 'object':
        df[date_col] = df[date_col].astype(str)
    elif dtype_name != 'string':
        return

    # for csv data
    if is_strip:
        df[date_col] = df[date_col].str.strip("'").str.strip()

    na_value = [np.nan, np.inf, -np.inf, 'nan', '<NA>', np.NAN, pd.NA]
    # convert to datetime value
    if not null_is_error:
        idxs = ~(df[date_col].isin(na_value))

    df[date_col] = pd.to_datetime(df[date_col], errors='coerce')  # failed records -> pd.NaT

    # mark error records
    if add_is_error_col:
        init_is_error_col(df)

        if null_is_error:
            df[IS_ERROR_COL] = np.where(pd.isna(df[date_col]), 1, df[IS_ERROR_COL])
            err_date_col_idxs = df[date_col].isna()
            for idx, _ in err_date_col_idxs.items():
                err_col_name = df[ERR_COLS_NAME].at[idx] + '{},'.format(date_col)
                df.at[idx, ERR_COLS_NAME] = err_col_name if pd.isna(df[date_col].at[idx]) else df[ERR_COLS_NAME].at[idx]
        else:
            df_temp = df.loc[idxs, [date_col, IS_ERROR_COL, ERR_COLS_NAME]]
            # df.loc[idxs, IS_ERROR_COL] = np.where(pd.isna(df.loc[idxs, date_col]), 1, df.loc[idxs, IS_ERROR_COL])
            df_temp[IS_ERROR_COL] = np.where(pd.isna(df_temp[date_col]), 1, df_temp[IS_ERROR_COL])
            df_temp[ERR_COLS_NAME] = np.where(
                pd.isna(df_temp[date_col]),
                df_temp[ERR_COLS_NAME] + date_col + ',',
                df_temp[ERR_COLS_NAME],
            )
            df.loc[idxs, IS_ERROR_COL] = df_temp
            df.loc[idxs, ERR_COLS_NAME] = df_temp

        df.head()


def init_is_error_col(df: DataFrame):
    if IS_ERROR_COL not in df.columns:
        df[IS_ERROR_COL] = 0
    if ERR_COLS_NAME not in df.columns:
        df[ERR_COLS_NAME] = ''


@log_execution_time()
def check_update_time_by_changed_tz(cfg_data_table: CfgDataTable, time_zone=None):
    if time_zone is None:
        time_zone = tz.tzutc()

    data_source = cfg_data_table.data_source
    use_os_tz = data_source.db_detail.use_os_timezone
    # check use ose time zone
    if check_timezone_changed(data_source.id, use_os_tz):
        # convert to local or convert from local
        if use_os_tz:
            # calculate offset +/-HH:MM
            tz_offset = calc_offset_between_two_tz(time_zone, tz.tzlocal())
        else:
            tz_offset = calc_offset_between_two_tz(tz.tzlocal(), time_zone)

        if tz_offset is None:
            return None

        # update time to new time zone
        # TODO: here . delete transaction data
        run_clean_trans_tables_job()

    # save latest use os time zone flag to db
    save_use_os_timezone_to_db(data_source.id, use_os_tz)

    return True


@log_execution_time()
def check_timezone_changed(data_source_id, yml_use_os_timezone):
    """check if use os timezone was changed by user

    Args:
        data_source_id ([type]): [description]
        yml_use_os_timezone ([type]): [description]

    Returns:
        [type]: [description]
    """
    if yml_use_os_timezone is None:
        return False

    db_use_os_tz = CfgConstant.get_value_by_type_name(
        CfgConstantType.USE_OS_TIMEZONE.name,
        data_source_id,
        lambda x: bool(eval(x)),
    )
    if db_use_os_tz is None:
        return False

    if db_use_os_tz == yml_use_os_timezone:
        return False

    return True


@log_execution_time()
def save_use_os_timezone_to_db(data_source_id, yml_use_os_timezone):
    """save os timezone to constant table

    Args:
        data_source_id ([type]): [description]
        yml_use_os_timezone ([type]): [description]

    Returns:
        [type]: [description]
    """
    if not yml_use_os_timezone:
        yml_use_os_timezone = False

    with make_session() as session:
        CfgConstant.create_or_update_by_type(
            session,
            const_type=CfgConstantType.USE_OS_TIMEZONE.name,
            const_value=str(yml_use_os_timezone),
            const_name=data_source_id,
        )

    return True


@log_execution_time()
def gen_insert_cycle_values(df):
    cycle_vals = df.replace({pd.NA: np.nan}).to_records(index=False).tolist()
    return cycle_vals


@log_execution_time()
def insert_data(db_instance, sql, vals):
    try:
        db_instance.execute_sql_in_transaction(sql, vals)
        return True
    except Exception as e:
        logger.error(e)
        return False


@log_execution_time()
def gen_bulk_insert_sql(tblname, cols_str, params_str):
    sql = f'INSERT INTO {tblname} ({cols_str}) VALUES ({params_str})'

    return sql


@log_execution_time()
def get_insert_params(columns):
    cols_str = ','.join(columns)
    params_str = ','.join([PARAM_SYMBOL] * len(columns))

    return cols_str, params_str


@log_execution_time()
def get_df_first_n_last(df: DataFrame, first_count=10, last_count=10):
    if len(df) <= first_count + last_count:
        return df

    return df.loc[df.head(first_count).index.append(df.tail(last_count).index)]


@log_execution_time()
def get_tzoffset_of_random_record(data_source, table_name, get_date_col):
    # exec sql
    with ReadOnlyDbProxy(data_source) as db_instance:
        # get timezone offset
        db_timezone = get_db_timezone(db_instance)

        sql = gen_sql(db_instance, table_name, get_date_col)
        _, rows = db_instance.run_sql(sql, row_is_dict=False)

        date_val = None
        tzoffset_str = None
        if rows:
            date_val, tzoffset_str = rows[0]

    return date_val, tzoffset_str, db_timezone


def add_transaction_import_job(
    process_id: int,
    interval_sec: int = None,
    run_now: bool = False,
    is_past: bool = False,
    register_by_file_request_id: str = None,
):
    exist_proc_ids = MProcess.get_existed_process_ids()
    if process_id not in exist_proc_ids:
        return

    job_name = JobType.TRANSACTION_PAST_IMPORT.name if is_past else JobType.TRANSACTION_IMPORT.name
    job_id = f'{job_name}_{process_id}'

    if interval_sec:
        trigger = IntervalTrigger(seconds=interval_sec, timezone=utc)
    else:
        trigger = DateTrigger(datetime.now().astimezone(utc), timezone=utc)

    dic_import_param = {
        '_job_id': job_id,
        '_job_name': job_name,
        'data_table_id': None,
        'process_id': process_id,
        'is_past': is_past,
        'register_by_file_request_id': register_by_file_request_id,
    }

    add_job_to_scheduler(job_id, job_name, trigger, import_transaction_per_process_job, run_now, dic_import_param)


def remove_transaction_import_jobs(process_id: int) -> None:
    job_ids = [JobType.transaction_import_job_id(process_id, is_past=is_past) for is_past in [True, False]]
    for job_id in job_ids:
        job = scheduler.get_job(job_id)
        if job is not None:
            scheduler.remove_job(job_id)
