from __future__ import annotations

import codecs
import copy
import functools
import json
import locale
import os
import pickle
import re
import shutil
import socket
import sys
import zipfile
from collections import OrderedDict
from datetime import datetime, timedelta
from enum import Enum
from functools import wraps
from io import IOBase
from itertools import chain, permutations
from multiprocessing import Manager
from pathlib import Path
from typing import IO, List, TextIO, Tuple, Union

import chardet
import numpy as np
import pandas as pd

# from charset_normalizer import detect
from dateutil import parser, tz
from dateutil.relativedelta import relativedelta
from flask import g
from flask_assets import Bundle, Environment
from langdetect import detect_langs
from pandas import DataFrame, Series
from pandas.api.types import is_datetime64_any_dtype, is_numeric_dtype, is_string_dtype
from pandas.io import parquet
from pyarrow import feather

from ap.common.constants import (
    ANALYSIS_INTERFACE_ENV,
    DEFAULT_NONE_VALUE,
    EMPTY_STRING,
    ENCODING_ASCII,
    ENCODING_SHIFT_JIS,
    ENCODING_UTF_8,
    ENCODING_UTF_8_BOM,
    HALF_WIDTH_SPACE,
    INT16_MAX,
    INT16_MIN,
    INT32_MAX,
    INT32_MIN,
    INT64_MAX,
    INT64_MIN,
    LANGUAGES,
    LEFT_Z_TILL_SYMBOL,
    LOCK,
    MAPPING_DATA_LOCK,
    PROCESS_QUEUE,
    PROCESS_QUEUE_FILE_NAME,
    SAFARI_SUPPORT_VER,
    SQL_COL_PREFIX,
    TIME_COL,
    ZERO_FILL_PATTERN,
    ZERO_FILL_PATTERN_2,
    AbsPath,
    AppEnv,
    CsvDelimiter,
    CSVExtTypes,
    DataGroupType,
    DataType,
    FileExtension,
    FilterFunc,
    FlaskGKey,
    ListenNotifyType,
    RawDataTypeDB,
    ServerType,
)
from ap.common.logger import log_execution_time, logger
from ap.common.pysize import get_size
from ap.common.services.jp_to_romaji_utils import replace_special_symbols, to_romaji
from ap.common.services.normalization import NORMALIZE_FORM_NFKD, normalize_str, unicode_normalize

INCLUDES = ['*.csv', '*.tsv']
# TODO: confirm PO that we use datetime format like postgresql datetime column( to avoid convert time)
DATE_FORMAT_STR_SQLITE = '%Y-%m-%dT%H:%M:%S.%fZ'
DATE_FORMAT_STR_POSTGRES = '%Y-%m-%d %H:%M:%S.%f'  # use bridge station
TXT_FILE_TYPE = '.txt'
DATE_FORMAT_STR = '%Y-%m-%dT%H:%M:%S.%fZ'
DATE_FORMAT_SQLITE_STR = '%Y-%m-%dT%H:%M:%S'
DATE_FORMAT_QUERY = '%Y-%m-%dT%H:%M:%S.%f'
DATE_FORMAT_STR_CSV = '%Y-%m-%d %H:%M:%S.%f'
DATE_FORMAT_STR_FACTORY_DB = '%Y-%m-%d %H:%M:%S.%f'
DATE_FORMAT_STR_ONLY_DIGIT = '%Y%m%d%H%M%S.%f'
DATE_FORMAT_STR_YYYYMM = '%Y%m'
DATE_FORMAT = '%Y-%m-%d'
TIME_FORMAT = '%H:%M'
TIME_FORMAT_WITH_SEC = '%H:%M:%S'
RL_DATETIME_FORMAT = '%Y-%m-%dT%H:%M'
DATE_FORMAT_STR_ONLY_DIGIT_SHORT = '%Y%m%d%H%M%S'
DATE_FORMAT_STR_PARTITION_VALUE = '%Y-%m-%d'
SQL_DATE_FORMAT_STR = 'YYYY-MM-DDThh:mi:ss.msZ'
GPRC_LIMITATION_SIZE = 4194304
DATE_FORMAT_SIMPLE = '%Y-%m-%d %H:%M:%S'
DATE_FORMAT_FOR_ONE_HOUR = '%Y-%m-%d %H:00:00'
API_DATETIME_FORMAT = '%Y-%m-%dT%H:%MZ'
# for data count
TERM_FORMAT = {'year': '%Y', 'month': '%Y-%m', 'week': DATE_FORMAT}
FREQ_FOR_RANGE = {
    'year': ('M', 'Y', '%Y'),
    'month': ('D', 'M', '%Y-%m'),
    'week': ('H', 'D', '%Y-%m-%d'),
}

EXTENSIONS = [CSVExtTypes.CSV.value, CSVExtTypes.TSV.value, CSVExtTypes.SSV.value, CSVExtTypes.ZIP.value]

dict_server_type_text = {
    ServerType.EdgeServer: 'Edge Server',
    ServerType.BridgeStationGrpc: 'Bridge Station',
    ServerType.BridgeStationWeb: 'Bridge Station',
    ServerType.IntegrationServer: 'Integration Server',
}


class PostgresFormatStrings(Enum):
    DATE = '%Y-%m-%d'
    TIME = '%H:%M:%S'
    DATETIME = '%Y-%m-%d %H:%M:%S.%f'


def parse_int_value(value):
    """
    Parse integral value from text or numeric data
    :param value:
    :return: parsed integral value.
    """
    if isinstance(value, str):
        value = unicode_normalize(value, convert_irregular_chars=False)
        if value.isdigit():
            return int(value)
    elif isinstance(value, int):
        return value

    return None


def gen_sql_cast_text(col):
    return f'CAST({col} as TEXT) AS {col}'


def gen_sql_cast_text_no_as(col):
    return f'CAST({col} as TEXT)'


def get_current_timestamp():
    return datetime.utcnow()


def dict_deep_merge(source, destination):
    """
    Deep merge two dictionary to one.

    >>> a = { 'first' : { 'all_rows' : { 'pass' : 'dog', 'number' : '1' } } }
    >>> b = { 'first' : { 'all_rows' : { 'fail' : 'cat', 'number' : '5' } } }
    >>> dict_deep_merge(b, a) == { 'first' : { 'all_rows' : { 'pass' : 'dog', 'fail' : 'cat', 'number' : '5' } } }
    """
    if source:
        for key, value in source.items():
            if isinstance(value, dict) and destination.get(key):
                # get node or create one
                node = destination.setdefault(key, {})
                dict_deep_merge(value, node)
            else:
                destination[key] = copy.deepcopy(value)

    return destination


def convert_json_to_ordered_dict(json):
    """
    Deeply convert a normal dict to OrderedDict.
    :param json: input json
    :return: ordered json
    """
    if isinstance(json, dict):
        ordered_json = OrderedDict(json)
        try:
            for key, value in ordered_json.items():
                ordered_json[key] = convert_json_to_ordered_dict(value)
        except AttributeError:
            pass
        return ordered_json

    return json


def get_latest_file(root_name):
    try:
        latest_files = get_files(
            root_name,
            depth_from=1,
            depth_to=100,
            extension=EXTENSIONS,
        )
        latest_files.sort()
        return latest_files[-1].replace(os.sep, '/')
    except Exception:
        return ''


def get_sorted_files(root_name) -> List[str]:
    try:
        latest_files = get_files(root_name, depth_from=1, depth_to=100, extension=EXTENSIONS)
        latest_files = [file_path.replace(os.sep, '/') for file_path in latest_files]
        latest_files.sort(reverse=True)
        return latest_files
    except Exception:
        return []


@log_execution_time()
def is_normal_zip(f_name: Union[str, Path]) -> bool:
    return Path(f_name).suffix == '.zip'


def get_largest_files_in_list(files: List[str]) -> List[str]:
    sorted_files = sorted(files, key=lambda x: os.path.getsize(x))
    sorted_files.reverse()
    return sorted_files


def get_sorted_files_in_list(files: List[str]) -> List[str]:
    sorted_files = sorted(files, key=lambda x: (os.path.getsize(x), os.path.getctime(x)))
    sorted_files.reverse()
    return sorted_files


def get_sorted_files_by_size(root_name: str) -> List[str]:
    try:
        files = get_files(root_name, depth_from=1, depth_to=100, extension=EXTENSIONS)
        largest_files = get_largest_files_in_list(files)
        return largest_files
    except FileNotFoundError:
        return []


def get_sorted_files_by_size_and_time(root_name: str) -> List[str]:
    try:
        files = get_files(root_name, depth_from=1, depth_to=100, extension=EXTENSIONS)
        largest_files = get_sorted_files_in_list(files)
        return largest_files
    except FileNotFoundError:
        return []


def get_latest_files(root_name: Union[Path, str]) -> List[str]:
    try:
        files = get_files(
            str(root_name),
            depth_from=1,
            depth_to=100,
            extension=EXTENSIONS,
        )
        files.sort(key=lambda x: os.path.getmtime(x), reverse=True)
        files = [f.replace(os.sep, '/') for f in files]
        return files
    except FileNotFoundError:
        return []


def start_of_minute(start_date, start_tm, delimiter=HALF_WIDTH_SPACE):
    if start_date is None or start_tm is None:
        return None
    if start_tm and len(start_tm) == 5:
        start_tm = start_tm + ':00'

    return '{}{}{}'.format(start_date.replace('/', '-'), delimiter, start_tm)


def end_of_minute(start_date, start_tm, delimiter=HALF_WIDTH_SPACE):
    if start_date is None or start_tm is None:
        return None
    if start_tm and len(start_tm) == 5:
        start_tm = start_tm + ':00'

    # start_tm = start_tm[:8] + '.999999'
    start_tm = start_tm[:8]

    return '{}{}{}'.format(start_date.replace('/', '-'), delimiter, start_tm)


def clear_special_char(target):
    if not target:
        return target

    if isinstance(target, (list, tuple)):
        return [_clear_special_char(s) for s in target]
    elif isinstance(target, str):
        return _clear_special_char(target)


def _clear_special_char(target_str):
    if not target_str:
        return target_str

    output = target_str
    for s in ('"', "'", '*'):
        output = output.replace(s, '')

    return output


def universal_db_exists():
    universal_db = os.path.join(os.getcwd(), 'instance', 'universal.sqlite3')
    # if getattr(sys, 'frozen', False): # Use for EXE file only
    instance_folder = os.path.join(os.getcwd(), 'instance')
    if not os.path.exists(instance_folder):
        os.makedirs(instance_folder)
    return os.path.exists(universal_db)


# convert time before save to database YYYY-mm-DDTHH:MM:SS.NNNNNNZ
def convert_time(
    time: object = None,
    format_str: str = DATE_FORMAT_STR,
    return_string: bool = True,
    only_millisecond: bool = False,
    without_timezone: bool = False,
    remove_ms: object = False,
) -> Union[datetime, str]:
    if not time or time == 'NaT' or pd.isna(time):
        time = datetime.utcnow()
    elif isinstance(time, str):
        if len(time) == 6:
            time = f'{time}01'

        time = parser.parse(time)
    else:
        time = parser.parse(str(time))

    if return_string:
        time = time.strftime(format_str)
        if only_millisecond:
            time = time[:-3]
        elif remove_ms:
            time = time[:-8]

    if without_timezone:
        time = time.replace(tzinfo=None)

    return time


def add_delta_to_datetime(
    datetime_input: object,
    days: int = 0,
    hours: int = 0,
    minutes: int = 0,
    seconds: int = 0,
    time_delta: timedelta = None,
    is_mssql_datetime: bool = False,
):
    """
    Add time delta to input datetime without changing format

    :param datetime_input: input datetime, string type or datetime type
    :param days: among of days want to add
    :param hours: among of days want to add
    :param minutes: among of days want to add
    :param seconds: among of days want to add
    :param time_delta: time_delta want to add
    :param is_mssql_datetime: is format datetime in SQL Server (format: YYYY-MM-DD hh:mm:ss[.nnn])
    :return: datetime that added time delta with original format
    """

    result = datetime_input
    if datetime_input is None:
        return result

    delta = time_delta if time_delta else timedelta(days=days, hours=hours, minutes=minutes, seconds=seconds)
    if isinstance(datetime_input, str):
        # Regex filter for date with time +- timezone
        regex = r'^(\d{4})(|[\/-])(\d{2})(|[\/-])(\d{2})(|[\sT])(|\d{2})(|[:])(|\d{2})(|[:])(|\d{2})(|\.\d+)(|[\sZ])'
        regex += r'(|[+-]\d{1,2}[:]\d{2})$'
        template = '{}{}{}{}{}{}{}{}{}{}{}{}{}'
        mode = 1
        try:
            (
                year,
                ym_delimiter,
                month,
                md_delimiter,
                day,
                dt_delimiter,
                hour,
                hm_delimiter,
                minute,
                ms_delimiter,
                second,
                microsecond,
                ending,
                timezone,
            ) = re.search(regex, datetime_input).groups()
        except Exception:
            mode = 2
            # date time format dd-mm-yyyy
            regex = (
                r'^(\d{2})(|[\/-])(\d{2})(|[\/-])(\d{4})(|[\sT])(|\d{2})(|[:])(|\d{2})(|[:])(|\d{2})(|\.\d+)(|[\sZ])'
            )
            regex += r'(|[+-]\d{1,2}[:]\d{2})$'
            (
                day,
                ym_delimiter,
                month,
                md_delimiter,
                year,
                dt_delimiter,
                hour,
                hm_delimiter,
                minute,
                ms_delimiter,
                second,
                microsecond,
                ending,
                timezone,
            ) = re.search(regex, datetime_input).groups()

        s_microsecond = microsecond.replace('.', '')
        i_year, i_month, i_day = int(year), int(month), int(day)
        i_hour, i_minute, i_second = (int(value) if value != '' else 0 for value in [hour, minute, second])
        i_microsecond = int(s_microsecond.ljust(6, '0')) if s_microsecond != '' else 0

        datetime_obj = datetime(i_year, i_month, i_day, i_hour, i_minute, i_second, i_microsecond)
        new_datetime_obj = datetime_obj + delta

        year, month, day = (
            new_datetime_obj.year,
            f'{new_datetime_obj.month:0>2}',
            f'{new_datetime_obj.day:0>2}',
        )
        hour, minute, second = (
            f'{new_datetime_obj.__getattribute__(name):0>2}' if old_value != '' else old_value
            for name, old_value in zip(['hour', 'minute', 'second'], [hour, minute, second])
        )
        microsecond = f'.{new_datetime_obj.microsecond:0>{len(microsecond) - 1}}' if microsecond != '' else microsecond
        if is_mssql_datetime and microsecond != '' and len(s_microsecond) < 4 < len(microsecond):
            microsecond = microsecond[:4]  # datetime - YYYY-MM-DD hh:mm:ss[.nnn]

        if mode == 1:
            result = template.format(
                year,
                ym_delimiter,
                month,
                md_delimiter,
                day,
                dt_delimiter,
                hour,
                hm_delimiter,
                minute,
                ms_delimiter,
                second,
                microsecond,
                ending,
                timezone,
            )
        else:
            result = template.format(
                day,
                ym_delimiter,
                month,
                md_delimiter,
                year,
                dt_delimiter,
                hour,
                hm_delimiter,
                minute,
                ms_delimiter,
                second,
                microsecond,
                ending,
                timezone,
            )

    if isinstance(datetime_input, datetime):
        result = datetime_input + delta

    return result


def calculator_month_ago(from_time, to_time=None):
    if not to_time:
        to_time = datetime.now()
    month_ago = (to_time.year - from_time.year) * 12 + to_time.month - from_time.month
    if to_time.day < from_time.day:
        month_ago -= 1
    return month_ago


def add_miliseconds(time=None, milis=0):
    """add miliseconds

    Keyword Arguments:
        time {[type]} -- [description] (default: {datetime.now()})
        days {int} -- [description] (default: {0})

    Returns:
        [type] -- [description]
    """
    if not time:
        time = datetime.utcnow()

    return time + timedelta(milliseconds=milis)


def calculator_day_ago(from_time, is_tz_col, to_time=None):
    if not to_time:
        to_time = datetime.now()
        if is_tz_col:
            to_time = datetime.utcnow().replace(tzinfo=tz.tzutc())
    day_ago = (to_time - from_time).days
    return day_ago


def add_seconds(time=None, seconds=0):
    """add seconds

    Keyword Arguments:
        time {[type]} -- [description] (default: {datetime.now()})
        days {int} -- [description] (default: {0})

    Returns:
        [type] -- [description]
    """
    if not time:
        time = datetime.utcnow()

    return time + timedelta(seconds=seconds)


def add_days(time=datetime.utcnow(), days=0):
    """add days

    Keyword Arguments:
        time {[type]} -- [description] (default: {datetime.now()})
        days {int} -- [description] (default: {0})

    Returns:
        [type] -- [description]
    """
    return time + timedelta(days)


def add_months(time=None, months=0, is_format_yymm=False):
    """
    add month
    :param time:
    :param months:
    :return:
    """
    if time is None:
        time = datetime.utcnow()

    output = time + relativedelta(months=months)
    if is_format_yymm:
        output = output.strftime('%Y%m')
        output = output[2:]

    return output


def add_years(time=datetime.utcnow(), years=0):
    """add days

    Keyword Arguments:
        time {[type]} -- [description] (default: {datetime.now()})
        years {int} -- [description] (default: {0})

    Returns:
        [type] -- [description]
    """
    return time + relativedelta(years=years)


@log_execution_time()
def get_files(directory, depth_from=1, depth_to=2, extension=[''], file_name_only=False):
    """get files in folder

    Arguments:
        directory {[type]} -- [description]

    Keyword Arguments:
        depth_limit {int} -- [description] (default: 2)
        extension {list} -- [description] (default: [''])
        in_modified_days {int} -- [description] (default: 0)

    Returns:
        [type] -- [description]
    """
    output_files = []
    if not directory:
        return output_files

    if not check_exist(directory):
        raise FileNotFoundError('Folder not found!')

    root_depth = directory.count(os.path.sep)
    for root, _, files in os.walk(directory):
        # limit depth of recursion
        current_depth = root.count(os.path.sep) + 1
        # assume that directory depth is 1, sub folders are 2, 3, ...
        # default is to just read children sub folder, depth from 1 to 2
        if (current_depth < root_depth + depth_from) or (current_depth > root_depth + depth_to):
            continue

        # list files with extension
        for file in files:
            # Check file is modified in [in_modified_days] days or not
            if any(file.lower().endswith(ext) for ext in extension):
                if file_name_only:
                    output_files.append(file)
                else:
                    output_files.append(os.path.join(root, file))

    return output_files


def add_grave_accent(instr: str):
    """add grave accent to a string (column name) FOR MYSQL only

    Arguments:
        instr {str} -- [description]

    Returns:
        [type] -- [description]
    """
    if not instr:
        return instr

    instr = instr.strip('`')

    return f'`{instr}`'


def add_double_quotes(instr: str):
    """add double quotes to a string (column name)

    Arguments:
        instr {str} -- [description]

    Returns:
        [type] -- [description]
    """
    if not instr:
        return instr

    instr = instr.strip('"')

    return f'"{instr}"'


def guess_data_types(instr: str):
    """guess data type of all kind of databases to 4 type (INTEGER,REAL,DATETIME,TEXT)

    Arguments:
        instr {str} -- [description]

    Returns:
        [type] -- [description]
    """
    dates = ['date', 'time']
    ints = ['int', 'bit', r'num.*\([^,]+$', r'num.*\(.*,\ *0']
    reals = ['num', 'real', 'float', 'double', 'long', 'dec', 'money']

    instr = instr.lower()
    for data_type in dates:
        if re.search(data_type, instr):
            return DataType.DATETIME

    for data_type in ints:
        if re.search(data_type, instr):
            return DataType.INTEGER

    for data_type in reals:
        if re.search(data_type, instr):
            return DataType.REAL
    return DataType.TEXT


def resource_path(*relative_path, level=AbsPath.SHOW):
    """make absolute path

    Keyword Arguments:
        level {int} -- [0: auto, 1: user can see folder, 2: user can not see folder(MEIPASS)] (default: {0})

    Returns:
        [type] -- [description]
    """

    show_path = os.getcwd()
    hide_path = getattr(sys, '_MEIPASS', show_path)

    if level is AbsPath.SHOW:
        basedir = show_path
    elif level is AbsPath.HIDE or getattr(sys, 'frozen', False):
        basedir = hide_path
    else:
        basedir = show_path

    return os.path.join(basedir, *relative_path)


def get_file_size(f_name):
    """get file size

    Arguments:
        f_name {[type]} -- [description]

    Returns:
        [type] -- [description]
    """
    return os.stat(f_name).st_size


def create_file_path(prefix, suffix='.tsv', dt=None):
    f_name = f'{prefix}_{convert_time(dt, format_str=DATE_FORMAT_STR_ONLY_DIGIT)}{suffix}'
    file_path = resource_path(get_data_path(abs=False), f_name, level=AbsPath.SHOW)

    if not os.path.exists(os.path.dirname(file_path)):
        os.makedirs(os.path.dirname(file_path))

    return file_path


def copy_file(source, target):
    """copy file

    Arguments:
        source {[type]} -- [description]
        target {[type]} -- [description]
    """
    if not check_exist(source):
        return False

    shutil.copy2(source, target)
    return True


# def path_split_all(path):
#     """split all part of a path
#
#     Arguments:
#         path {[string]} -- [full path]
#     """
#     allparts = []
#     while True:
#         parts = os.path.split(path)
#         if parts[0] == path:  # sentinel for absolute paths
#             allparts.insert(0, parts[0])
#             break
#         elif parts[1] == path:  # sentinel for relative paths
#             allparts.insert(0, parts[1])
#             break
#         else:
#             path = parts[0]
#             allparts.insert(0, parts[1])
#
#     return allparts


def get_data_path(abs=True):
    """get data folder path

    Returns:
        [type] -- [description]
    """
    folder_name = 'data'
    return resource_path(folder_name, level=AbsPath.SHOW) if abs else folder_name


def get_log_path(abs=True):
    """get data folder path

    Returns:
        [type] -- [description]
    """
    folder_name = 'log'
    return resource_path(folder_name, level=AbsPath.SHOW) if abs else folder_name


def get_error_trace_path(abs=True):
    """get import folder path

    Returns:
        [type] -- [description]
    """
    folder_name = ['error', 'trace']
    return resource_path(*folder_name, level=AbsPath.SHOW) if abs else folder_name


def get_error_cast_path(abs=True):
    """get error cast folder path

    Returns:
        [type] -- [description]
    """
    folder_name = ['error', 'cast']
    return resource_path(*folder_name, level=AbsPath.SHOW) if abs else folder_name


def get_error_duplicate_path(abs=True):
    """get duplicate folder path

    Returns:
        [type] -- [description]
    """
    folder_name = ['error', 'duplicate']
    return resource_path(*folder_name, level=AbsPath.SHOW) if abs else folder_name


def get_error_import_path(abs=True):
    """get import folder path

    Returns:
        [type] -- [description]
    """
    folder_name = ['error', 'import']
    return resource_path(*folder_name, level=AbsPath.SHOW) if abs else folder_name


def get_about_md_file():
    """
    get about markdown file path
    """
    folder_name = 'about'
    file_name = 'Endroll.md'
    return resource_path(folder_name, file_name, level=AbsPath.SHOW)


def get_terms_of_use_md_file(current_locale):
    """
    get about markdown file path
    """
    folder_name = 'about'
    file_name = 'terms_of_use_jp.md' if current_locale.language == 'ja' else 'terms_of_use_en.md'
    return resource_path(folder_name, file_name, level=AbsPath.SHOW)


def get_wrapr_path():
    """get wrap r folder path

    Returns:
        [type] -- [description]
    """
    folder_names = ['ap', 'script', 'r_scripts', 'wrapr']
    return resource_path(*folder_names, level=AbsPath.SHOW)


def get_temp_path():
    """get temporaty folder path

    Returns:
        [type] -- [description]
    """
    folder_name = 'temp'
    data_folder = get_data_path()
    return resource_path(data_folder, folder_name, level=AbsPath.SHOW)


def get_error_path():
    """get error folder path

    Returns:
        [type] -- [description]
    """
    folder_name = 'error'
    data_folder = get_data_path()
    return resource_path(data_folder, folder_name, level=AbsPath.SHOW)


def get_transaction_import_path():
    """get import transaction folder path

    Returns:
        [type] -- [description]
    """
    folder_name = 'transaction_import_file'
    data_folder = get_data_path()
    return resource_path(data_folder, folder_name, level=AbsPath.SHOW)


def get_import_transaction_past_path():
    folder_name = 'past'
    data_folder = get_transaction_import_path()
    return resource_path(data_folder, folder_name, level=AbsPath.SHOW)


def get_import_transaction_future_path():
    folder_name = 'future'
    data_folder = get_transaction_import_path()
    return resource_path(data_folder, folder_name, level=AbsPath.SHOW)


def get_nayose_path():
    """Get nayose folder path

    Returns:
        [type] -- [description]
    """
    folder_name = 'nayose'
    data_folder = get_data_path()
    return resource_path(data_folder, folder_name, level=AbsPath.SHOW)


def get_dummy_data_path():
    """Get dummy data folder path

    Returns:
        [type] -- [description]
    """
    folder_names = ['data_files']
    return resource_path(*folder_names, level=AbsPath.SHOW)


def get_update_master_script_path():
    folder_name = 'update_master_script'
    data_folder = get_dummy_data_path()
    return resource_path(data_folder, folder_name, level=AbsPath.SHOW)


def get_preview_data_path():
    folder_name = 'preview'
    data_folder = get_data_path()
    return resource_path(data_folder, folder_name, level=AbsPath.SHOW)


def get_transaction_data_unknown_master_path():
    folder_name = 'transaction_data_unknown_master'
    data_folder = get_data_path()
    return resource_path(data_folder, folder_name, level=AbsPath.SHOW)


def get_dummy_data_config_path(filename=None):
    folder_name = 'config'
    dummy_data_path = get_dummy_data_path()

    full_path = [dummy_data_path, folder_name]
    if filename:
        full_path.append(filename)
    return resource_path(*full_path, level=AbsPath.SHOW)


def get_export_data_path():
    folder_name = 'export_data'
    data_folder = get_data_path()
    return resource_path(data_folder, folder_name, level=AbsPath.SHOW)


def get_cache_path():
    """get cache folder path

    Returns:
        [type] -- [description]
    """
    folder_name = 'cache'
    data_folder = get_data_path()
    return resource_path(data_folder, folder_name, level=AbsPath.SHOW)


def get_trans_zip_path():
    """get v2 zip folder

    Returns:
        [type] -- [description]
    """
    folder_name = 'transaction_zip_file'
    data_folder = get_data_path()
    return resource_path(data_folder, folder_name, level=AbsPath.SHOW)


def get_export_path():
    """get cache folder path

    Returns:
        [type] -- [description]
    """
    folder_name = 'export'
    data_folder = get_data_path()
    return resource_path(data_folder, folder_name, level=AbsPath.SHOW)


def get_export_setting_path():
    """get cache folder path

    Returns:
        [type] -- [description]
    """
    folder_name = 'export_setting'
    return resource_path(folder_name, level=AbsPath.SHOW) if abs else folder_name


def get_init_path():
    """get init folder path

    Returns:
        [type] -- [description]
    """
    folder_name = 'init'
    return resource_path(folder_name, level=AbsPath.SHOW) if abs else folder_name


def get_view_path():
    """get view/image folder path

    Returns:
        [type] -- [description]
    """
    folder_name = 'view'
    data_folder = get_data_path()
    return resource_path(data_folder, folder_name, level=AbsPath.SHOW)


def get_etl_path(*sub_paths):
    """get etl output folder path

    Returns:
        [type] -- [description]
    """
    folder_name = 'etl'
    data_folder = get_data_path()

    return resource_path(data_folder, folder_name, *sub_paths, level=AbsPath.SHOW)


def get_backup_data_path():
    folder_name = 'backup_data'
    data_folder = get_data_path()
    return resource_path(data_folder, folder_name, level=AbsPath.SHOW)


def get_backup_data_folder(process_id):
    folder = get_backup_data_path()
    if not check_exist(folder):
        os.makedirs(folder)
    return os.path.join(folder, str(process_id))


def chunks(lst, size=900):
    """Yield n-sized chunks from lst."""
    for i in range(0, len(lst), size):
        yield lst[i : i + size]


def get_base_dir(path, is_file=True):
    dir_name = os.path.dirname(path) if is_file else path
    return os.path.basename(dir_name)


def make_dir(dir_path):
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)

    return True


def get_basename(path):
    return os.path.basename(path)


def handle_read_only(check_sql_statement: bool = True):
    def decorator(fn):
        @wraps(fn)
        def wrapper(self, *args, **kwargs):
            if not self.read_only:
                return fn(self, *args, **kwargs)

            msg = 'Read only connection -> Cannot execute sql make any changes in database !!!'
            if not check_sql_statement:
                raise Exception(msg)

            changes_keywords = [
                'ADD',
                'ALTER',
                'CREATE',
                'REPLACE',
                'DELETE',
                'DROP',
                'EXEC',
                'PROCEDURE',
                'SET',
                'UPDATE',
                'TRUNCATE',
            ]
            pattern = re.compile(rf'(\s|\b)({"|".join(changes_keywords)})\s', re.IGNORECASE)
            arg_sql = args[0] if 'sql' not in kwargs else kwargs.get('sql')
            matched = re.match(pattern, arg_sql)
            if matched:  # in case sql statement contains changes data keywords.
                raise Exception(msg)

            result = fn(self, *args, **kwargs)

            return result

        return wrapper

    return decorator


def strip_all_quote(instr):
    return str(instr).strip("'").strip('"')


def get_csv_delimiter(csv_delimiter):
    """
    return tab , comma depend on input data
    :param csv_delimiter:
    :return:
    """
    if csv_delimiter is None:
        return CsvDelimiter.CSV.value

    if isinstance(csv_delimiter, CsvDelimiter):
        return csv_delimiter.value

    return CsvDelimiter[csv_delimiter].value


def sql_regexp(expr, item):
    reg = re.compile(expr, re.I)
    return reg.search(str(item)) is not None


def set_sqlite_params(conn):
    cursor = conn.cursor()
    cursor.execute('PRAGMA journal_mode=WAL')
    cursor.execute('PRAGMA synchronous=NORMAL')
    cursor.execute('PRAGMA cache_size=10000')
    cursor.execute('pragma mmap_size = 30000000000')
    cursor.execute('PRAGMA temp_store=MEMORY')
    cursor.close()


def gen_sql_label(*args):
    return SQL_COL_PREFIX + SQL_COL_PREFIX.join([str(name).strip(SQL_COL_PREFIX) for name in args if name is not None])


def gen_proc_time_label(proc_id: int) -> str:
    return f'{TIME_COL}_{proc_id}'


def gen_sql_like_value(val, func: FilterFunc, position=None):
    if func is FilterFunc.STARTSWITH:
        return [val + '%']

    if func is FilterFunc.ENDSWITH:
        return ['%' + val]

    if func is FilterFunc.CONTAINS:
        return ['%' + val + '%']

    if func is FilterFunc.SUBSTRING:
        if position is None:
            position = 1
        return ['_' * max(0, (position - 1)) + val + '%']

    if func is FilterFunc.AND_SEARCH:
        conds = set(val.split())
        cond_patterns = list(permutations(conds))  # temp solution, conditions are not so many
        return ['%' + '%'.join(cond_pattern) + '%' for cond_pattern in cond_patterns]

    if func is FilterFunc.OR_SEARCH:
        return ['%' + cond + '%' for cond in val.split()]

    return []


def make_dir_from_file_path(file_path):
    dirname = os.path.dirname(file_path)
    # make dir
    if not os.path.exists(dirname):
        os.makedirs(dirname)

    return dirname


def delete_file(file_path):
    if os.path.exists(file_path):
        os.remove(file_path)


def rename_file(src, des):
    if os.path.exists(src):
        os.rename(src, des)


def check_exist(file_path):
    return os.path.exists(file_path)


def count_file_in_folder(folder_path):
    if not check_exist(folder_path):
        return 0

    return len([name for name in os.listdir(folder_path) if name.endswith('.sqlite3')])


def calc_overflow_boundary(arr):
    if len(arr):
        q1, q3 = np.quantile(arr, [0.25, 0.75], interpolation='midpoint')
        iqr = q3 - q1
        if iqr:
            lower_boundary = q1 - 4.5 * iqr
            upper_boundary = q3 + 4.5 * iqr
            return lower_boundary, upper_boundary
    return None, None


def reformat_dt_str(start_time, dt_format=DATE_FORMAT_QUERY, to_pandas_timestamp=False):
    if not start_time:
        return start_time
    dt = parser.parse(start_time)
    return dt.strftime(dt_format) if not to_pandas_timestamp else pd.Timestamp(dt).tz_localize(None)


def as_list(param):
    if type(param) in [tuple, list, set]:
        return list(param)
    else:
        return [param]


def is_empty(v):
    if not v and v != 0:
        return True
    return False


def detect_file_encoding(file):
    encoding = chardet.detect(file).get('encoding')
    if encoding == ENCODING_ASCII:
        encoding = ENCODING_UTF_8

    if not encoding:
        encoding = detect_encoding(file)

    return encoding


def detect_encoding_from_list(data):
    encoding = None
    encodings = [ENCODING_SHIFT_JIS, ENCODING_ASCII, ENCODING_UTF_8_BOM, ENCODING_UTF_8]

    for ecd in encodings:
        try:
            str_data = data.decode(ecd)
            if str_data:
                encoding = ecd
                return encoding
        except Exception:
            continue

    if encoding is None:
        return locale.getpreferredencoding(False)


def detect_encoding(f_name, read_line: int = 200):
    if isinstance(f_name, IOBase):
        return detect_encoding_stream(f_name, read_line)
    if isinstance(f_name, str):
        return detect_encoding_file_name(f_name, read_line)
    return None


@log_execution_time()
def detect_encoding_stream(file_stream, read_line: int = 10000):
    # current stream position
    current_pos = file_stream.tell()

    if read_line:
        data = functools.reduce(
            lambda x, y: x + y,
            (file_stream.readline() for _ in range(read_line)),
        )
    else:
        data = file_stream.read()

    if isinstance(data, str):  # default is string, zip file is byte
        data = data.encode()

    encoding = chardet.detect(data).get('encoding')
    encoding = check_detected_encoding(encoding, data)

    file_stream.seek(current_pos)
    return encoding


@log_execution_time()
def detect_encoding_file_name(f_name, read_line: int):
    with open_with_zip(f_name, 'rb') as f:
        return detect_encoding_stream(f, read_line)


def check_detected_encoding(encoding, data):
    if encoding:
        try:
            data.decode(encoding)
        except Exception:
            encoding = detect_encoding_from_list(data)
    else:
        encoding = detect_encoding_from_list(data)

    if encoding == ENCODING_ASCII:
        encoding = ENCODING_UTF_8

    return encoding


def replace_str_in_file(file_name, search_str, replace_to_str):
    # get encoding
    encoding = detect_encoding(file_name)
    with open(file_name, encoding=encoding) as f:
        replaced_text = f.read().replace(search_str, replace_to_str)

    with open(file_name, 'w', encoding=encoding) as f_out:
        f_out.write(replaced_text)


def get_file_modify_time(file_path):
    file_time = datetime.utcfromtimestamp(os.path.getmtime(file_path))
    return convert_time(file_time)


def split_path_to_list(file_path):
    folders = os.path.normpath(file_path).split(os.path.sep)
    return folders


def gen_abbr_name(name, len_of_col_name=10):
    suffix = '...'
    short_name = str(name)
    if len(short_name) > len_of_col_name:
        short_name = name[: len_of_col_name - len(suffix)] + suffix

    return short_name


# def remove_inf(series):
#     return series[~series.isin([float('inf'), float('-inf')])]


def read_pickle_file(file):
    with open(file, 'rb') as f:
        pickle_data = pickle.load(f)
    return pickle_data


def write_to_pickle(data, file):
    dir_path = os.path.dirname(file)
    if not check_exist(dir_path):
        make_dir(dir_path)

    with open(file, 'wb') as f:
        pickle.dump(data, f)
    return file


def read_feather_file(file):
    # df = pd.read_feather(file)
    df = feather.read_feather(file)
    return df


def read_parquet_file(file):
    df = parquet.read_parquet(file)
    return df


def convert_int64_to_object(df, cols=None):
    if cols is None:
        cols = df.columns

    for col in cols:
        if df[col].dtype.name in ('Int64', 'int64'):
            df[col] = np.where(pd.isnull(df[col]), None, df[col].astype(str))
    return df


def write_feather_file(df: DataFrame, file):
    make_dir_from_file_path(file)
    # df.reset_index(drop=True, inplace=True)
    # df.to_feather(file, compression='lz4')
    # Use LZ4 explicitly
    if len(file) > 255:
        file = f'{file[:250]}.{FileExtension.Feather.value}'

    try:
        feather.write_feather(df.reset_index(drop=True), file, compression='lz4')
    except Exception as e:
        print(str(e))
        for col in df.columns:
            if df[col].dtype.name in ('object', 'category'):
                df[col] = np.where(pd.isnull(df[col]), None, df[col].astype(str))
                # df[col] = df[col].astype('category') # error in some cases
        feather.write_feather(df.reset_index(drop=True), file, compression='lz4')

    return file


def write_parquet_file(df: DataFrame, file):
    make_dir_from_file_path(file)
    if len(file) > 255:
        file = f'{file[:250]}.{FileExtension.Feather.value}'

    try:
        parquet.to_parquet(df.reset_index(drop=True), file, compression='gzip')
    except Exception as e:
        print(str(e))
        for col in df.columns:
            if df[col].dtype.name in ('object', 'category'):
                df[col] = np.where(pd.isnull(df[col]), None, df[col].astype(str))
                # df[col] = df[col].astype('category') # error in some cases
        parquet.to_parquet(df.reset_index(drop=True), file, compression='gzip')

    return file


def get_debug_g_dict():
    return g.setdefault(FlaskGKey.DEBUG_SHOW_GRAPH, {})


def set_debug_data(func_name, data):
    if not func_name:
        return

    g_debug = get_debug_g_dict()
    g_debug[func_name] = data

    return True


def get_debug_data(key):
    g_debug = get_debug_g_dict()
    data = g_debug.get(key, None)
    return data


@log_execution_time()
def zero_variance(df: DataFrame):
    is_zero_var = False
    err_cols = []
    for col in df.columns:
        if pd.api.types.is_numeric_dtype(df[col]):
            variance = df[col].replace([np.inf, -np.inf], np.nan).var()
            if pd.isna(variance) or variance == 0:
                is_zero_var = True
                err_cols.append(col)
    return is_zero_var, err_cols


@log_execution_time()
def find_babel_locale(lang):
    if not lang:
        return lang

    lang = str(lang).lower()
    lang = lang.replace('-', '_')
    for _lang in LANGUAGES:
        if lang == _lang.lower():
            return _lang

    return lang


def init_sample_config(ignore_tables: list[str] = None):
    file_name = 'sample_config.zip'
    from ap.common.services.import_export_config_and_master_data import clear_db_n_data, import_config_and_master
    from ap.common.services.import_export_config_n_data import get_zip_sample_default_config_path

    clear_db_n_data(is_drop_t_process_tables=True)
    file_path = get_zip_sample_default_config_path(file_name)
    import_config_and_master(file_path, ignore_tables=ignore_tables)
    return True


def init_config(target_file, init_file):
    if not check_exist(target_file) and check_exist(init_file):
        shutil.copyfile(init_file, target_file)
    return True


def convert_nullable_int64_to_numpy_int64(df, column_names):
    for col in column_names:
        if col not in df.columns:
            continue

        # TODO: check why data_factid error ( m_data table)
        try:
            series = df[col].astype(np.object.__name__)
            not_none_idx_s = ~series.isna()
            series[not_none_idx_s] = pd.to_numeric(series[not_none_idx_s], downcast='integer', errors='raise').astype(
                pd.Int64Dtype.name,
            )
            df[col] = series.astype(pd.Int64Dtype.name)
        except Exception:
            pass


def db_model_to_dict(obj):
    cols = [col.name for col in list(obj.__table__.columns)]
    return {col: obj.__dict__.get(col) for col in cols}


def split_grpc_limitation(records):
    max_record_of_chunk = 1
    if records:
        pickle_obj = pickle.dumps(records[0])
        size_of_single_row = len(pickle_obj)
        max_record_of_chunk = GPRC_LIMITATION_SIZE // size_of_single_row
        max_record_of_chunk = int(max_record_of_chunk * 0.8)

    for data_chunk in chunks(records, max_record_of_chunk):
        yield data_chunk


def split_grpc_limitation_for_archived_cycle(records):
    if not records:
        yield records
        return

    max_chunk_size = GPRC_LIMITATION_SIZE * 0.8
    size_chunk = 0
    chunk_vals = []
    for vals in records:
        size_rec = sum([sys.getsizeof(val) for val in vals])
        size_chunk += size_rec
        if size_chunk < max_chunk_size:
            chunk_vals.append(vals)
        else:
            yield chunk_vals
            chunk_vals = [vals]
            size_chunk = size_rec

    if chunk_vals:
        yield chunk_vals


def split_grpc_limitation_old(records, limit_by_number_of_row=None):
    if limit_by_number_of_row:
        for data_chunk in chunks(records, limit_by_number_of_row):
            yield data_chunk
    else:
        size = get_size(records)
        if size > GPRC_LIMITATION_SIZE:
            # get max size of 3 records: first, last, mid
            size_of_single_row = max(get_size(records[0]), get_size(records[-1]), get_size(int(len(records[-1])) / 2))

            # 1.1 -> add 10% buffer
            max_record_of_chunk = int(GPRC_LIMITATION_SIZE // (size_of_single_row * 1.1))
            for data_chunk in chunks(records, max_record_of_chunk):
                yield data_chunk
        else:
            yield records


def split_path_and_file_name(full_file_path):
    path_str = full_file_path.rsplit('\\', 1)  # todo. make common for '//' and '\\'
    path = path_str[0] if path_str else ''
    file_name = path_str[len(path_str) - 1]
    return path, file_name


def get_windows_base_name(file_path, is_extension=True):  # full_file_path must be windows path
    name = split_path_and_file_name(file_path)[-1]
    return name if is_extension else name.split('.')[0]


def get_server_name_and_ip(server_type):
    server_type_text = dict_server_type_text[server_type]
    server_info = f'{server_type_text} ({get_local_ip_address()})'
    return server_info


def get_local_ip_address():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.connect(('8.8.8.8', 1))  # connect() for UDP doesn't send packets
    return s.getsockname()[0]


def rename_df_column(target_cols: List[Tuple[int, str]]):
    dic_outputs = {col_name: gen_sql_label(col_id, col_name) for col_id, col_name in target_cols}
    return dic_outputs


def merge_list_in_list_to_one_list(origin_list: List):
    return chain.from_iterable(origin_list)


def is_int_16(data):
    return (data >= INT16_MIN) & (data <= INT16_MAX)


def is_int_32(data):
    return (data >= INT32_MIN) & (data <= INT32_MAX)  # use & operator, be able apply for dataframe


def is_int_64(data):
    return (data >= INT64_MIN) & (data <= INT64_MAX)  # use | operator, be able apply for dataframe


def is_boolean(data: Series):
    return (data >= 0) & (data <= 1)


def get_index_group_by_column(df, column):
    """
    Count from 0,1,2,... for each group

    :param df:
    :param column:
    :return:
    """
    _temp = df[column].drop_duplicates().reset_index()
    return df.index - df.merge(_temp, how='left')['index']


def convert_nan_to_none(df: DataFrame, convert_to_list: bool = False):
    df = df.convert_dtypes().reset_index(drop=True)
    dict_replace = dict.fromkeys([str(pd.NaT), str(pd.NA), str(np.NAN), EMPTY_STRING, np.inf, -np.inf], None)

    for col in df.columns:
        is_na_series = df[col].isna()
        s: Series = df[~is_na_series][col]
        na_s: Series = df[is_na_series][col]
        if is_numeric_dtype(s):
            s = s.replace({np.NAN: None})
        elif is_string_dtype(s):
            s = s.replace(dict_replace)
        elif is_datetime64_any_dtype(s):
            s = s.replace({pd.NaT: None})
        else:
            s = s.replace(dict_replace)

        na_s = na_s.append(s[s == None])
        df[col] = df[col].astype(np.object.__name__)
        df.loc[na_s.index, col] = None

    if convert_to_list:
        return df.values.tolist()

    return df


def get_none_series(len_series):
    """
    Add None add cast to type Int64
    :param len_series:
    :return:
    """
    return pd.Series([None] * len_series, dtype=pd.Int64Dtype())


def convert_last_import_object(object_data, data_type: str):
    output = None
    if data_type == DataType.INTEGER.name:
        output = int(object_data)
    elif data_type == DataType.REAL.name:
        output = float(object_data)
    elif data_type in [DataType.DATETIME.name, DataType.TEXT.name]:
        output = str(object_data)
    return output


def get_list_attr(list_obj, attr_name):
    _set = {getattr(obj, attr_name) for obj in list_obj if getattr(obj, attr_name)}
    return sorted(_set)


def get_current_utc():
    return datetime.utcnow()


def convert_to_full_name_data_type(cfg_column):
    # value.value ?  don't know why but work (tunghh)
    data_type_db_values = [value.value for name, value in RawDataTypeDB.__members__.items()]
    if cfg_column.data_type in data_type_db_values:
        cfg_column.data_type = RawDataTypeDB(cfg_column.data_type).name


def detect_language(df, column):
    return detect_language_str(''.join(list(df[column].values)))


def detect_language_str(_str):
    if not _str:
        return None
    return detect_langs(_str)


def add_addition_cols(db_instance, df, table_name):
    sql = f'''
 SELECT
    column_name
FROM
    information_schema.columns
WHERE
    table_schema = '{db_instance.schema}'
    AND table_name = '{table_name}'
    '''
    cols, rows = db_instance.run_sql(sql, row_is_dict=False)
    table_columns = merge_list_in_list_to_one_list(rows)
    now = get_current_timestamp()
    addition_cols = ['created_at', 'updated_at']
    vals = [now, now]
    for col, val in zip(addition_cols, vals):
        if col in table_columns and col not in df.columns:
            df[col] = now


def convert_list_file_to_pickle(files):
    result = {}
    zip_path = get_trans_zip_path()

    # TODO: determine file path is v2 type or not, if not -> find same key
    for file in files:
        path = os.path.normpath(file)
        # temp = path.split(zip_path)[1]
        temp = path.split(zip_path)[0]
        path = os.path.normpath(temp)
        file_name = os.path.basename(file)
        key = path.split(f'\\{file_name}')[0]

        if key in result:
            result[key].append(file_name)
        else:
            result[key] = [file_name]
    return codecs.encode(pickle.dumps(result), 'base64').decode()


def convert_pickle_to_list_file(string_binary_from_pickle):
    zip_path = get_trans_zip_path()
    file_dict = pickle.loads(codecs.decode(string_binary_from_pickle.encode(), 'base64'))
    result = []
    for key in file_dict:
        for item in file_dict[key]:
            result.append(f'{zip_path}\\{key}\\{item}')
    return dict.fromkeys(result)


@log_execution_time()
def detect_file_delimiter(file_stream, default_delimiter, encoding=None):
    white_list = [CsvDelimiter.CSV.value, CsvDelimiter.TSV.value, CsvDelimiter.SMC.value]
    candidates = []

    for i in range(200):
        try:
            line = file_stream.readline()
            if isinstance(line, bytes):
                line = line.decode(encoding)
        except StopIteration:
            break

        if line:
            _, row_delimiter = max([(len(line.split(split_char)), split_char) for split_char in white_list])
            candidates.append(row_delimiter)

    if candidates:
        good_delimiter = max(candidates, key=candidates.count)
        if good_delimiter is not None:
            return good_delimiter

    file_stream.seek(0)

    return default_delimiter


def parse_master_table(model_cls, dict_params):
    data_dict = {}
    for _property in dir(model_cls):
        if _property in dict_params:
            value = dict_params[_property]
            if _property == 'outsourcing_flag':
                value = bool(value) if value else False
            data_dict[_property] = value

    return model_cls(**data_dict)


def get_id_columns(column_names: list) -> list:
    """
    Get only column name contain 'id' except transaction columns
    :param column_names: list of column name
    :return: list if id columns
    """
    return [col for col in column_names if re.search(r'^([^t]|[a-z]{2,})_([a-z]+_){0,2}id$', col)]


def get_t_columns(column_names: list) -> list:
    """
    Get only transaction columns
    :param column_names: list of column name
    :return: list if transaction columns
    """
    return [col for col in column_names if re.search(r'^t_[a-z]+_(id|no|name)$', col)]


def convert_data_type_and_remove_na(df: DataFrame) -> DataFrame:
    result = df.convert_dtypes()
    result.replace(dict.fromkeys([pd.NA, np.inf, -np.inf, np.nan, EMPTY_STRING], None), inplace=True)
    return result


def get_common_element(arr1, arr2) -> List:
    return list(set(arr1) & set(arr2))


class NoDataFoundException(Exception):
    def __init__(self):
        super().__init__()
        self.code = 999


def get_recent_elements_in_list(rows, recent_day):
    latest = rows[-1]
    return [row for row in rows if abs((latest[-1] - row[1]).days) <= recent_day]


def should_get_sample_data(process_ids):
    for process_id in process_ids:
        sample_data_path = get_preview_data_file_folder(process_id)
        if not check_exist(sample_data_path) or not get_files(sample_data_path):
            return True

    return False


def get_transaction_data_unknown_master_file_folder(data_table_id: int):
    folder = get_transaction_data_unknown_master_path()
    if not check_exist(folder):
        os.makedirs(folder)
    return os.path.join(folder, str(data_table_id))


def get_preview_data_file_folder(process_id):
    folder = get_preview_data_path()
    if not check_exist(folder):
        os.makedirs(folder)
    return os.path.join(folder, str(process_id))


def delete_preview_data_file_folder(process_id: str | int):
    folder = get_preview_data_path()
    target_folder = os.path.join(folder, str(process_id))
    if not check_exist(target_folder):
        return False
    return shutil.rmtree(target_folder)


def replace_dataframe_symbol(df, cols=None):
    replace_to = EMPTY_STRING
    if cols is None:
        cols = df.columns
    elif isinstance(cols, str):
        cols = [cols]

    symbols = (pd.NA, np.nan, np.inf, -np.inf)
    replace_dict = dict.fromkeys(symbols, replace_to)
    for col in cols:
        try:
            data_type = df[col].dtype.name
            if data_type == 'category':
                for symbol in symbols:
                    if symbol in df[col].cat.categories:
                        if replace_to not in df[col].cat.categories:
                            df[col].cat.add_categories(replace_to, inplace=True)
                        df[col].replace({symbol: replace_to}, inplace=True)
            else:
                df[col].replace(replace_dict, inplace=True)
        except Exception:
            pass


class JobBreakException(Exception):
    def __init__(self):
        super().__init__()
        self.code = 998


def get_sample_files(process_id):
    files = []
    folder_path = get_preview_data_file_folder(process_id)
    if check_exist(folder_path):
        _files = get_files(folder_path, extension=['csv', 'pck'])
        for _file in _files:
            files.append(_file)
    return files


def get_column_order(data_group_type):
    if data_group_type == DataGroupType.DATA_SERIAL.value:
        return 0

    if data_group_type == DataGroupType.AUTO_INCREMENTAL.value:
        return 1

    return 2


def format_df(df: DataFrame) -> DataFrame:
    if df.empty or df.columns.empty:
        return df

    df = df.replace({None: DEFAULT_NONE_VALUE})
    for col in df.columns:
        series: Series = df[col]
        is_na_series: Series[bool] = series.isnull()
        if any(is_na_series):
            non_na_series = series[~is_na_series]
            converted_series = non_na_series.convert_dtypes()
            df[col] = converted_series
        else:
            df[col] = df[col].convert_dtypes()

    return df


def convert_type_base_df(incorrect_df: DataFrame, correct_df: DataFrame, columns: list[str] | pd.Index):
    for column in columns:
        # Cast to float32 if it is float64 to can be compared or join truly data type pd and database
        if np.float64.__name__ in correct_df[column].dtypes.name.lower():
            correct_df[column] = correct_df[column].astype(pd.Float32Dtype.name)

        correct_data_type = correct_df[column].dtypes.name
        correct_data_type_lower = correct_data_type.lower()
        if incorrect_df[column].dtypes.name != correct_data_type:
            series = incorrect_df[column]
            if series.dtypes.name in [object.__name__, pd.StringDtype.name]:
                if np.int.__name__ in correct_data_type_lower or np.float.__name__ in correct_data_type_lower:
                    series = pd.to_numeric(series)
                elif np.datetime64.__name__ in correct_data_type_lower:
                    series = pd.to_datetime(series)
                elif np.timedelta64.__name__ in correct_data_type_lower:
                    series = pd.to_timedelta(series)
                elif np.bool.__name__ in correct_data_type_lower:
                    series = series.replace(
                        dict.fromkeys([1, 'TRUE', 'True', 'true'], True)
                        | dict.fromkeys([0, 'FALSE', 'False', 'false'], False),
                    )

            incorrect_df[column] = series.astype(correct_data_type)


def convert_string_df(df: DataFrame, columns) -> DataFrame:
    for column in columns:
        df[column] = df[column].fillna('').astype('string').str.strip()
    return df


def get_type_all_columns(db_instance, table_name: str):
    sql = f"SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '{table_name}'"
    _, list_dict_rows = db_instance.run_sql(sql, row_is_dict=True)
    return list_dict_rows


def get_month_diff(str_min_datetime: str | datetime, str_max_datetime: str | datetime):
    min_datetime = parser.parse(str_min_datetime) if isinstance(str_min_datetime, str) else str_min_datetime
    max_datetime = parser.parse(str_max_datetime) if isinstance(str_max_datetime, str) else str_max_datetime
    diff = relativedelta(max_datetime, min_datetime)
    return diff.years * 12 + diff.months


def get_nullable_int64_columns(db_instance, table_name: str, list_dict_rows: dict = None):
    if not list_dict_rows:
        list_dict_rows = get_type_all_columns(db_instance, table_name)
    nullable_int64_cols = [
        col_name.get('column_name') for col_name in list_dict_rows if col_name.get('data_type') == 'bigint'
    ]
    return nullable_int64_cols


def camel_to_snake(name, limit_len=None):
    if pd.isnull(name):
        return name
    elif isinstance(name, str):
        name = str(name)

    name = re.sub(r'[^A-Za-z0-9_]', '_', name)
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    s1 = re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()
    if limit_len:
        s1 = s1[:limit_len]
    return s1


def convert_to_d3_format(fmt: str = None):
    """
    :param fmt:'{:d}', '{:,.2f}'
    :return: 'd', ',.2f'
    """
    if not fmt:
        return fmt

    pattern = r'{:([^{}]+)}'
    return re.sub(pattern, r'\1', fmt)


def get_format_padding(column_format: str):
    if not column_format:
        return None

    format_padding = None
    match_pattern = re.match(ZERO_FILL_PATTERN, column_format) or re.match(ZERO_FILL_PATTERN_2, column_format)
    if match_pattern:
        fill_char, symbol, width = match_pattern.groups()
        format_padding = (fill_char, symbol, int(width))

    return format_padding


def add_zero_padding_df(df: DataFrame, column: str, format_padding: Tuple[str, str, int]):
    if format_padding is None:
        return

    if column in df.columns:
        df[column] = add_zero_padding_series(df[column], format_padding)


def add_zero_padding_series(series: Series, format_padding: Tuple[str, str, int]) -> Series:
    if format_padding is None:
        return series

    fill_char, symbol, width = format_padding
    _series = series.replace({None: pd.NA}).dropna().astype(str)
    if symbol == LEFT_Z_TILL_SYMBOL:
        _series = _series.str.ljust(width, fill_char)
    else:
        _series = _series.str.rjust(width, fill_char)
    series = series.astype('object')
    series.update(_series)
    return series


def get_key_from_dict(dictionary: dict, value, default_key=None):
    for _key, _value in dictionary.items():
        if value == _value:
            return _key

    return default_key


def is_subset(self, others):
    self_len = len(self)
    return any(self == other[:self_len] for other in others)


def remove_subset(columns: List[List]):
    new_cols = []
    for idx in range(len(columns)):
        cols = columns[idx]
        compare_cols = columns[:idx] + columns[idx + 1 :]
        if not is_subset(cols, compare_cols):
            new_cols.append(cols)

    return new_cols


def bundle_assets(_app):
    """
    bundle assets when application be started at the first time
    for commnon assets (all page), and single page
    """
    env = os.environ.get(ANALYSIS_INTERFACE_ENV)
    # bundle js files
    assets_path = os.path.join('ap', 'common', 'assets', 'assets.json')
    with open(assets_path, 'r') as f:
        _assets = json.load(f)

    assets = Environment(_app)
    if env != AppEnv.PRODUCTION.value:
        assets.debug = True

    for page in _assets:
        js_assets = _assets[page].get('js') or []
        css_assets = _assets[page].get('css') or []
        js_asset_name = f'js_{page}'
        css_asset_name = f'css_{page}'
        if env != AppEnv.PRODUCTION.value:
            assets.register(js_asset_name, *js_assets)
            assets.register(css_asset_name, *css_assets)
        else:
            js_bundle = Bundle(*js_assets, output=f'common/js/{page}.packed.js')
            css_bundle = Bundle(*css_assets, output=f'common/css/{page}.packed.css')
            assets.register(js_asset_name, js_bundle)
            assets.register(css_asset_name, css_bundle)
            # build assets
            js_bundle.build()
            css_bundle.build()


def open_with_zip(file_name, mode, encoding=None) -> Union[IO[bytes], TextIO]:
    """
    :param file_name:
    :param mode:
    :param encoding: not for zip file
    :return:
    """
    if str(file_name).endswith('.zip'):
        zip_mode = 'w' if 'w' in mode else 'r'
        zf = zipfile.ZipFile(file_name)

        assert len(zf.namelist()) == 1, "We currently don't support multiple zip file"
        return zf.open(zf.namelist()[0], zip_mode)
    else:
        return open(file_name, mode, encoding=encoding)


def get_hostname():
    hostname = socket.gethostname()
    return hostname


@log_execution_time()
def get_ip_address():
    hostname = get_hostname()
    ip_addr = socket.gethostbyname(hostname)

    return ip_addr


def check_client_browser(client_request):
    is_valid_browser = False
    is_valid_version = True
    safari_support_version = str(SAFARI_SUPPORT_VER).split('.')  # >= ver15.4

    request_env = client_request.headers.environ
    http_ch_ua = request_env.get('HTTP_SEC_CH_UA') if request_env else None
    http_user_agent = request_env.get('HTTP_USER_AGENT') if request_env else client_request.user_agent

    # Windows
    if http_ch_ua:
        if 'Google Chrome' in http_ch_ua or 'Microsoft Edge' in http_ch_ua:
            is_valid_browser = True
            is_valid_version = True

        return is_valid_browser, is_valid_version

    # iOS
    if http_user_agent:
        if 'Edg' in http_user_agent:
            is_valid_browser = True

        if 'Safari' in http_user_agent:
            is_valid_browser = True
            user_agents = http_user_agent.split('Version/')
            if len(user_agents) == 1:
                # chrome in ios
                return is_valid_browser, is_valid_version

            [safari_version, _] = user_agents[1].split(' Safari/')
            if safari_version:
                versions = safari_version.split('.')
                v1 = versions[0]
                v2 = 0
                if len(versions) > 1:
                    v2 = versions[1]

                is_valid_version = (int(v1) > int(safari_support_version[0])) or (
                    int(v1) == int(safari_support_version[0]) and int(v2) >= int(safari_support_version[1])
                )

    return is_valid_browser, is_valid_version


def gen_transaction_table_name(proc_id: int):
    return f't_process_{proc_id}'


def gen_data_count_table_name(proc_id: int):
    return f't_data_finder_{proc_id}'


def gen_import_history_table_name(proc_id: int):
    return f't_import_history_{proc_id}'


def gen_bridge_column_name(id, name):
    name = to_romaji(name)
    return f"_{id}_{name.replace('-', '_').lower()}"[:50]


def gen_end_proc_start_end_time(start_tm, end_tm, return_string: bool = True, buffer_days=14):
    end_proc_start_tm = convert_time(
        add_days(convert_time(start_tm, format_str=DATE_FORMAT_STR_POSTGRES, return_string=False), -buffer_days),
        format_str=DATE_FORMAT_STR_POSTGRES,
        return_string=return_string,
    )
    end_proc_end_tm = convert_time(
        add_days(convert_time(end_tm, format_str=DATE_FORMAT_STR_POSTGRES, return_string=False), buffer_days),
        format_str=DATE_FORMAT_STR_POSTGRES,
        return_string=return_string,
    )
    return end_proc_start_tm, end_proc_end_tm


def init_process_queue():
    manager = Manager()
    process_queue = manager.dict()
    process_queue[LOCK] = manager.Lock()
    process_queue[MAPPING_DATA_LOCK] = manager.Lock()
    for notify_type in ListenNotifyType.__members__:
        process_queue[notify_type] = manager.dict()

    write_to_pickle(process_queue, get_multiprocess_queue_file())
    return process_queue


def get_process_queue():
    from ap import dic_config

    process_queue = dic_config.get(PROCESS_QUEUE)
    if process_queue is None:
        process_queue_file = get_multiprocess_queue_file()
        if os.path.exists(process_queue_file):
            try:
                process_queue = read_pickle_file(get_multiprocess_queue_file())
            except Exception as e:
                # in case of old file, renew file
                logger.warning(e)
                process_queue = init_process_queue()
        else:
            process_queue = init_process_queue()

    return process_queue


def get_multiprocess_queue_file():
    data_folder = get_data_path()
    return resource_path(data_folder, PROCESS_QUEUE_FILE_NAME, level=AbsPath.SHOW)


def remove_non_ascii_chars(string, convert_irregular_chars=True):
    # special case for vietnamese: đ letter
    normalized_input = re.sub(r'[đĐ]', 'd', string)

    # pascal case
    normalized_input = normalized_input.title()

    # `[μµ]` in `English Name` should be replaced in to `u`.
    # convert u before kakasi applied to keep u instead of M
    normalized_input = re.sub(r'[μµ]', 'uu', normalized_input)

    # normalize with NFKD
    normalized_input = normalize_str(
        normalized_input,
        convert_irregular_chars=convert_irregular_chars,
        normalize_form=NORMALIZE_FORM_NFKD,
    )

    normalized_input = replace_special_symbols(normalized_input)

    normalized_string = normalized_input.encode(ENCODING_ASCII, 'ignore').decode()
    return normalized_string
