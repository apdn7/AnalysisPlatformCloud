from __future__ import annotations

import math
import os
import re
from collections import defaultdict
from datetime import datetime
from typing import List, Optional

# import pyarrow.feather as feather
from apscheduler.triggers.date import DateTrigger
from pytz import utc

from ap import logger, scheduler
from ap.api.setting_module.services.master_data_job import add_scan_master_job
from ap.common.common_utils import (
    DATE_FORMAT_STR_YYYYMM,
    EXTENSIONS,
    chunks,
    convert_time,
    detect_encoding,
    detect_file_delimiter,
    get_current_timestamp,
    get_file_size,
    get_files,
    get_key_from_dict,
    open_with_zip,
)
from ap.common.constants import ENCODING_UTF_8, NROWS_FOR_SCAN_FILES, CsvDelimiter, DataGroupType, JobType, MasterDBType
from ap.common.pydn.dblib.postgresql import PostgreSQL
from ap.common.scheduler import scheduler_app_context
from ap.common.services.csv_content import get_number_of_reading_lines, read_csv_with_transpose
from ap.common.timezone_utils import gen_dummy_datetime
from ap.setting_module.services.background_process import send_processing_info
from bridge.models.bridge_station import BridgeStationModel
from bridge.models.cfg_data_table import CfgDataTable as BSCfgDataTable
from bridge.models.cfg_data_table_column import CfgDataTableColumn as BSCfgDataTableColumn
from bridge.models.t_csv_management import CsvManagement
from bridge.services.utils import check_missing_column_by_column_name

MISSING_DATA_GROUP_TYPE = {
    DataGroupType.DATA_TIME: '0',
    DataGroupType.PROCESS_NAME: 'P0',
    DataGroupType.LINE_NAME: 'L0',
}
MISSING_VALS = [
    MISSING_DATA_GROUP_TYPE[DataGroupType.DATA_TIME],
    MISSING_DATA_GROUP_TYPE[DataGroupType.PROCESS_NAME],
    MISSING_DATA_GROUP_TYPE[DataGroupType.LINE_NAME],
]
SCAN_MASTER_RATIO = 5


def add_scan_files_job(data_table_id, columns, is_scan_master=False):
    job_name = JobType.SCAN_FILE.name
    job_id = f'{job_name}_{data_table_id}'
    dic_params = {
        '_job_id': job_id,
        '_job_name': job_name,
        'is_scan_master': is_scan_master,
        'data_table_id': data_table_id,
        'columns': columns,
    }
    scheduler.add_job(
        job_id,
        scan_files_job,
        trigger=DateTrigger(run_date=datetime.now().astimezone(utc), timezone=utc),
        replace_existing=True,
        kwargs=dic_params,
    )


@scheduler_app_context
def scan_files_job(_job_id, _job_name, is_scan_master, *args, **kwargs):
    """
    zip v2
    :param _job_id:
    :param _job_name:
    :param is_scan_master:
    :param args:
    :param kwargs:
    :return:
    """
    is_for_test = kwargs.pop('is_for_test', None)

    data_table_id = kwargs.get('data_table_id')
    gen = scan_files(*args, **kwargs)
    send_processing_info(gen, _job_name, data_table_id=data_table_id)

    if is_scan_master:
        with BridgeStationModel.get_db_proxy() as db_instance:
            save_scan_master_target_files(db_instance, data_table_id)

        if is_for_test:
            return

        # scan master
        add_scan_master_job(data_table_id)


@BridgeStationModel.use_db_instance_generator()
def scan_files(
    data_table_id,
    columns,
    datetime_idx=0,
    db_instance: PostgreSQL = None,
    scan_status: Optional[bool] = None,
):
    yield 0
    cfg_data_table: BSCfgDataTable = BSCfgDataTable.get_by_id(db_instance, data_table_id, is_cascade=True)
    skip_head = cfg_data_table.data_source.csv_detail.skip_head
    n_rows = cfg_data_table.data_source.csv_detail.n_rows
    is_transpose = cfg_data_table.data_source.csv_detail.is_transpose
    dummy_header = cfg_data_table.data_source.csv_detail.dummy_header
    master_type = cfg_data_table.get_master_type()
    detail_master_type = cfg_data_table.detail_master_type
    root_folder = cfg_data_table.data_source.csv_detail.directory
    if detail_master_type == MasterDBType.V2_HISTORY.name:
        root_folder = cfg_data_table.data_source.csv_detail.second_directory
    root_folder = os.path.abspath(root_folder)

    all_files = (
        [root_folder]
        if cfg_data_table.data_source.csv_detail.is_file_path
        else get_files(root_folder, depth_from=1, depth_to=100, extension=EXTENSIONS)
    )
    root_folder = (
        os.path.dirname(os.path.abspath(root_folder))
        if cfg_data_table.data_source.csv_detail.is_file_path
        else root_folder
    )

    _, rows = CsvManagement.select_records(
        db_instance,
        dic_conditions={CsvManagement.Columns.data_table_id.name: data_table_id},
        select_cols=[CsvManagement.Columns.file_name.name],
        row_is_dict=False,
    )

    defined_cols = [table_column.column_name for table_column in cfg_data_table.columns]
    exist_files = {''.join([root_folder, row[0]]) for row in rows}

    files = list(set(all_files) - set(exist_files))
    if not files:
        yield 100
        return

    # columns = ['登録日時', '工程名', 'ライン名']
    encoding, sep = None, None
    percent_per_file = 100 / len(files)
    insert_records = []
    root_folder_len = len(root_folder)
    for idx, file_path in enumerate(files, start=1):
        created_at = get_current_timestamp()
        if not encoding or not sep:
            encoding, sep = get_delimiter_n_encoding(file_path)

        try:
            data, is_missing_column = read_file(
                file_path,
                columns,
                datetime_idx,
                MISSING_VALS,
                master_type,
                defined_cols,
                encoding,
                sep=sep,
                skip_head=skip_head,
                n_rows=n_rows,
                is_transpose=is_transpose,
                dummy_header=dummy_header,
            )
        except Exception:
            try:
                encoding, sep = get_delimiter_n_encoding(file_path)
                data, is_missing_column = read_file(
                    file_path,
                    columns,
                    datetime_idx,
                    MISSING_VALS,
                    master_type,
                    defined_cols,
                    encoding,
                    sep=sep,
                    skip_head=skip_head,
                    n_rows=n_rows,
                    is_transpose=is_transpose,
                    dummy_header=dummy_header,
                )
            except Exception as e:
                logger.error(e)
                continue

        # there is no data
        if not data or is_missing_column:
            continue

        # fill default value if missing (line info or process info)
        if len(data) != len(MISSING_VALS):
            group_types: List[DataGroupType] = BSCfgDataTableColumn.get_split_columns(db_instance, data_table_id)
            col_data_group_dic = BSCfgDataTableColumn.get_data_group_types_by_column_names(
                db_instance,
                data_table_id,
                columns,
            )
            raw_val_dic = dict(zip(columns, data))
            data = []
            for group_type in group_types:
                if group_type in col_data_group_dic.values():
                    val = raw_val_dic[get_key_from_dict(col_data_group_dic, group_type)]
                else:
                    val = MISSING_DATA_GROUP_TYPE[group_type]
                data.append(val)

        data = [remove_word_in_folder(val) for val in data]
        if data != MISSING_VALS:
            file_size = get_file_size(file_path)
            # # TODO: temporary add data_process & data_line
            insert_records.append(
                (
                    file_path[root_folder_len:],
                    cfg_data_table.id,
                    *data,
                    sep,
                    encoding,
                    file_size,
                    created_at,
                ),
            )

        percent = int(math.floor(idx * percent_per_file))
        yield percent

    if insert_records:
        insert_columns = [
            CsvManagement.Columns.file_name.name,
            CsvManagement.Columns.data_table_id.name,
            CsvManagement.Columns.data_time.name,
            CsvManagement.Columns.data_process.name,
            CsvManagement.Columns.data_line.name,
            CsvManagement.Columns.data_delimiter.name,
            CsvManagement.Columns.data_encoding.name,
            CsvManagement.Columns.data_size.name,
            CsvManagement.Columns.created_at.name,
        ]

        if scan_status is not None:
            insert_columns.append(CsvManagement.Columns.scan_status.name)
            insert_records = [tuple([*pair_values] + [scan_status]) for pair_values in insert_records]

        db_instance.bulk_insert(CsvManagement.get_original_table_name(), insert_columns, insert_records)

    print('Done')
    yield 100


# -------------------------------- #
def read_file(
    target_file,
    columns,
    datetime_idx,
    missing_vals,
    master_type: str,
    defined_cols: list[str],
    encoding=None,
    sep=',',
    skip_head: int = 0,
    n_rows: int | None = None,
    is_transpose: bool = False,
    dummy_header: bool = False,
):
    vals = []
    is_missing_column = False
    params = {
        'nrows': get_number_of_reading_lines(n_rows, NROWS_FOR_SCAN_FILES),
        'encoding': encoding or ENCODING_UTF_8,
        'sep': sep,
        'skiprows': skip_head,
    }

    if dummy_header:
        # use defined_cols because csv file does not have header columns
        params.update({'names': defined_cols, 'header': None})

    df = read_csv_with_transpose(target_file, is_transpose=is_transpose, **params)
    if master_type != MasterDBType.OTHERS.name:
        is_missing_column = check_missing_column_by_column_name(
            master_type,
            defined_cols,
            df.columns.to_list(),
        )
    if is_missing_column:
        logger.info(f'[FILE MISSING COLUMN]: {target_file}')
    if not len(df):
        return None, is_missing_column

    if columns[datetime_idx] not in df.columns:
        df = gen_dummy_datetime(df)

    for idx, col in enumerate(columns):
        val = missing_vals[idx]
        if col in df:
            series = df[col].dropna()
            if len(series):
                val = series.iloc[0]
                if idx == datetime_idx:
                    try:
                        val = convert_time(val, format_str=DATE_FORMAT_STR_YYYYMM)
                        val = f'{val[2:4]}{val[4:6]}'
                    except Exception:
                        val = missing_vals[idx]

        vals.append(val)

    return vals, is_missing_column


def remove_word_in_folder(folder_name):
    return re.sub(r'[\\/:*?"<>|]', '', folder_name)


def save_scan_master_target_files(db_instance, data_table_id):
    _, rows = CsvManagement.get_import_target_files(db_instance, data_table_id)
    cfg_data_table: BSCfgDataTable = BSCfgDataTable.get_by_id(db_instance, data_table_id)
    dic_records = defaultdict(list)
    for dic_row in rows:
        rec = CsvManagement(dic_row)
        dic_records[(rec.data_time, rec.data_process, rec.data_line)].append(rec)

    scan_target_ids = []
    for chunk_recs in dic_records.values():
        chunk_len = len(chunk_recs)
        if chunk_len < 10 or not cfg_data_table.is_has_auto_increment_col():
            chunk_to_jump = 1
        else:
            target_jump_count = SCAN_MASTER_RATIO * chunk_len / 100
            chunk_to_jump = math.ceil(chunk_len / target_jump_count)

        for chunk in chunks(chunk_recs, chunk_to_jump):
            target_rec: CsvManagement = chunk[0]
            if target_rec.scan_status:
                continue

            scan_target_ids.append(target_rec.id)

    CsvManagement.save_scan_master_target(db_instance, scan_target_ids, False)

    return True


def get_delimiter_n_encoding(file_path):
    with open_with_zip(file_path, 'rb') as file_stream:
        encoding = detect_encoding(file_stream)
        sep = detect_file_delimiter(file_stream, CsvDelimiter.CSV.value, encoding)

    return encoding, sep
