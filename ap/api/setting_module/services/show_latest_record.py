from __future__ import annotations

import os
import re
import time
from contextlib import suppress
from itertools import islice
from typing import List

import pandas as pd
from flask_babel import get_locale
from pandas import DataFrame

from ap.api.efa.services.etl import detect_file_path_delimiter, preview_data
from ap.api.setting_module.services.common import get_datetime_val
from ap.api.setting_module.services.data_import import (
    convert_df_col_to_utc,
    convert_df_datetime_to_str,
    strip_special_symbol,
    validate_datetime,
)
from ap.common.common_utils import (
    get_csv_delimiter,
    get_sorted_files,
    get_sorted_files_by_size_and_time,
    remove_non_ascii_chars,
)
from ap.common.constants import (
    COLUMN_CONVERSION,
    DATETIME_DUMMY,
    EMPTY_STRING,
    LATEST_RECORDS_SQL_LIMIT,
    MAX_VALUE_INT,
    NUM_CHARS_THRESHOLD,
    PREVIEW_DATA_RECORDS,
    PREVIEW_DATA_TIMEOUT,
    WR_HEADER_NAMES,
    WR_TYPES,
    WR_VALUES,
    DataGroupType,
    DataType,
    DBType,
    MasterDBType,
    RelationShip,
)
from ap.common.logger import log_execution_time, logger
from ap.common.memoize import memoize
from ap.common.pydn.dblib.db_common import db_instance_exec
from ap.common.pydn.dblib.db_proxy_readonly import ReadOnlyDbProxy
from ap.common.pydn.dblib.postgresql import PostgreSQL
from ap.common.services import csv_header_wrapr as chw
from ap.common.services.csv_content import (
    check_exception_case,
    get_delimiter_encoding,
    is_normal_csv,
    read_data,
)
from ap.common.services.data_type import gen_data_types
from ap.common.services.jp_to_romaji_utils import change_duplicated_columns, to_romaji
from ap.common.services.normalization import (
    normalize_big_rows,
    normalize_list,
)
from ap.common.timezone_utils import (
    gen_dummy_datetime,
    get_time_info,
)
from ap.setting_module.models import (
    CfgDataSource,
    CfgProcess,
    CfgVisualization,
    crud_config,
    make_session,
)
from ap.setting_module.schemas import VisualizationSchema
from ap.setting_module.services.process_config import get_process_cfg
from bridge.models.bridge_station import BridgeStationModel
from bridge.models.cfg_data_table import CfgDataTable as BSCfgDataTable
from bridge.models.cfg_process import CfgProcess as BSCfgProcess
from bridge.models.cfg_process_column import CfgProcessColumn as BSCfgProcessColumn
from bridge.models.cfg_process_function_column import CfgProcessFunctionColumn as BSCfgProcessFunctionColumn
from bridge.models.mapping_factory_machine import MappingFactoryMachine as BSMappingFactoryMachine
from bridge.models.r_factory_machine import RFactoryMachine as BSRFactoryMachine
from bridge.services.data_import import get_preview_data_files
from bridge.services.etl_services.etl_software_workshop_services import SoftwareWorkshopService
from bridge.services.utils import get_master_type, get_well_known_columns


@BridgeStationModel.use_db_instance()
def get_process_latest_records(process_id, limit=PREVIEW_DATA_RECORDS, db_instance: PostgreSQL = None):
    file_name = None
    rows = []
    # for data_table in data_tables:
    cfg_process: BSCfgProcess = BSCfgProcess.get_by_id(db_instance, process_id, is_cascade=True)
    cols_with_types = []
    is_has_dummy_datetime_col = False
    for col in cfg_process.columns:
        if col.column_name == DATETIME_DUMMY:
            is_has_dummy_datetime_col = True

        cols_with_types.append(
            {
                'name': col.column_name,
                'data_type': col.raw_data_type,
                'romaji': col.column_name,
                'is_get_date': col.is_get_date,
                'is_auto_increment': col.is_auto_increment,
            },
        )
    preview_data_file_names = get_preview_data_files(process_id)
    if preview_data_file_names:
        file_name = preview_data_file_names[-1]
        try:
            # df = read_feather_file(file_name)
            df = pd.read_csv(file_name, dtype=str)
            df = df.fillna('')
            if is_has_dummy_datetime_col:
                df = gen_dummy_datetime(df)
            # if DataGroupType.DATA_TIME.name in df.columns:
            #     df[DataGroupType.DATA_TIME.name] = pd.to_datetime(df[DataGroupType.DATA_TIME.name], errors='coerce')
            df = df.astype(str)
            rows = transform_df_to_rows(df.columns, df, limit)
        except Exception as e:
            # TODO : check error
            print(e)

    return cols_with_types, rows, False, file_name


@BridgeStationModel.use_db_instance()
def check_is_csv(cfg_process: dict, db_instance: PostgreSQL):
    rows = BSRFactoryMachine.get_data_table_id_with_process_id(db_instance, cfg_process.get('id'))
    cfg_process['is_csv'] = False
    for row in rows:
        data_table_id = row.get(BSMappingFactoryMachine.Columns.data_table_id.name)
        cfg_data_table: BSCfgDataTable = BSCfgDataTable.get_by_id(db_instance, data_table_id, is_cascade=True)
        if cfg_data_table.data_source.type == DBType.CSV.name:
            cfg_process['is_csv'] = True
            break
    return cfg_process


def get_process_config_info(proc_id, db_instance: PostgreSQL = None) -> dict | None:
    process = get_process_cfg(proc_id, db_instance=db_instance)
    check_is_csv(process, db_instance=db_instance)

    if process:
        tables = ['temp']
        columns = process.get('columns', [])

        def cmp_key(item) -> int:
            val = item.get('order')
            return val if val is not None else MAX_VALUE_INT

        columns.sort(key=lambda item: cmp_key(item))

        master_col_types = DataGroupType.not_master_data_column()
        if process.get(BSCfgProcess.Columns.is_show_file_name.name) is not None:
            master_col_types.remove(DataGroupType.DATA_TIME.value)
            master_col_types.remove(DataGroupType.MAIN_DATE.value)
            master_col_types.remove(DataGroupType.MAIN_TIME.value)

        hide_column_values = DataGroupType.get_hide_column_type_cfg_proces_columns()
        for column in columns:
            if not column[BSCfgProcessColumn.Columns.raw_data_type.name]:
                column[BSCfgProcessColumn.Columns.raw_data_type.name] = column[
                    BSCfgProcessColumn.Columns.data_type.name
                ]
            column['is_show'] = column.get(
                BSCfgProcessColumn.Columns.column_type.name,
            ) not in hide_column_values and not (
                column.get('is_function_column')
                and not (column.get('is_me_function_column') and column.get('is_category_function_column'))
            )
            column['is_master_col'] = column.get(
                BSCfgProcessColumn.Columns.column_type.name,
            ) not in master_col_types and not len(column.get('function_details'))
            column['is_checked'] = True

        # ignore function columns
        show_columns = list(
            filter(
                lambda item: (
                    item.get(BSCfgProcessColumn.Columns.column_type.name) not in hide_column_values
                    and not (
                        column.get('is_function_column')
                        and not (column.get('is_me_function_column') and column.get('is_category_function_column'))
                    )
                ),
                columns,
            ),
        )
        process['columns'] = show_columns

        latest_rec = get_process_latest_records(proc_id, db_instance=db_instance)
        if not latest_rec:
            return None

        cols_with_types, rows, cols_duplicated, previewed_files = latest_rec
        col_id_in_funcs = BSCfgProcessFunctionColumn.get_all_cfg_col_ids(db_instance=db_instance)
        return {
            'status': 200,
            'data': process,
            'col': cols_with_types,
            'cols_duplicated': cols_duplicated,
            'rows': rows,
            'tables': tables,
            'col_id_in_funcs': col_id_in_funcs,
        }


def get_latest_records(data_source, table_name, file_name=None, limit=5, detail_master_type=''):
    blank_output = {
        'cols': [],
        'rows': [],
        'cols_duplicated': [],
        'fail_limit': None,
        'has_ct_col': None,
        'dummy_datetime_idx': None,
    }

    if not data_source or not data_source.id:
        return blank_output

    master_type = data_source.master_type

    previewed_files = None
    cols_with_types = []
    is_csv_or_v2 = data_source.type.lower() in [DBType.CSV.name.lower(), DBType.V2.name.lower()]
    if is_csv_or_v2:
        csv_detail = data_source.csv_detail
        directory = csv_detail.directory
        if detail_master_type == MasterDBType.V2_HISTORY.name:
            directory = csv_detail.second_directory
        file_name = directory if csv_detail.is_file_path else None

        n_rows = csv_detail.n_rows
        is_transpose = csv_detail.is_transpose

        # Get raw column names in file that have not been normalized yet
        line_skip = csv_detail.skip_head
        dic_preview = preview_csv_data(
            directory,
            csv_detail.etl_func,
            csv_detail.delimiter,
            limit,
            return_df=True,
            max_records=1000,
            file_name=file_name,
            # TODO: v2 does not use line_skip??
            line_skip=line_skip,
            n_rows=n_rows,
            is_transpose=is_transpose,
            is_skip_normalizing_header=True,
        )
        pd_names = []
        if data_source.master_type != MasterDBType.V2.name:
            pd_names = dic_preview.get('pd_names')

        column_raw_name = dic_preview.get('org_headers')
        headers = dic_preview.get('header')

        # Do not normalize header, show raw column name
        # normalized_headers = headers
        # normalized_headers = map(normalize_str, normalized_headers)
        # normalized_headers = dict(zip(normalized_headers, headers))

        master_type = get_master_type(master_type, column_names=normalize_list(column_raw_name))
        data_types = dic_preview.get('dataType')
        same_values = dic_preview.get('same_values')
        df_rows = dic_preview.get('content', None)

        # sort columns
        cols = headers
        cols = remove_not_known_cols(master_type, df_rows, cols)
        if headers and data_types:
            cols_with_types = gen_cols_with_types(
                headers,
                data_types,
                same_values,
                white_list_cols=cols,
                pd_names=pd_names,
            )

        # get rows
        previewed_files = dic_preview.get('previewed_files')
    else:
        master_type = get_master_type(master_type, table_name=table_name)
        cols, df_rows = get_info_from_db(data_source, table_name)
        cols = remove_not_known_cols(master_type, df_rows, cols)
        data_types = [gen_data_types(df_rows[col]) for col in cols]
        same_values = check_same_values_in_df(df_rows, cols)
        if cols and data_types:
            cols_with_types = gen_cols_with_types(cols, data_types, same_values)
        # format data
        df_rows = convert_utc_df(df_rows, cols, data_types, data_source, table_name)

    # change name if romaji cols is duplicated
    cols_with_types, cols_duplicated = change_duplicated_columns(cols_with_types)

    # Rename column
    # copy code todo refactor
    conversion = {key: value for key, value in COLUMN_CONVERSION.items() if key in df_rows.columns}
    if conversion:
        logger.info(f'Rename column: {conversion}')
        df_rows.rename(columns=conversion, inplace=True)
        for dict_record in cols_with_types:
            if dict_record['name'] in COLUMN_CONVERSION:
                dict_record['name'] = COLUMN_CONVERSION[dict_record['name']]
        for i in range(len(cols)):
            if cols[i] in COLUMN_CONVERSION:
                cols[i] = COLUMN_CONVERSION[cols[i]]

    determine_candidate_date_serial_column(df_rows, master_type, cols_with_types)

    # transform to rows
    df_rows = df_rows.astype('string').fillna(EMPTY_STRING)
    rows = transform_df_to_rows(cols, df_rows, limit, is_skip_normalizing_header=True)

    is_rdb = not is_csv_or_v2
    return cols_with_types, rows, cols_duplicated, previewed_files, master_type, is_rdb


def determine_candidate_date_serial_column(df_rows: DataFrame, master_type: str, cols_with_types: list):
    # Determine date column & Serial column in (CHECK_DATE1~10, LOT_NO1~10)
    well_known_columns = get_well_known_columns(master_type, None)
    if well_known_columns and master_type in [MasterDBType.EFA.name]:
        __LOW_PRIORITY_DATE_COL__ = 'SET_DATE'
        __LOW_PRIORITY_SERIAL_COL__ = 'LOT_NO'
        date_col_weight_points = []
        serial_col_weight_points = []
        for record in cols_with_types:
            col_name = record['name']
            if (
                col_name in well_known_columns
                and col_name not in [__LOW_PRIORITY_DATE_COL__, __LOW_PRIORITY_SERIAL_COL__]
                and well_known_columns[col_name] in [DataGroupType.DATA_TIME.value, DataGroupType.DATA_SERIAL.value]
            ):
                if record.get('check_same_value', {}).get('is_null', False):
                    weight_point = 0
                else:
                    if well_known_columns[col_name] == DataGroupType.DATA_TIME.value:
                        # 14-digit integers that can be converted to date and time data in "CHECK_DATE1~10" column
                        match_condition_series = df_rows[col_name].str.isdigit() & (df_rows[col_name].str.len() == 14)
                    elif well_known_columns[col_name] == DataGroupType.DATA_SERIAL.value:
                        match_condition_series = df_rows[col_name].notnull()
                    weight_point = match_condition_series.sum()

                if well_known_columns[col_name] == DataGroupType.DATA_TIME.value:
                    date_col_weight_points.append((col_name, weight_point))
                elif well_known_columns[col_name] == DataGroupType.DATA_SERIAL.value:
                    serial_col_weight_points.append((col_name, weight_point))

        date_col_max_weight_point = max(date_col_weight_points, key=lambda tup: tup[1])
        if date_col_max_weight_point[1] == 0:
            candidate_datatime_col = __LOW_PRIORITY_DATE_COL__
        else:
            candidate_datatime_col = date_col_max_weight_point[0]

        serial_col_max_weight_point = max(serial_col_weight_points, key=lambda tup: tup[1])
        if serial_col_max_weight_point[1] == 0:
            candidate_serial_col = __LOW_PRIORITY_SERIAL_COL__
        else:
            candidate_serial_col = serial_col_max_weight_point[0]

        # Add flag to candidate columns
        for record in cols_with_types:
            if record['name'] in [candidate_datatime_col, candidate_serial_col]:
                record['is_candidate_col'] = True
            elif well_known_columns[record['name']] in [
                DataGroupType.DATA_TIME.value,
                DataGroupType.DATA_SERIAL.value,
            ]:
                record['is_candidate_col'] = False


def remove_not_known_cols(master_type: str, df_rows: pd.DataFrame, cols: list[str]):
    well_known_columns = get_well_known_columns(master_type, cols)
    if well_known_columns and master_type != MasterDBType.OTHERS.name:
        cols = [col for col in cols if col in well_known_columns]
        hide_columns = [col for col in df_rows.columns if col not in well_known_columns]
        if hide_columns:
            logger.info(f'Hide column: {hide_columns}')
            df_rows.drop(columns=hide_columns, inplace=True)

    return cols


def get_info_from_db(data_source, table_name, sql_limit: int = 2000):
    if data_source.master_type == MasterDBType.SOFTWARE_WORKSHOP.name:
        return SoftwareWorkshopService.get_info_from_db(
            data_source.id,
            table_name,
            child_equip_id=None,
            sql_limit=sql_limit,
            is_transform_horizontal=False,
        )
    return get_info_from_db_normal(data_source.id, table_name, sql_limit)


@memoize(duration=300)
def get_info_from_db_normal(data_source_id, table_name, sql_limit: int = 2000):
    data_source = CfgDataSource.query.get(data_source_id)
    with ReadOnlyDbProxy(data_source) as db_instance:
        if not db_instance or not table_name:
            return [], []

        cols, rows = db_instance_exec(db_instance, from_table=table_name, limit=LATEST_RECORDS_SQL_LIMIT)

    # cols = normalize_list(cols)
    df_rows = normalize_big_rows(rows, cols, strip_quote=False)
    return cols, df_rows


def get_exist_data_partition(data_source: CfgDataSource, partition_tables: list):
    """
    Get table name that have data

    :param data_source:
    :param partition_tables:
    :return:
    """

    with ReadOnlyDbProxy(data_source) as db_instance:
        exist_data_table = None
        if not db_instance or not partition_tables:
            return exist_data_table

        for table_name in sorted(partition_tables, reverse=True):
            cols, rows = db_instance_exec(db_instance, select=1, from_table=table_name, limit=LATEST_RECORDS_SQL_LIMIT)
            if len(rows) > 0:
                exist_data_table = table_name
                break

    return exist_data_table


def save_master_vis_config(proc_id, cfg_jsons):
    vis_schema = VisualizationSchema()

    with make_session() as meta_session:
        proc: CfgProcess = meta_session.query(CfgProcess).get(proc_id or -1)
        if proc:
            cfg_vis_data = []
            for cfg_json in cfg_jsons:
                cfg_vis_data.append(vis_schema.load(cfg_json))
            crud_config(
                meta_session=meta_session,
                data=cfg_vis_data,
                model=CfgVisualization,
                key_names=CfgVisualization.id.key,
                parent_key_names=CfgVisualization.process_id.key,
                parent_obj=proc,
                parent_relation_key=CfgProcess.visualizations.key,
                parent_relation_type=RelationShip.MANY,
            )


@log_execution_time()
def get_csv_data_from_files(
    sorted_files,
    line_skip: int | None,
    n_rows: int,
    is_transpose: bool,
    etl_func,
    csv_delimiter,
    max_records=5,
):
    skip_head = 0 if not line_skip else int(line_skip)
    csv_file = sorted_files[0]
    skip_tail = 0
    encoding = None
    skip_head_detected = None

    # call efa etl
    has_data_file = None
    if etl_func:
        # try to get file which has data to detect data types + get col names
        for file_path in sorted_files:
            preview_file_path = preview_data(file_path)
            if preview_file_path and not isinstance(preview_file_path, Exception):
                has_data_file = True
                csv_file = preview_file_path
                csv_delimiter = detect_file_path_delimiter(csv_file, csv_delimiter)
                break

        if has_data_file:
            for i in range(2):
                data = None
                try:
                    data = read_data(
                        csv_file,
                        skip_head=line_skip,
                        n_rows=n_rows,
                        is_transpose=is_transpose,
                        delimiter=csv_delimiter,
                        do_normalize=False,
                    )
                    header_names = next(data)

                    # strip special symbols
                    if i == 0:
                        data = strip_special_symbol(data)

                    # get 5 rows
                    data_details = list(islice(data, max_records))
                finally:
                    if data:
                        data.close()

                if data_details:
                    break
    elif is_normal_csv(csv_file, csv_delimiter, skip_head=skip_head, n_rows=n_rows, is_transpose=is_transpose):
        header_names, data_details, encoding = retrieve_data_from_several_files(
            None,
            csv_delimiter,
            max_records,
            csv_file,
            skip_head=skip_head,
            n_rows=n_rows,
            is_transpose=is_transpose,
        )
    else:
        # try to get file which has data to detect data types + get col names
        dic_file_info, csv_file = get_etl_good_file(sorted_files)
        if dic_file_info and csv_file:
            skip_head = chw.get_skip_head(dic_file_info)
            skip_head_detected = skip_head
            skip_tail = chw.get_skip_tail(dic_file_info)
            header_names = chw.get_columns_name(dic_file_info)
            etl_headers = chw.get_etl_headers(dic_file_info)
            data_types = chw.get_data_type(dic_file_info)
            for i in range(2):
                data = None
                try:
                    data = read_data(
                        csv_file,
                        headers=header_names,
                        skip_head=skip_head,
                        delimiter=csv_delimiter,
                        do_normalize=False,
                    )
                    # non-use header
                    next(data)

                    # strip special symbols
                    if i == 0:
                        data = strip_special_symbol(data)

                    # get 5 rows
                    get_limit = max_records + skip_tail if max_records else None
                    data_details = list(islice(data, get_limit))
                    data_details = data_details[: len(data_details) - skip_tail]
                finally:
                    if data:
                        data.close()

                if data_details:
                    break

            # Merge heads with Machine, Line, Process
            if etl_headers[WR_VALUES]:
                header_names += etl_headers[WR_HEADER_NAMES]
                data_types += etl_headers[WR_TYPES]
                data_details = chw.merge_etl_heads(etl_headers[WR_VALUES], data_details)

        else:
            raise ValueError('Cannot get headers_name and data_details')

    # generate column name if there is not header in file
    org_header, header_names, dummy_header, partial_dummy_header, data_details = gen_dummy_header(
        header_names,
        data_details,
        line_skip,
    )

    skip_head = skip_head_detected if skip_head_detected else line_skip
    return org_header, header_names, dummy_header, partial_dummy_header, data_details, encoding, skip_tail, skip_head


@log_execution_time()
def preview_csv_data(
    folder_url,
    etl_func,
    csv_delimiter,
    limit,
    return_df=False,
    max_records=5,
    file_name=None,
    line_skip=None,
    n_rows: int | None = None,
    is_transpose: bool = False,
    is_convert_date_time=False,
    is_skip_normalizing_header: bool = False,
):
    csv_delimiter = get_csv_delimiter(csv_delimiter)

    if not file_name:
        sorted_files = get_sorted_files(folder_url)
        sorted_files = sorted_files[0:5]
    else:
        sorted_files = [file_name]

    csv_file = ''
    header_names = []
    data_types = []
    data_details = []
    same_values = []
    if not sorted_files:
        return {
            'directory': folder_url,
            'file_name': csv_file,
            'header': header_names,
            'content': [] if return_df else data_details,
            'dataType': data_types,
            'skip_head': line_skip,
            'n_rows': n_rows,
            'is_transpose': is_transpose,
            'skip_tail': 0,
            'previewed_files': sorted_files,
            'same_values': same_values,
        }

    csv_file = sorted_files[0]

    (
        org_header,
        header_names,
        dummy_header,
        partial_dummy_header,
        data_details,
        encoding,
        skip_tail,
        skip_head_detected,
    ) = get_csv_data_from_files(
        sorted_files,
        line_skip,
        n_rows,
        is_transpose,
        etl_func,
        csv_delimiter,
        max_records,
    )

    # normalize data detail
    df_data_details, org_headers, header_names, dupl_cols, data_types, pd_names = extract_data_detail(
        header_names,
        data_details,
        org_header,
        is_skip_normalizing_header,
    )
    has_ct_col = True
    dummy_datetime_idx = None
    if df_data_details is not None:
        # convert utc
        for col, dtype in zip(header_names, data_types):
            if DataType(dtype) is not DataType.DATETIME:
                continue
            # Convert UTC time
            if is_convert_date_time:
                validate_datetime(df_data_details, col, False, False)
                convert_csv_timezone(df_data_details, col)

            df_data_details.dropna(subset=[col], inplace=True)

        df_data_details = df_data_details[0:limit]
        same_values = check_same_values_in_df(df_data_details, header_names)

        if not return_df:
            df_data_details = df_data_details.to_records(index=False).tolist()
    elif not return_df:
        df_data_details = []

    if csv_file:
        csv_file = csv_file.replace('/', os.sep)

    has_dupl_cols = False
    if len(dupl_cols) and same_values:
        if dummy_datetime_idx is not None:
            # for dummy datetime column
            dupl_cols = [False] + dupl_cols
        for key, value in enumerate(same_values):
            is_dupl_col = bool(dupl_cols[key])
            same_values[key]['is_dupl'] = is_dupl_col
            if is_dupl_col:
                has_dupl_cols = True

    return {
        'directory': folder_url,
        'file_name': csv_file,
        'header': header_names,
        'content': df_data_details,
        'dataType': data_types,
        'skip_head': 0 if dummy_header and not skip_head_detected else skip_head_detected,
        'skip_tail': skip_tail,
        'n_rows': n_rows,
        'is_transpose': is_transpose,
        'previewed_files': sorted_files,
        'has_ct_col': has_ct_col,
        'dummy_datetime_idx': dummy_datetime_idx,
        'same_values': [{key: bool(value) for key, value in same_value.items()} for same_value in same_values],
        'has_dupl_cols': False if dummy_header else has_dupl_cols,
        'org_headers': org_headers,
        'encoding': encoding,
        'pd_names': pd_names,
        'is_dummy_header': dummy_header,
        'partial_dummy_header': partial_dummy_header,
    }


@log_execution_time()
def convert_csv_timezone(df, get_date_col):
    datetime_val = get_datetime_val(df[get_date_col])
    is_timezone_inside, csv_timezone, utc_offset = get_time_info(datetime_val, None)
    df[get_date_col] = convert_df_col_to_utc(df, get_date_col, is_timezone_inside, csv_timezone, utc_offset)
    df[get_date_col] = convert_df_datetime_to_str(df, get_date_col)


def check_same_values_in_df(df, cols):
    same_values = []
    len_df = len(df)
    for col in cols:
        is_null = False
        null_count = df[col].isnull().sum()
        if null_count == len_df:
            is_null = True
        else:
            null_count = (df[col].astype(str) == EMPTY_STRING).sum()
            if null_count == len_df:
                is_null = True

        is_same = df[col].nunique() == 1

        same_values.append({'is_null': is_null, 'is_same': is_same})

    return same_values


@log_execution_time()
def get_etl_good_file(sorted_files):
    csv_file = None
    dic_file_info = None
    try:
        for file_path in sorted_files:
            check_result = chw.get_file_info_py(file_path)
            if isinstance(check_result, Exception):
                continue

            dic_file_info, is_empty_file = check_result

            if dic_file_info is None or isinstance(dic_file_info, Exception):
                continue

            if is_empty_file:
                continue

            csv_file = file_path
            break
    except IndexError as e:
        logger.exception(e)
    return dic_file_info, csv_file


@log_execution_time()
def gen_cols_with_types(cols, data_types, same_values, white_list_cols=None, pd_names=None, column_raw_name=[]):
    ja_locale = False
    cols_with_types = []
    with suppress(Exception):
        ja_locale = get_locale().language == 'ja'
    has_is_get_date_col = False
    if not column_raw_name:
        column_raw_name = cols.copy()
    if not pd_names:
        pd_names = cols.copy()

    for col_name, col_raw_name, data_type, same_value, pd_name in zip(
        cols,
        column_raw_name,
        data_types,
        same_values,
        pd_names,
    ):
        is_date = False if has_is_get_date_col else DataType(data_type) is DataType.DATETIME
        if is_date:
            has_is_get_date_col = True

        if not col_name:
            continue

        if white_list_cols and col_name not in white_list_cols:
            continue

        if str(data_type).isnumeric():
            continue

        is_big_int = DataType(data_type) is DataType.BIG_INT
        system_name = to_romaji(col_name) if ja_locale else remove_non_ascii_chars(col_name)
        cols_with_types.append(
            {
                'name': pd_name,
                'column_name': col_name,
                'data_type': DataType(data_type).name if not is_big_int else DataType.TEXT.name,
                'name_en': system_name,  # this is system_name
                'romaji': to_romaji(col_name),
                'is_get_date': is_date,
                'check_same_value': same_value,
                'is_big_int': is_big_int,
                'name_jp': col_name if ja_locale else '',
                'name_local': col_name if not ja_locale else '',
                'column_raw_name': col_raw_name,
            },
        )

    return cols_with_types


@log_execution_time()
def convert_utc_df(df_rows, cols, data_types, data_source, table_name):
    for col_name, data_type in zip(cols, data_types):
        is_date = DataType(data_type) is DataType.DATETIME
        if not is_date:
            continue

        # convert utc
        # date_val, tzoffset_str, db_timezone = get_tzoffset_of_random_record(data_source, table_name, col_name)

        # use os timezone
        if data_source.db_detail.use_os_timezone:
            continue

        # is_tz_inside, _, time_offset = get_time_info(date_val, db_timezone)

        # Convert UTC time
        validate_datetime(df_rows, col_name, False, False)
        convert_csv_timezone(df_rows, col_name)
        df_rows.dropna(subset=[col_name], inplace=True)

    return df_rows


@log_execution_time()
def transform_df_to_rows(cols, df_rows, limit, is_skip_normalizing_header: bool = False):
    if not is_skip_normalizing_header:
        df_rows.columns = normalize_list(df_rows.columns)
    return [dict(zip(cols, vals)) for vals in df_rows[0:limit][cols].to_records(index=False).tolist()]


@log_execution_time()
def gen_preview_data_check_dict(rows, previewed_files):
    dic_preview_limit = {}
    file_path = previewed_files[0] if previewed_files else ''
    file_name = ''
    folder_path = ''
    if file_path:
        file_name = os.path.basename(file_path)
        folder_path = os.path.dirname(file_path)

    dic_preview_limit['reach_fail_limit'] = bool(not rows and previewed_files)
    dic_preview_limit['file_name'] = file_name
    dic_preview_limit['folder'] = folder_path
    return dic_preview_limit


@log_execution_time()
def gen_colsname_for_duplicated(cols_name):
    org_cols_name = cols_name.copy()
    cols_name, dup_cols, pd_names = chw.add_suffix_if_duplicated(cols_name)
    return org_cols_name, cols_name, dup_cols, pd_names


def is_valid_list(df_rows):
    return (isinstance(df_rows, list) and len(df_rows)) or (isinstance(df_rows, pd.DataFrame) and not df_rows.empty)


@log_execution_time()
def retrieve_data_from_several_files(
    csv_files,
    csv_delimiter,
    max_record=1000,
    file_name=None,
    skip_head=None,
    n_rows: int | None = None,
    is_transpose: bool = False,
):
    header_names = []
    data_details = []
    sorted_list = get_sorted_files_by_size_and_time(csv_files) if not file_name else [file_name]
    start_time = time.time()
    encoding = None
    for i in range(len(sorted_list)):
        data = None
        csv_file = sorted_list[i]
        try:
            delimiter, encoding = get_delimiter_encoding(csv_file, preview=True)
            csv_delimiter = csv_delimiter or delimiter
            data = read_data(
                csv_file,
                delimiter=csv_delimiter,
                do_normalize=False,
                skip_head=skip_head,
                n_rows=n_rows,
                is_transpose=is_transpose,
            )
            header_names = next(data)
            # strip special symbols
            if i == 0:
                data = strip_special_symbol(data)

            data_details += list(islice(data, max_record))
        finally:
            if data:
                data.close()

        current_time = time.time()
        over_timeout = (current_time - start_time) > PREVIEW_DATA_TIMEOUT
        if (max_record and len(data_details) >= max_record) or over_timeout:
            break

    if data_details:
        is_exception = check_exception_case(header_names, data_details)
        # remove end column because there is trailing comma
        if is_exception:
            data_details = [row[:-1] for row in data_details]

    return header_names, data_details, encoding


@log_execution_time()
def re_order_items_by_datetime_idx(datetime_idx: int, items: List) -> List:
    new_column_ordered = [items[datetime_idx]]
    new_column_ordered += [col for (i, col) in enumerate(items) if i != datetime_idx]
    return new_column_ordered


@log_execution_time()
def extract_data_detail(header_names, data_details, org_header=None, is_skip_normalizing_header: bool = False):
    org_headers = org_header or header_names.copy()
    if not is_skip_normalizing_header:
        # normalization
        header_names = normalize_list(header_names)
    _, header_names, dupl_cols, pd_names = gen_colsname_for_duplicated(header_names)
    df_data_details = normalize_big_rows(data_details, header_names)
    data_types = [gen_data_types(df_data_details[col]) for col in header_names]
    return df_data_details, org_headers, header_names, dupl_cols, data_types, pd_names


def is_header_contains_invalid_chars(header_names: list[str]) -> bool:
    first_row = ''.join(header_names)
    total_num = len(first_row)
    subst_num = len(re.findall(r'[\d\s\t,.:;\-/ ]', first_row))
    nchars = subst_num * 100 / total_num
    return nchars > NUM_CHARS_THRESHOLD


def gen_dummy_header(header_names, data_details=None, line_skip=''):
    """Generate dummy header for current data source
    - if line_skip is not provided (None or '') or line_skip > 0:
        generate dummy header if and only if number of invalid chars > 90%
    - if line_skip = 0:
        always generate dummy header
    @param header_names:
    @param data_details:
    @param line_skip:
    @return:
    """
    dummy_header = False
    partial_dummy_header = False
    org_header = header_names.copy()

    is_blank = line_skip is None or line_skip == EMPTY_STRING
    is_auto_generate_dummy_header = is_header_contains_invalid_chars(header_names)

    # auto generate dummy header rules
    is_gen_from_blank_skip = is_blank and is_auto_generate_dummy_header
    is_gen_from_zero_skip = not is_blank and int(line_skip) == 0
    is_gen_from_number_skip = not is_blank and int(line_skip) > 0 and is_auto_generate_dummy_header
    if is_gen_from_blank_skip or is_gen_from_zero_skip or is_gen_from_number_skip:
        if data_details:
            data_details = [header_names] + data_details
        header_names = ['col'] * len(header_names)
        dummy_header = True

    if EMPTY_STRING in header_names:
        header_names = [name or 'col' for name in header_names]
        partial_dummy_header = True

    return org_header, header_names, dummy_header, partial_dummy_header, data_details
