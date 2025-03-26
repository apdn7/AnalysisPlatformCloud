from __future__ import annotations

import re
from collections.abc import Iterator
from pathlib import Path
from typing import List, Optional, Union

import numpy as np
import pandas as pd
from pandas import DataFrame

from ap.common.common_utils import open_with_zip
from ap.common.constants import (
    WELL_KNOWN_COLUMNS,
    DataType,
    DBType,
    v2_PART_NO_REGEX,
)
from ap.common.logger import log_execution_time
from ap.common.services.csv_content import get_metadata
from ap.common.services.data_type import gen_data_types
from ap.common.services.jp_to_romaji_utils import to_romaji
from ap.setting_module.models import (
    CfgDataSourceCSV,
    CfgProcess,
    CfgProcessColumn,
    CfgProcessUnusedColumn,
    crud_config,
    make_session,
)
from ap.setting_module.schemas import ProcessColumnSchema


@log_execution_time()
def predict_v2_data_type(columns, df):
    """
    predict data type for v2 columns
    """
    data_types = [gen_data_types(df[col]) for col in columns]
    return data_types


@log_execution_time()
def add_process_columns(process_id, column_data: list):
    proc_column_schemas = ProcessColumnSchema()
    current_columns = CfgProcessColumn.get_all_columns(process_id)
    with make_session() as meta_session:
        sensors = set()
        for column in column_data:
            proc_column = proc_column_schemas.load(column)
            # proc_column.english_name = to_romaji(proc_column.column_name)
            proc_column.name_en = to_romaji(proc_column.column_name)
            proc_column.process_id = process_id
            current_columns.append(proc_column)
            sensors.add((process_id, proc_column.column_name, DataType[proc_column.data_type].value))

        # save columns
        crud_config(
            meta_session=meta_session,
            data=current_columns,
            parent_key_names=CfgProcessColumn.process_id.key,
            key_names=CfgProcessColumn.column_name.key,
            model=CfgProcessColumn,
        )

    return True


def add_remaining_v2_columns(df, process_id):
    remaining_columns = find_remaining_columns(process_id, df.columns)
    if not remaining_columns:
        return False

    data_types = predict_v2_data_type(remaining_columns, df)
    columns = []
    for i, col in enumerate(remaining_columns):
        columns.append(
            {
                'column_name': col,
                'data_type': DataType(data_types[i]).name,
                'predict_type': DataType(data_types[i]).name,
            },
        )

    return add_process_columns(process_id, columns)


@log_execution_time()
def get_datasource_type(process_id):
    proc_cfg: CfgProcess = CfgProcess.query.get(process_id)
    data_src: CfgDataSourceCSV = CfgDataSourceCSV.query.get(proc_cfg.data_source_id)
    if data_src:
        return data_src.cfg_data_source.type
    return None


@log_execution_time()
def is_v2_data_source(ds_type=None, process_id=None):
    ds_type = ds_type or get_datasource_type(process_id)
    if ds_type:
        return ds_type.lower() == DBType.V2.name.lower()
    return False


@log_execution_time()
def build_read_csv_for_v2(file_path: str, datasource_type: DBType = DBType.V2):
    from ap.api.setting_module.services.data_import import NA_VALUES

    # copy from bridge's `build_read_csv_params`
    params = {}
    with open_with_zip(file_path, 'rb') as f:
        metadata = get_metadata(f, is_full_scan_metadata=True, default_csv_delimiter=',')
        params.update(metadata)

    must_get_columns = tuple(WELL_KNOWN_COLUMNS[datasource_type.name].keys())
    dtype = 'str'
    params.update(
        {
            'usecols': lambda x: x.startswith(must_get_columns),
            'skipinitialspace': True,
            'na_values': NA_VALUES,
            'error_bad_lines': False,
            'skip_blank_lines': True,
            'dtype': dtype,
        },
    )
    return params


@log_execution_time()
def save_unused_columns(process_id, unused_columns):
    is_v2 = is_v2_data_source(process_id=process_id)
    if not is_v2:
        return

    if unused_columns:
        unused_columns = [CfgProcessUnusedColumn(process_id=process_id, column_name=name) for name in unused_columns]
        with make_session() as meta_session:
            crud_config(
                meta_session=meta_session,
                data=unused_columns,
                parent_key_names=CfgProcessUnusedColumn.process_id.key,
                key_names=CfgProcessUnusedColumn.column_name.key,
                model=CfgProcessUnusedColumn,
            )
    else:
        CfgProcessUnusedColumn.delete_all_columns_by_proc_id(process_id)


@log_execution_time()
def find_remaining_columns(process_id, all_columns):
    """
    Get new columns of V2 process that are not in unused columns and used columns
    :param process_id:
    :param all_columns:
    :return: remaining column that need to import
    """
    unused_columns = CfgProcessUnusedColumn.get_all_unused_columns_by_process_id(process_id)

    import_columns = [col.column_name for col in CfgProcessColumn.get_all_columns(process_id)]
    used_columns = unused_columns + import_columns
    return [col for col in all_columns if col not in used_columns]


def get_v2_datasource_type_from_file(v2_file: Union[Path, str]) -> Optional[DBType]:
    """Check if this file is v2, v2 multi or v2 history"""
    df = pd.read_csv(v2_file, nrows=1)
    return get_v2_datasource_type_from_df(df)


def get_v2_datasource_type_from_df(df: DataFrame) -> Optional[DBType]:
    columns = {col.strip() for col in df.columns}
    for datasource_type in [DBType.V2_HISTORY, DBType.V2, DBType.V2_MULTI]:
        must_exist_columns = set(WELL_KNOWN_COLUMNS[datasource_type.name].keys())
        if columns >= must_exist_columns:
            return datasource_type
    return None


@log_execution_time()
def transform_partno_value(df: pd.DataFrame, partno_columns: List) -> pd.DataFrame:
    """
    tranform part-no value to import data
    input: JP1234567890
    output: 7890
    """
    if df.empty:
        return df

    if partno_columns:
        r = re.compile(v2_PART_NO_REGEX)
        for column in partno_columns:
            df[column] = np.vectorize(lambda x: x[-4:] if isinstance(x, str) and bool(r.match(x)) else x)(df[column])
    return df


def normalize_column_name(columns_name: list[str] | set[str] | Iterator[str]):
    # define to convert these symbols to underscore
    convert_symbols = ['.', '/', ' ', '-']
    normalize_cols = []
    for column_name in columns_name:
        col_name = column_name.lower()
        for symbol in convert_symbols:
            col_name = col_name.replace(symbol, '_')
        if col_name[-1] == '_':
            # remove last underscore of column name
            # eg. serial_no_ -> serial_no
            col_name = col_name[:-1]
        normalize_cols.append(col_name)
    return normalize_cols
