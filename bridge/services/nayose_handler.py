from __future__ import annotations

import json
import os
import re
import traceback
from functools import wraps
from pathlib import Path
from typing import Dict, Type, Union

import pandas as pd
from flask import request
from flask_babel import get_locale, gettext
from pandas import DataFrame

from ap import multiprocessing_lock
from ap.common.common_utils import check_exist, get_files, get_nayose_path, read_feather_file
from ap.common.constants import EMPTY_STRING, IGNORE_STRING, MAPPING_DATA_LOCK, FileExtension, Suffixes
from ap.common.logger import logger
from ap.common.mapping_constants import DBColumnName
from ap.setting_module.models import (
    MasterDBModel,
    MData,
    MDataGroup,
    MDept,
    MEquip,
    MEquipGroup,
    MFactory,
    MLine,
    MLineGroup,
    MPart,
    MPartType,
    MPlant,
    MProcess,
    MProd,
    MProdFamily,
    MSect,
)
from bridge.models.mapping_factory_machine import MappingFactoryMachine
from bridge.models.mapping_part import MappingPart
from bridge.models.mapping_process_data import MappingProcessData

ALL_DATA_RELATION = 'TheAll'
COMPLETE_MAPPING_DATA = 'complete_mapping_data'
INCOMPLETE_MAPPING_DATA = 'incomplete_mapping_data'
ALL_DATA_RELATION_FILE_NAME = 'scan_all_master_relationship'
SCANNED_FACTORY_FILE_NAME = 'scan_mapping_factory'
SCANNED_PART_FILE_NAME = 'scan_mapping_part'
SCANNED_PROCESS_DATA_FILE_NAME = 'scan_mapping_process_data'
COMPLETE_MAPPING_DATA_FILE_NAME = 'map_complete_mapping_data'
INCOMPLETE_MAPPING_DATA_FILE_NAME = 'map_incomplete_mapping_data'
MASTER_DATA_CFG_FILE_NAME = 'map_master_data'
MASTER_EQUIP_CFG_FILE_NAME = 'map_master_equip'
MASTER_LINE_CFG_FILE_NAME = 'map_master_line'
MASTER_PART_CFG_FILE_NAME = 'map_master_part'
MASTER_SECTION_CFG_FILE_NAME = 'map_master_section'
TRANSACTION_DATA = 'transaction_data'
META_DATA = 'meta_data'
NAYOSE_FILE_DIR = get_nayose_path()
NAYOSE_FILE_NAMES = {
    MappingFactoryMachine.__name__: SCANNED_FACTORY_FILE_NAME,
    MappingPart.__name__: SCANNED_PART_FILE_NAME,
    MappingProcessData.__name__: SCANNED_PROCESS_DATA_FILE_NAME,
    ALL_DATA_RELATION: ALL_DATA_RELATION_FILE_NAME,
    TRANSACTION_DATA: TRANSACTION_DATA,
    META_DATA: META_DATA,
    MLine.get_original_table_name(): MASTER_LINE_CFG_FILE_NAME,
    MSect.get_original_table_name(): MASTER_SECTION_CFG_FILE_NAME,
    MEquip.get_original_table_name(): MASTER_EQUIP_CFG_FILE_NAME,
    MPart.get_original_table_name(): MASTER_PART_CFG_FILE_NAME,
    MData.get_original_table_name(): MASTER_DATA_CFG_FILE_NAME,
    COMPLETE_MAPPING_DATA: COMPLETE_MAPPING_DATA_FILE_NAME,
    INCOMPLETE_MAPPING_DATA: INCOMPLETE_MAPPING_DATA_FILE_NAME,
}


@multiprocessing_lock(MAPPING_DATA_LOCK)
def read_mapping_config_file(
    file_name: str,
    data_table_ids: list[Union[int, str]],
    get_data_source_info=None,
    default_columns: list[str] = None,
):
    def _read_file_to_df(_file_name):
        dfs = []
        for data_table_id in data_table_ids:
            path = os.path.join(NAYOSE_FILE_DIR, str(data_table_id))
            if not os.path.exists(path):
                continue

            files = get_files(path, extension=(FileExtension.Feather.value,))
            files = [file for file in files if file.endswith(f'{_file_name}.{FileExtension.Feather.value}')]
            if not files:
                continue

            for name in files:
                df = read_feather_file(name)
                dfs.append(df)

        df_data = pd.concat(dfs) if dfs else pd.DataFrame()

        # Ignore unused columns
        if _file_name in [
            NAYOSE_FILE_NAMES.get(MappingFactoryMachine.__name__),
            NAYOSE_FILE_NAMES.get(MappingPart.__name__),
            NAYOSE_FILE_NAMES.get(MappingProcessData.__name__),
        ]:
            filter_cols = [
                col
                for col in df_data.columns
                if re.match(r'.*_id$', col) and not col.startswith('t_') and col not in ['data_table_id', 'data_id']
            ]
            df_data.drop(filter_cols, axis=1, inplace=True)

        df_data.drop_duplicates(ignore_index=True, inplace=True)
        return df_data

    df_candidate_data = _read_file_to_df(file_name)
    if file_name == NAYOSE_FILE_NAMES.get(ALL_DATA_RELATION):
        df_mapping_factory_machine = _read_file_to_df(NAYOSE_FILE_NAMES.get(MappingFactoryMachine.__name__))
        df_mapping_part = _read_file_to_df(NAYOSE_FILE_NAMES.get(MappingPart.__name__))
        df_mapping_process_data = _read_file_to_df(NAYOSE_FILE_NAMES.get(MappingProcessData.__name__))
        df_candidate_data = df_candidate_data.merge(
            df_mapping_factory_machine,
            on=[f'{MappingFactoryMachine.__name__}_INDEX', DBColumnName.data_table_id.value],
            suffixes=Suffixes.KEEP_LEFT,
        )
        df_candidate_data = df_candidate_data.merge(
            df_mapping_part,
            on=[f'{MappingPart.__name__}_INDEX', DBColumnName.data_table_id.value],
            suffixes=Suffixes.KEEP_LEFT,
        )
        df_candidate_data = df_candidate_data.merge(
            df_mapping_process_data,
            on=[f'{MappingProcessData.__name__}_INDEX', DBColumnName.data_table_id.value],
            suffixes=Suffixes.KEEP_LEFT,
        )
        # remote column IGNORE_STRING
        df_candidate_data.drop(
            df_candidate_data.filter(like=IGNORE_STRING).columns,
            axis=1,
            inplace=True,
        )
        from ap.mapping_config.services.base_config import empty_replace_dict

        # remote column master id in scan mater file
        cols = [col for col in df_candidate_data.columns if re.match('^t_.*|data_table_id|.*INDEX$', col)]
        df_candidate_data = df_candidate_data[cols].replace(empty_replace_dict)

    # Add data source name & source column
    if not df_candidate_data.empty and get_data_source_info:
        from ap.setting_module.models import CfgDataTableColumn

        data_table_ids = (
            df_candidate_data[CfgDataTableColumn.data_table_id.name].drop_duplicates().astype(int).to_list()
        )
        data_source_dict = get_data_source_info(data_table_ids)
        data_source_df = pd.DataFrame(data_source_dict, dtype=pd.StringDtype())
        df_candidate_data[CfgDataTableColumn.data_table_id.name] = df_candidate_data[
            CfgDataTableColumn.data_table_id.name
        ].astype(pd.StringDtype())
        df_candidate_data = df_candidate_data.merge(
            data_source_df,
            how='left',
            on=[CfgDataTableColumn.data_table_id.name],
        )
        df_candidate_data.replace({pd.NA: EMPTY_STRING}, inplace=True)

    # Add missing columns into df
    if default_columns:
        for col in default_columns:
            if col not in df_candidate_data:
                df_candidate_data[col] = EMPTY_STRING

    return df_candidate_data


def select_data_table_ids(from_data_table_id: int, to_data_table_id: int) -> list[str]:
    if not check_exist(NAYOSE_FILE_DIR):
        return []

    from_data_table_id = (
        int(from_data_table_id) if from_data_table_id and isinstance(from_data_table_id, str) else from_data_table_id
    )
    to_data_table_id = (
        int(to_data_table_id) if to_data_table_id and isinstance(to_data_table_id, str) else to_data_table_id
    )
    with os.scandir(NAYOSE_FILE_DIR) as it:
        if not from_data_table_id and not to_data_table_id:
            return [entry.name for entry in it if entry.is_dir()]
        else:
            return [
                entry.name
                for entry in it
                if entry.is_dir() and from_data_table_id <= int(entry.name) <= to_data_table_id
            ]


def check_scanned_files_exist(from_data_table_id, to_data_table_id, file_names: list) -> [dict, bool]:
    result = {}
    does_all_files_exist = True
    from_data_table_id = (
        int(from_data_table_id) if from_data_table_id and isinstance(from_data_table_id, str) else from_data_table_id
    )
    to_data_table_id = (
        int(to_data_table_id) if to_data_table_id and isinstance(to_data_table_id, str) else to_data_table_id
    )
    data_table_ids = select_data_table_ids(from_data_table_id, to_data_table_id)

    for data_table_id in data_table_ids:
        file_exists = {}
        for file_name in file_names:
            path = os.path.join(NAYOSE_FILE_DIR, data_table_id, f'{file_name}.{FileExtension.Feather.value}')
            file_exists[file_name] = check_exist(path)
            if not file_exists[file_name]:
                does_all_files_exist = False
        result[data_table_id] = file_exists

    return result, does_all_files_exist


DEFAULT_DIRECT_ID = 2
DICT_DIRECT_COLUMN_ID = {
    DBColumnName.line_id.value: DEFAULT_DIRECT_ID,
    DBColumnName.line_group_id.value: DEFAULT_DIRECT_ID,
    DBColumnName.equip_id.value: DEFAULT_DIRECT_ID,
    DBColumnName.equip_group_id.value: DEFAULT_DIRECT_ID,
    DBColumnName.prod_id.value: DEFAULT_DIRECT_ID,
    DBColumnName.prod_family_id.value: DEFAULT_DIRECT_ID,
}


def get_master_data(table_classes: list[Type[MasterDBModel]], is_add_name_all_column: bool = True):
    from ap.mapping_config.services.base_config import empty_replace_dict

    dict_result = {}
    for model_cls in table_classes:
        model_df: DataFrame = (
            model_cls.get_all_as_df(is_convert_null_string_to_na=False)
            .astype(pd.StringDtype())
            .replace(empty_replace_dict)
        )
        for col in model_df.columns:
            direct_id = DICT_DIRECT_COLUMN_ID.get(col)
            if direct_id:
                model_df = model_df[model_df[col] != str(direct_id)]

        # determine name all column
        if is_add_name_all_column and model_cls in [
            MFactory,
            MPlant,
            MDept,
            MSect,
            MProdFamily,
            MProd,
            MLineGroup,
            MEquipGroup,
            MPartType,
            MProcess,
            MDataGroup,
        ]:
            model_df = determine_name_all_column(model_cls, model_df)

        dict_result[model_cls] = model_df

    return dict_result


def determine_name_all_column(model_cls: Type[MasterDBModel], model_df):
    current_locale = get_locale().language
    name_all_column = model_cls.get_name_all_column()
    if current_locale == 'ja':
        highest_priority_lang = model_cls.get_jp_name_column()
        second_priority_lang = model_cls.get_en_name_column()
        third_priority_lang = model_cls.get_local_name_column()
    elif current_locale == 'en':
        highest_priority_lang = model_cls.get_en_name_column()
        second_priority_lang = model_cls.get_local_name_column()
        third_priority_lang = model_cls.get_jp_name_column()
    else:
        highest_priority_lang = model_cls.get_local_name_column()
        second_priority_lang = model_cls.get_en_name_column()
        third_priority_lang = model_cls.get_jp_name_column()

    model_df[name_all_column] = model_df[highest_priority_lang]
    missing_value_index = model_df[model_df[name_all_column] == EMPTY_STRING].index.tolist()
    if missing_value_index:
        # if FIRST priority lang value is empty, fill by SECOND priority lang value
        model_df[name_all_column].update(model_df.loc[missing_value_index][second_priority_lang])
        missing_value_index = model_df[model_df[name_all_column] == EMPTY_STRING].index.tolist()
        if missing_value_index:
            # if SECOND priority lang value is empty, fill by THIRD priority lang value
            model_df[name_all_column].update(model_df.loc[missing_value_index][third_priority_lang])

    return model_df


def duplicate_check(model_cls, data: Dict):
    is_duplicate, row = model_cls.is_row_exist(data)
    return is_duplicate, row


def has_new_master(data_table_id: str | int) -> bool:
    all_master_file_path = (
        Path(NAYOSE_FILE_DIR)
        / str(data_table_id)
        / f'{NAYOSE_FILE_NAMES[ALL_DATA_RELATION]}.{FileExtension.Feather.value}'
    )
    file_empty = True
    if all_master_file_path.exists():
        file_empty = read_feather_file(all_master_file_path).empty
    return not file_empty


def check_files(file_names):
    """Decorator to check job id that there are full pickle files in folder or not"""

    def decorator(fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            try:
                request_data = json.loads(request.data if len(request.data) > 0 else '{}')
                data_table_id = request_data.get('data_table_id')
                if not data_table_id:
                    from_data_table_id = request_data.get('from_data_table_id')
                    to_data_table_id = request_data.get('to_data_table_id')
                else:
                    from_data_table_id = data_table_id
                    to_data_table_id = data_table_id

                data_table_id_file_status_dict, does_all_files_exist = check_scanned_files_exist(
                    from_data_table_id,
                    to_data_table_id,
                    file_names,
                )
                data_table_ids = list(data_table_id_file_status_dict.keys())
                kwargs['data_table_ids'] = data_table_ids

                if does_all_files_exist:
                    result = fn(*args, **kwargs)
                else:
                    result = {
                        'status': False,
                        'responseData': None,
                        'is_warn': True,
                        'message': gettext('Required scanned files does exist. Please go back this page later.'),
                    }

            except Exception as e:
                logger.error(e)
                traceback.print_exc()
                return {
                    'status': False,
                    'responseData': None,
                    'message': str(e),
                }

            return result

        return wrapper

    return decorator
