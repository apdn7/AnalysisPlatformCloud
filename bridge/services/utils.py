from __future__ import annotations

import copy
import re

from ap import log_execution_time
from ap.api.setting_module.services.v2_etl_services import normalize_column_name
from ap.common.constants import WELL_KNOWN_COLUMNS, DataGroupType, EFAMasterColumn, MasterDBType, V2MasterColumn
from ap.common.memoize import memoize
from bridge.models.cfg_data_source import EFA_MASTERS, EFA_TABLES

MUST_EXISTED_COLUMNS_FOR_MASTER_TYPE = {
    MasterDBType.V2.name: [
        {'計測日時', '計測項目名', '計測値'},
    ],
    MasterDBType.V2_MULTI.name: [
        {'加工日時', '測定項目名', '測定値'},
        {'processed_date_time', 'measurement_item_name', 'measured_value'},
    ],
    MasterDBType.V2_HISTORY.name: [
        {'計測日時', '子部品シリアルNo'},
    ],
    MasterDBType.V2_MULTI_HISTORY.name: [
        {'加工日時', '子部品シリアルNo'},
    ],
}


def check_missing_column_by_column_name(master_type: str, required_columns: list[str], file_columns: list[str]) -> bool:
    dummy_columns = set()
    if MasterDBType.is_v2_group(master_type):
        dummy_columns = set(V2MasterColumn.get_dummy_column_name())
        dummy_columns.add(DataGroupType.FileName.name)
    elif MasterDBType.is_efa_group(master_type):
        dummy_columns = set(EFAMasterColumn.get_dummy_column_name())
    contains_all_required_columns = set(file_columns) >= {c for c in required_columns if c not in dummy_columns}
    has_missing = not contains_all_required_columns
    return has_missing


def check_missing_column_by_data_group_type(master_type: str, file_columns: list[str]) -> bool:
    existed_group_types = set(get_well_known_columns(master_type, file_columns).values())
    required_group_types = set(get_well_known_columns(master_type, cols=None).values())
    contains_all_required_group_types = required_group_types <= existed_group_types
    has_missing = not contains_all_required_group_types
    return has_missing


@log_execution_time()
def get_well_known_columns_for_others_type(
    well_known_columns: dict[str, str],
    cols: list[str] | set[str],
) -> dict[str, int]:
    results = {}
    master_date_group_types = []
    for col in cols:
        for data_group_type, pattern_regex in well_known_columns.items():
            if pattern_regex and re.search(pattern_regex, col, re.IGNORECASE):
                if data_group_type not in master_date_group_types:
                    results[col] = data_group_type
                    master_date_group_types.append(data_group_type)
                else:
                    results[col] = DataGroupType.HORIZONTAL_DATA.value

                break

            results[col] = DataGroupType.HORIZONTAL_DATA.value

    return results


@log_execution_time()
def get_well_known_columns_for_v2_type(
    well_known_columns: dict[str, str],
    cols: list[str] | set[str],
) -> dict[str, int]:
    normalized_cols = normalize_column_name(cols)

    def get_group_type(col: str, normalized_col: str) -> str | None:
        return well_known_columns.get(col, None) or well_known_columns.get(normalized_col, None)

    group_types = map(get_group_type, cols, normalized_cols)
    return {col: group_type for col, group_type in zip(cols, group_types) if group_type is not None}


@memoize()
def get_well_known_columns(master_type: str, cols: list[str] | set[str] | None = None) -> dict[str, int]:
    old_well_known_columns = WELL_KNOWN_COLUMNS.get(master_type, {})
    if not cols:
        return copy.deepcopy(old_well_known_columns)

    if master_type == MasterDBType.OTHERS.name:
        well_known_columns = get_well_known_columns_for_others_type(old_well_known_columns, cols)
    elif MasterDBType.is_v2_group(master_type):
        well_known_columns = get_well_known_columns_for_v2_type(old_well_known_columns, cols)
    else:
        well_known_columns = copy.deepcopy(old_well_known_columns)
    return well_known_columns


def get_master_type_based_on_table_name(master_type: str, table_name: str) -> str:
    """Currently only works for EFA master data"""
    if master_type != MasterDBType.EFA.name:
        return master_type

    dic_master_type_n_table = dict(zip(EFA_MASTERS, EFA_TABLES))
    efa_history_table_name = dic_master_type_n_table[MasterDBType.EFA_HISTORY.name]
    if str(table_name).startswith(efa_history_table_name):
        master_type = MasterDBType.EFA_HISTORY.name

    return master_type


def get_master_type_based_on_column_names(
    master_type: str,
    column_names: list[str] | set[str] | None = None,
) -> str:
    """Currently only works for V2 master data
    Checking if column names is referring to V2, V2 multi, V2 history or V2 multi history
    Currently, we use hardcoded values through `MUST_EXISTED_COLUMNS_FOR_MASTER_TYPE`
    Consider refactor this later
    """
    if master_type != MasterDBType.V2.name or column_names is None:
        return master_type

    return get_specific_v2_type_based_on_column_names(column_names)


def get_specific_v2_type_based_on_column_names(
    column_names: list[str] | set[str] | None = None,
) -> str | None:
    """Currently only works for V2 master data
    Checking if column names is referring to V2, V2 multi, V2 history or V2 multi history
    Currently, we use hardcoded values through `MUST_EXISTED_COLUMNS_FOR_MASTER_TYPE`
    Consider refactor this later
    """
    normalized_columns = set(normalize_column_name(column_names))

    for m_type in [
        MasterDBType.V2.name,
        MasterDBType.V2_MULTI.name,
        MasterDBType.V2_HISTORY.name,
        MasterDBType.V2_MULTI_HISTORY.name,
    ]:
        if DataGroupType.FileName.name in column_names:
            column_names = list(set(column_names) - set(DataGroupType.FileName.name))

        has_missing_columns = check_missing_column_by_data_group_type(m_type, column_names)
        if has_missing_columns:
            continue

        for must_existed_columns in MUST_EXISTED_COLUMNS_FOR_MASTER_TYPE.get(m_type):
            if normalized_columns >= set(normalize_column_name(must_existed_columns)):
                return m_type

    return None


@memoize()
def get_master_type(
    master_type: str,
    *,
    table_name: str | None = None,
    column_names: list[str] | set[str] | None = None,
) -> str:
    if table_name is not None:
        master_type = get_master_type_based_on_table_name(master_type, table_name)
    if column_names is not None:
        master_type = get_master_type_based_on_column_names(master_type, column_names)
    return master_type
