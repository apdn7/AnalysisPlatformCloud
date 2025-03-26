from __future__ import annotations

import collections
from collections import defaultdict

import pandas as pd

from ap.common.constants import MasterDBType
from ap.setting_module.models import CfgProcessColumn
from bridge.models.cfg_data_source import CfgDataSource
from bridge.models.transaction_model import MergeFlag, TransactionData, combine_rows_one_by_one


def get_df_insert_and_duplicated_ids(
    db_instance,
    transaction_data: TransactionData,
    *,
    df_insert: pd.DataFrame,
    df_old: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.Series]:
    """Should return inserting dataframe and deleting ids"""
    deleting_ids = pd.Series()

    if df_old.empty:
        return df_insert, deleting_ids

    if df_insert.empty:
        return pd.DataFrame(), deleting_ids

    data_source_ids = pd.concat(
        [
            df_insert[transaction_data.data_source_id_col_name],
            df_old[transaction_data.data_source_id_col_name],
        ],
    ).unique()
    ids_by_master_type = get_master_types_from_data_sources(db_instance, data_source_ids)

    list_df_insert = []
    list_deleting_ids = []

    others_ids = ids_by_master_type.get(MasterDBType.OTHERS, set())
    if others_ids:
        df_insert_others, deleting_ids_others = get_df_insert_and_duplicated_ids_others(
            transaction_data,
            df_insert=df_insert[df_insert[transaction_data.data_source_id_col_name].isin(others_ids)],
            df_old=df_old[df_old[transaction_data.data_source_id_col_name].isin(others_ids)],
        )
        list_df_insert.append(df_insert_others)
        list_deleting_ids.append(deleting_ids_others)

    v2_ids = ids_by_master_type.get(MasterDBType.V2, set())
    if v2_ids:
        df_insert_v2, deleting_ids_v2 = get_df_insert_and_duplicated_ids_v2(
            transaction_data,
            df_insert=df_insert[df_insert[transaction_data.data_source_id_col_name].isin(v2_ids)],
            df_old=df_old[df_old[transaction_data.data_source_id_col_name].isin(v2_ids)],
        )
        list_df_insert.append(df_insert_v2)
        list_deleting_ids.append(deleting_ids_v2)

    df_insert = pd.concat(list_df_insert)
    deleting_ids = pd.concat(list_deleting_ids)

    return df_insert, deleting_ids


def get_df_insert_and_duplicated_ids_others(
    transaction_data: TransactionData,
    *,
    df_insert: pd.DataFrame,
    df_old: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.Series]:
    other_columns = get_drop_duplicated_columns_for_others(transaction_data)

    df_all = pd.concat([df_insert, df_old])
    is_duplicated = df_all.duplicated(subset=other_columns, keep=False)
    duplicated_ids = df_all[is_duplicated][transaction_data.id_col_name].unique()
    df_insert = df_insert[~df_insert[transaction_data.id_col_name].isin(duplicated_ids)]

    # deleting ids always is empty in this case.
    # We don't delete in database, we just don't insert that record back again
    return df_insert, pd.Series()


MergeResult = collections.namedtuple(
    'MergeResult',
    ['df_merged', 'df_measurement_after', 'df_history_after', 'measurement_used_ids', 'history_used_ids'],
)


def get_df_insert_and_duplicated_ids_v2(
    transaction_data: TransactionData,
    *,
    df_insert: pd.DataFrame,
    df_old: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.Series]:
    """
    Get non duplicated columns from v2 process
    let compare 2 records: left and right
        - if they have different master columns -> different
        - if they have merge flag: MEASUREMENT and HISTORY -> merge them
        - if they have merge flag: MEASUREMENT/HISTORY and DONE -> different
    """

    done_flag = MergeFlag.done_flag(MasterDBType.V2.name)
    measurement_flag = MergeFlag.V2_MEASUREMENT.value
    history_flag = MergeFlag.V2_HISTORY.value

    # always insert df with `DONE` status
    df_insert_done = df_insert[df_insert[transaction_data.merge_flag_col_name] == done_flag]

    # always ignore df old with `DONE` status
    df_old = df_old[df_old[transaction_data.merge_flag_col_name] != done_flag]

    merged_measurement_to_history = get_df_merge_measurement_and_history(
        transaction_data,
        df_measurement=df_insert[df_insert[transaction_data.merge_flag_col_name] == measurement_flag],
        df_history=df_old[df_old[transaction_data.merge_flag_col_name] == history_flag],
    )

    merged_history_to_measurement = get_df_merge_measurement_and_history(
        transaction_data,
        df_measurement=df_old[df_old[transaction_data.merge_flag_col_name] == measurement_flag],
        df_history=df_insert[df_insert[transaction_data.merge_flag_col_name] == history_flag],
    )

    df_insert = pd.concat(
        [
            df_insert_done,
            merged_measurement_to_history.df_merged,
            merged_measurement_to_history.df_measurement_after,
            merged_history_to_measurement.df_merged,
            merged_history_to_measurement.df_history_after,
        ],
    )

    deleting_ids = pd.concat(
        [
            merged_history_to_measurement.measurement_used_ids,
            merged_measurement_to_history.history_used_ids,
        ],
    )

    return df_insert, deleting_ids  # noqa


def get_master_types_from_data_sources(db_instance, data_source_ids: list[int]) -> dict[MasterDBType, set[int]]:
    dict_data_sources = CfgDataSource.get_in_ids(db_instance, ids=data_source_ids, is_return_dict=True)
    ids_by_master_type: dict[MasterDBType, set[int]] = defaultdict(set)
    for data_source_id, data_source in dict_data_sources.items():
        master_type_str = data_source[CfgDataSource.Columns.master_type.name]
        master_type = MasterDBType[master_type_str]
        ids_by_master_type[master_type].add(data_source_id)
    return ids_by_master_type


def remove_unused_columns_and_add_missing_columns(
    df: pd.DataFrame,
    required_columns: list[str] | pd.Index,
) -> pd.DataFrame:
    columns = set(required_columns)

    # remove redundant
    intersected_columns = columns.intersection(df.columns)
    df = df[intersected_columns]

    # add missing
    missing_columns = columns.difference(df.columns)
    df.loc[:, missing_columns] = None

    return df


def get_drop_duplicated_columns_for_others(transaction_data: TransactionData) -> list[str]:
    def is_good_column(column: CfgProcessColumn) -> bool:
        # do not include function column
        if column.function_details:
            return False
        # do not include data source id column since the name is wrong
        if column.is_data_source_name_column():
            return False
        return True

    good_columns = [col.bridge_column_name for col in filter(is_good_column, transaction_data.cfg_process_columns)]

    # add `data_source_id` to good column
    good_columns.append(transaction_data.data_source_id_col_name)

    return good_columns


def get_df_merge_measurement_and_history(
    transaction_data: TransactionData,
    df_measurement: pd.DataFrame,
    df_history: pd.DataFrame,
) -> MergeResult:
    get_date_col = transaction_data.getdate_column.bridge_column_name
    df_measurement = df_measurement.sort_values(get_date_col)
    df_history = df_history.sort_values(get_date_col)

    # merge record from history to measurement
    df_merged, is_measurement_used, is_history_used = combine_rows_one_by_one(
        df_measurement,
        df_history,
        on=transaction_data.master_columns,
    )
    df_merged[TransactionData.merge_flag_col_name] = MergeFlag.done_flag(MasterDBType.V2.name)
    df_measurement_after = df_measurement[~is_measurement_used]
    df_history_after = df_history[~is_history_used]
    measurement_used_ids = df_measurement.loc[is_measurement_used, transaction_data.id_col_name]
    history_used_ids = df_history.loc[is_history_used, transaction_data.id_col_name]

    return MergeResult(
        df_merged=df_merged,
        df_measurement_after=df_measurement_after,
        df_history_after=df_history_after,
        measurement_used_ids=measurement_used_ids,
        history_used_ids=history_used_ids,
    )
