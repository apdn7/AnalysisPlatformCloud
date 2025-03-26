from __future__ import annotations

import functools
from typing import Literal

import pandas as pd
from pandas import Series

from ap import log_execution_time
from ap.common.common_utils import get_current_timestamp
from ap.common.constants import CacheType
from ap.common.memoize import set_all_cache_expired
from ap.trace_data.models import ProcDataCount


@log_execution_time()
def save_proc_data_count(db_instance, df, proc_id, get_date_col, job_id=None):
    save_proc_data_count_multiple_dfs(
        db_instance,
        proc_id=proc_id,
        get_date_col=get_date_col,
        dfs_push_to_db=df,
        job_id=job_id,
    )


@log_execution_time()
def save_proc_data_count_multiple_dfs(
    db_instance,
    *,
    proc_id,
    get_date_col,
    dfs_push_to_db: list[pd.DataFrame] | pd.DataFrame = None,
    dfs_pop_from_db: list[pd.DataFrame] | pd.DataFrame = None,
    dfs_push_to_file: list[pd.DataFrame] | pd.DataFrame = None,
    dfs_pop_from_file: list[pd.DataFrame] | pd.DataFrame = None,
    job_id=None,
):
    def check_args(dfs):
        if dfs is None:
            return []
        if not isinstance(dfs, list):
            return [dfs]
        return dfs

    dfs_push_to_db = check_args(dfs_push_to_db)
    dfs_pop_from_db = check_args(dfs_pop_from_db)
    dfs_push_to_file = check_args(dfs_push_to_file)
    dfs_pop_from_file = check_args(dfs_pop_from_file)

    get_proc_data_count_df_func = functools.partial(
        get_proc_data_count_df,
        proc_id=proc_id,
        get_date_col=get_date_col,
        job_id=job_id,
    )

    # TODO: aggregate to one df instead of run 4 times
    aggregated_df = pd.DataFrame()

    for df in dfs_push_to_db:
        count_df = get_proc_data_count_df_func(df, is_db=True, decrease=False)
        aggregated_df = pd.concat([aggregated_df, count_df])

    for df in dfs_pop_from_db:
        count_df = get_proc_data_count_df_func(df, is_db=True, decrease=True)
        aggregated_df = pd.concat([aggregated_df, count_df])

    for df in dfs_push_to_file:
        count_df = get_proc_data_count_df_func(df, is_db=False, decrease=False)
        aggregated_df = pd.concat([aggregated_df, count_df])

    for df in dfs_pop_from_file:
        count_df = get_proc_data_count_df_func(df, is_db=False, decrease=True)
        aggregated_df = pd.concat([aggregated_df, count_df])

    if aggregated_df.empty:
        return

    agg_keys = {ProcDataCount.count.key: 'sum', ProcDataCount.count_file.key: 'sum'}
    aggregated_df = aggregated_df.groupby(ProcDataCount.datetime.key).agg(agg_keys).reset_index()
    aggregated_df[ProcDataCount.process_id.key] = proc_id
    aggregated_df[ProcDataCount.job_id.key] = job_id
    aggregated_df[ProcDataCount.created_at.key] = get_current_timestamp()

    db_instance.bulk_insert(ProcDataCount.get_table_name(), aggregated_df.columns, aggregated_df.values.tolist())

    # clear cache
    set_all_cache_expired(CacheType.TRANSACTION_DATA)


def get_proc_data_count_df(df, *, proc_id, get_date_col, decrease: bool, is_db: bool, job_id=None):
    if df.empty:
        return pd.DataFrame()

    count_column = ProcDataCount.count.key if is_db else ProcDataCount.count_file.key
    count_df = calculate_value_counts_per_hours(df, proc_id, get_date_col, job_id, count_column=count_column)
    if decrease:
        count_df[count_column] = -count_df[count_column]

    return count_df


@log_execution_time()
def calculate_value_counts_per_hours(
    df,
    proc_id,
    get_date_col,
    job_id=None,
    count_column: Literal['count', 'count_file'] = ProcDataCount.count.key,
):
    s = pd.to_datetime(df[get_date_col], errors='coerce')
    s: Series = (s.dt.year * 1_00_00_00 + s.dt.month * 1_00_00 + s.dt.day * 1_00 + s.dt.hour).value_counts()
    count_df = s.rename(count_column).reset_index(name=count_column)
    count_df[ProcDataCount.datetime.key] = pd.to_datetime(count_df['index'], format='%Y%m%d%H')
    count_df[ProcDataCount.process_id.key] = proc_id
    count_df[ProcDataCount.job_id.key] = job_id
    count_df[ProcDataCount.created_at.key] = get_current_timestamp()

    if count_column == ProcDataCount.count.key:
        count_df[ProcDataCount.count_file.key] = 0
    else:
        count_df[ProcDataCount.count.key] = 0

    columns = [
        ProcDataCount.process_id.key,
        ProcDataCount.datetime.key,
        ProcDataCount.count.key,
        ProcDataCount.count_file.key,
        ProcDataCount.job_id.key,
        ProcDataCount.created_at.key,
    ]
    return count_df[columns]
