from collections import defaultdict

import pandas as pd
from pandas import DataFrame

from ap.common.common_utils import DATE_FORMAT_FOR_ONE_HOUR, FREQ_FOR_RANGE, add_seconds, convert_time
from ap.common.constants import CacheType, DataCountType
from ap.common.logger import log_execution_time
from ap.common.memoize import memoize
from ap.trace_data.models import ProcDataCount


# TODO(209-210): this function might not be correctly implemented
@log_execution_time()
@memoize(cache_type=CacheType.TRANSACTION_DATA)
def get_data_count_by_time_range(proc_id, start_date, end_date, query_type, local_tz, count_in_file):
    data_count = ProcDataCount.get_by_proc_id(proc_id, start_date, end_date, count_in_file)
    data_count = [[r.datetime, r.count] for r in data_count]
    if not data_count:
        return None, None, None

    freq, final_freq, str_format = FREQ_FOR_RANGE[query_type]
    start_date = convert_time(start_date, return_string=False)
    end_date = convert_time(end_date, return_string=False)
    start_date = pd.Series(start_date).dt.tz_localize(tz='UTC').dt.tz_convert(tz=local_tz).to_list()[0]
    end_date = pd.Series(end_date).dt.tz_localize(tz='UTC').dt.tz_convert(tz=local_tz).to_list()[0]

    date_time_col = ProcDataCount.datetime.key
    count_col = ProcDataCount.count.key
    df = pd.DataFrame(data_count, columns=[date_time_col, count_col])
    df[date_time_col] = df[date_time_col].dt.tz_localize(tz='UTC').dt.tz_convert(tz=local_tz)

    date_range = pd.date_range(start_date, add_seconds(end_date, -1), name=date_time_col, freq=freq)
    df_sum: DataFrame = pd.DataFrame(date_range)
    df_sum[date_time_col] = pd.to_datetime(df_sum[date_time_col])
    df_sum[count_col] = 0

    df_sum = df_sum.append(df)
    date_time_series = df_sum[date_time_col].dt.strftime(DATE_FORMAT_FOR_ONE_HOUR)
    df_sum[date_time_col] = pd.to_datetime(date_time_series)
    df_sum = df_sum.groupby(pd.Grouper(key=date_time_col, freq=freq))[count_col].sum().reset_index()
    min_val = int(df_sum[count_col].min())
    max_val = int(df_sum[count_col].max())

    dic_sum = df_sum.groupby(pd.Grouper(key=date_time_col, freq=final_freq))[count_col].apply(list).to_dict()
    dic_output = defaultdict(dict)
    counts = []
    for key, vals in dic_sum.items():
        new_key = convert_time(key, format_str=str_format)
        dic_output[new_key][count_col] = vals
        counts.extend(vals)

    if query_type == DataCountType.MONTH.value:
        dic_output[count_col] = counts

    return dic_output, min_val, max_val
