from pandas import DataFrame

from ap.api.trace_data.services.regex_infinity import (
    check_validate_target_column,
    get_changed_value_after_validate,
    validate_data_with_regex,
    validate_data_with_simple_searching,
)
from ap.common.constants import THIN_DATA_COUNT
from ap.common.logger import log_execution_time


@log_execution_time()
def validate_data(df: DataFrame):
    if len(df) > THIN_DATA_COUNT:
        df_before = get_sample_df(df)
        df_before = df_before.convert_dtypes()
        df_after = validate_data_with_regex(df_before)
        checked_cols, dic_abnormal = get_changed_value_after_validate(df_before, df_after)
        df = validate_data_with_simple_searching(df, checked_cols, dic_abnormal)
    else:
        df = validate_data_with_regex(df)

    return df


@log_execution_time()
def get_sample_df(df):
    sample_df = df.head(THIN_DATA_COUNT)
    number_cols = df.select_dtypes(include=['integer', 'float']).columns.tolist()
    for col in number_cols:
        if not check_validate_target_column(col):
            continue
        try:
            min_idx = df[col].idxmin()
            max_idx = df[col].idxmax()
            sample_df = sample_df.append(df.loc[min_idx], ignore_index=True)
            sample_df = sample_df.append(df.loc[max_idx], ignore_index=True)
        except Exception:
            pass

    return sample_df
