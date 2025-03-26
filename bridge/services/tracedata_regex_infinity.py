# copy from trace_data.services.regex_infinity.py

import re

import numpy as np
import pandas as pd
from pandas import DataFrame

from ap.common.logger import log_execution_time

PATTERN_POS_1 = re.compile(r'^(9{4,}(\.0+)?|9{1,3}\.9{3,}0*)$')
PATTERN_NEG_1 = re.compile(r'^-(9{4,}(\.0+)?|9{1,3}\.9{3,}0*)$')

PATTERN_POS_2 = re.compile(r'^((\d)\2{3,}(\.0+)?|(\d)\4{0,2}\.\4{3,}0*)$')
PATTERN_NEG_2 = re.compile(r'^-((\d)\2{3,}(\.0+)?|(\d)\4{0,2}\.\4{3,}0*)$')

PATTERN_3 = re.compile(r'^(-+|0+(\d)\2{3,}(\.0+)?|(.)\3{4,}0*)$')


@log_execution_time()
def filter_method(df: DataFrame, col_name, idxs, cond_gen_func, return_vals):
    if len(idxs) == 0:
        return

    target_data = df.loc[idxs, col_name].astype(str)
    if len(target_data) == 0:
        return

    conditions = cond_gen_func(target_data)
    df.loc[target_data.index, col_name] = np.select(conditions, return_vals, df.loc[target_data.index, col_name])


@log_execution_time()
def validate_numeric_minus(df: DataFrame, col_name, return_vals):
    num = 0
    if df[col_name].count() == 0:
        return

    min_val = df[col_name].min()
    if min_val >= num:
        return

        # return_vals = [pd.NA, pd.NA]
    # idxs = df.eval(f'{col_name} < {num}')
    idxs = pd.eval(f'df["{col_name}"] < {num}')
    filter_method(df, col_name, idxs, gen_neg_conditions, return_vals)


@log_execution_time()
def validate_numeric_plus(df: DataFrame, col_name, return_vals):
    num = 0
    if df[col_name].count() == 0:
        return

    max_val = df[col_name].max()
    if max_val < num:
        return

        # return_vals = [pd.NA, pd.NA, pd.NA]
    idxs = pd.eval(f'df["{col_name}"] >= {num}')
    filter_method(df, col_name, idxs, gen_pos_conditions, return_vals)


@log_execution_time()
def validate_string(df: DataFrame, col_name):
    if df[col_name].count() == 0:
        return df

    target_data = df[col_name].astype(str)
    if len(target_data) == 0:
        return df

    conditions = gen_all_conditions(target_data)
    return_vals = ['inf', '-inf', 'inf', '-inf', pd.NA]
    df.loc[target_data.index, col_name] = np.select(conditions, return_vals, df.loc[target_data.index, col_name])

    return df


def gen_pos_conditions(df_str: DataFrame):
    return [
        df_str.str.contains(PATTERN_POS_1),
        df_str.str.contains(PATTERN_POS_2),
        df_str.str.contains(PATTERN_3),
    ]


def gen_neg_conditions(df_str: DataFrame):
    return [df_str.str.contains(PATTERN_NEG_1), df_str.str.contains(PATTERN_NEG_2)]


def gen_all_conditions(df_str: DataFrame):
    return [
        df_str.str.contains(PATTERN_POS_1),
        df_str.str.contains(PATTERN_NEG_1),
        df_str.str.contains(PATTERN_POS_2),
        df_str.str.contains(PATTERN_NEG_2),
        df_str.str.contains(PATTERN_3),
    ]


# def chk_regex(val, re_pattern, return_val):
#     return return_val if pd.isna(val) or bool(re_pattern.match(str(val))) else val
#
#
# def unique_chk_regex(val, re_pattern):
#     return val if not pd.isna(val) and bool(re_pattern.match(str(val))) else np.nan
#
#
# def should_unique_method(df: DataFrame, col_name):
#     total = df[col_name].size
#     if total < 10000:
#         return False, None
#
#     unique_vals = df[col_name].unique()
#     unique_vals = list(unique_vals)
#     return total / len(unique_vals) > 3, unique_vals
#
#
# def unique_method(df: DataFrame, col_name, re_pattern, unique_vals, return_val):
#     vec_func = np.vectorize(unique_chk_regex)
#     vals = vec_func(unique_vals, re_pattern)
#     vals = set(vals) - {None, np.nan, pd.NA}
#
#     if len(vals):
#         df.loc[df[col_name].isin(vals), col_name] = return_val
#
#     return df
#
#
