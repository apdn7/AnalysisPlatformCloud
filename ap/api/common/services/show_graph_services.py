import contextlib
import itertools
import json
from collections import defaultdict
from copy import deepcopy
from datetime import datetime
from math import ceil
from typing import Any, Dict, List, Optional, Set, Tuple

import numpy as np
import pandas as pd
from numpy import quantile
from pandas import DataFrame, Series
from pandas.core.dtypes.common import is_datetime64_ns_dtype

from ap import TraceErrKey, dic_request_info
from ap.api.common.services.show_graph_database import DictToClass, ShowGraphConfigData, gen_dict_procs
from ap.api.common.services.utils import TraceGraph, get_col_cfg
from ap.api.external_api.services import save_odf_data_of_request
from ap.api.trace_data.services.regex_infinity import (
    check_validate_target_column,
    get_changed_value_after_validate,
    validate_data_with_regex,
    validate_data_with_simple_searching,
)
from ap.common.common_utils import (
    DATE_FORMAT_STR_POSTGRES,
    add_days,
    add_zero_padding_df,
    add_zero_padding_series,
    as_list,
    convert_time,
    convert_to_d3_format,
    end_of_minute,
    gen_abbr_name,
    gen_end_proc_start_end_time,
    gen_sql_label,
    gen_sql_like_value,
    get_debug_data,
    get_format_padding,
    start_of_minute,
)
from ap.common.constants import (
    ACT_FROM,
    ACT_TO,
    ACTUAL_RECORD_NUMBER,
    ARRAY_FORMVAL,
    ARRAY_MASTER_IDS,
    ARRAY_PLOTDATA,
    ARRAY_X,
    ARRAY_Y,
    ARRAY_Y_MAX,
    ARRAY_Y_MIN,
    ARRAY_Y_TYPE,
    AVAILABLE_ORDERS,
    CAT_DISTRIBUTE,
    CAT_EXP_BOX,
    CAT_EXP_BOX_NAME,
    CAT_ON_DEMAND,
    CAT_SUMMARY,
    CAT_TOTAL,
    CATE_PROCS,
    CATEGORIZED_SUFFIX,
    CATEGORY_DATA,
    CATEGORY_MASTER_IDS,
    CHART_INFOS,
    CHART_INFOS_ORG,
    COL_DATA_TYPE,
    COL_ID,
    COL_NAME,
    COLOR_ORDER,
    COMMON,
    COMMON_INFO,
    COND_PROCS,
    CYCLE_IDS,
    DATA_GROUP_TYPE,
    DATA_SIZE,
    DATETIME_COL,
    DF_ALL_COLUMNS,
    DIC_CAT_FILTERS,
    DIC_STR_COLS,
    DIV_BY_CAT,
    END_COL,
    END_COL_FORMAT,
    END_COL_ID,
    END_COL_NAME,
    END_COL_SHOW_NAME,
    END_PROC,
    END_PROC_ID,
    END_PROC_NAME,
    END_TIME_COL,
    FACET_ROW,
    FILTER_DATA,
    FILTER_ON_DEMAND,
    GET02_CATE_SELECT,
    GET02_VALS_SELECT,
    ID,
    INF_IDXS,
    IS_CAT_LIMITED,
    IS_CATEGORY,
    IS_GRAPH_LIMITED,
    IS_MASTER_COL,
    KDE_DATA,
    LAST_REQUEST_TIME,
    LOWER_OUTLIER_IDXS,
    MASTER_INFO,
    MATCHED_FILTER_IDS,
    MATRIX_COL,
    MAX_CATEGORY_SHOW,
    MAX_GRAPH_COUNT,
    N_NA,
    N_NA_PCTG,
    N_PCTG,
    N_TOTAL,
    NA_STR,
    NEG_INF_IDXS,
    NO_FILTER,
    NONE_IDXS,
    NOT_EXACT_MATCH_FILTER_IDS,
    ORG_NONE_IDXS,
    PRC_MAX,
    PRC_MIN,
    RANK_COL,
    RANK_COLS_TO_MASTER_IDS,
    RL_CATEGORY,
    RL_HIST_COUNTS,
    RL_HIST_LABELS,
    ROWID,
    SCALE_AUTO,
    SCALE_COMMON,
    SCALE_FULL,
    SCALE_SETTING,
    SCALE_THRESHOLD,
    SELECT_ALL,
    SENSORS,
    SERIAL_COLUMN,
    SERIAL_COLUMNS,
    SERIAL_DATA,
    SERIAL_ORDER,
    SERIAL_PROCESS,
    SHOW_GRAPH_TEMP_TABLE_NAME,
    START_TIME_COL,
    STRING_COL_IDS,
    SUMMARIES,
    TEMP_CAT_EXP,
    TEMP_CAT_PROCS,
    TEMP_SERIAL_COLUMN,
    TEMP_SERIAL_ORDER,
    TEMP_SERIAL_PROCESS,
    TEMP_X_OPTION,
    THIN_DATA_CHUNK,
    THIN_DATA_COUNT,
    THRESH_HIGH,
    THRESH_LOW,
    TIME_COL,
    TIMES,
    UNIQUE_CATEGORIES,
    UNIQUE_COLOR,
    UNIQUE_DIV,
    UNIQUE_SERIAL,
    UNIT,
    UNLINKED_IDXS,
    UNMATCHED_FILTER_IDS,
    UPPER_OUTLIER_IDXS,
    X_ID,
    X_IS_MASTER,
    X_LABEL,
    X_OPTION,
    Y_ID,
    Y_IS_MASTER,
    Y_LABEL,
    Y_MAX,
    Y_MIN,
    CacheType,
    ColorOrder,
    DataGroupType,
    DataType,
    DebugKey,
    DuplicateSerialCount,
    DuplicateSerialShow,
    FilterFunc,
    N,
    Operator,
    RemoveOutlierType,
    YType,
)
from ap.common.logger import log_exec_time_inside_func, log_execution_time
from ap.common.memoize import memoize
from ap.common.pydn.dblib.db_common import PARAM_SYMBOL
from ap.common.services.ana_inf_data import calculate_kde_trace_data, detect_abnormal_count_values
from ap.common.services.form_env import bind_dic_param_to_class
from ap.common.services.request_time_out_handler import abort_process_handler
from ap.common.services.sse import MessageAnnouncer
from ap.common.services.statistics import convert_series_to_number, get_mode
from ap.common.sigificant_digit import get_fmt_from_array, signify_digit
from ap.common.trace_data_log import EventAction, Target, save_df_to_file, trace_log
from ap.equations.utils import get_function_class_by_id

# TODO: filter check
from ap.setting_module.models import (
    CfgProcess,
    CfgProcessColumn,
    CfgProcessFunctionColumn,
    CfgTrace,
    DataTraceLog,
    insert_or_update_config,
    make_session,
)
from ap.setting_module.schemas import TraceSchema
from ap.trace_data.schemas import CategoryProc, ConditionProc, DicParam, EndProc
from bridge.models.bridge_station import BridgeStationModel
from bridge.models.transaction_model import TransactionData
from bridge.services.master_catalog import MasterColumnMetaCatalog
from bridge.services.sql.sql_generator import (
    SQL_GENERATOR_PREFIX,
    SqlProcLink,
    SqlProcLinkKey,
    gen_cte_show_category,
    gen_id_stmt,
    gen_show_stmt,
    gen_tracing_cte,
    gen_tracing_cte_with_delta_time_cut_off,
)
from bridge.services.sql.utils import gen_sql_and_params
from grpc_server.services.grpc_service_proxy import grpc_api


@log_execution_time()
def gen_dic_data(
    dic_proc_cfgs: Dict[int, CfgProcess],
    df,
    orig_graph_param,
    graph_param_with_cate,
    max_graph=None,
):
    # create output data
    cat_exp_cols = orig_graph_param.common.cat_exp
    is_graph_limited = False
    if cat_exp_cols:
        _, dic_cfg_cat_exps = get_cfg_proc_col_info(dic_proc_cfgs, cat_exp_cols)
        # dic_cfg_cat_exps = {
        #     cfg_col.id: cfg_col for cfg_col in CfgProcessColumn.get_by_ids(cat_exp_cols)
        # }
        dic_data, is_graph_limited = gen_dic_data_cat_exp_from_df(
            dic_proc_cfgs,
            df,
            orig_graph_param,
            dic_cfg_cat_exps,
            max_graph,
        )
        dic_cates = defaultdict(dict)
        for proc in orig_graph_param.common.cate_procs:
            for col_id, col_name in zip(proc.col_ids, proc.col_names):
                sql_label = gen_sql_label(col_id, col_name)
                assert sql_label in df.columns, f'{sql_label} must exist in df.columns'
                dic_cates[proc.proc_id][col_id] = df[sql_label] if sql_label in df.columns else pd.Series()

        dic_data[CATEGORY_DATA] = dic_cates
    else:
        dic_data = gen_dic_data_from_df(df, graph_param_with_cate, cat_exp_mode=True)

    return dic_data, is_graph_limited


@log_execution_time()
@abort_process_handler()
def gen_dic_data_from_df(
    df: DataFrame,
    graph_param: DicParam,
    cat_exp_mode=None,
    dic_cat_exp_labels=None,
):
    """
    :param df:
    :param graph_param:
    :param cat_exp_mode:
    :param dic_cat_exp_labels:
    :return:
    """
    # convert to python datatype
    # df = deepcopy(df)
    # df = df_json_dumps(df)

    dic_data = defaultdict(dict)
    blank_vals = pd.Series([None] * df.index.size)
    for proc in graph_param.array_formval:
        dic_data_cat_exp = defaultdict(list)
        dic_data_none_idxs = defaultdict(list)
        for col_id, col_name in zip(proc.col_ids, proc.col_names):
            col_id_name = gen_sql_label(col_id, col_name)
            sql_labels = [col for col in df.columns if col.startswith(col_id_name)]
            series_lst = []
            for sql_label in sql_labels:
                series = df[sql_label] if sql_label in df.columns else blank_vals

                series_lst.append(series)
                if dic_cat_exp_labels:
                    sql_label_vals = dic_cat_exp_labels.get(sql_label)
                    if sql_label_vals:
                        dic_data_cat_exp[col_id].append(sql_label_vals[2])
                        dic_data_none_idxs[col_id].append(sql_label_vals[3])

            if len(series_lst):
                dic_data[proc.proc_id][col_id] = series_lst if cat_exp_mode else series_lst[0]
            else:
                dic_data[proc.proc_id][col_id] = []

        if len(dic_data_cat_exp):
            dic_data[proc.proc_id][CAT_EXP_BOX] = dic_data_cat_exp
            dic_data[proc.proc_id][NONE_IDXS] = dic_data_none_idxs

        time_col_alias = '{}_{}'.format(TIME_COL, proc.proc_id)
        if time_col_alias in df:
            dic_data[proc.proc_id][TIME_COL] = df[time_col_alias]
        else:
            dic_data[proc.proc_id][TIME_COL] = pd.Series()

        # if CAT_EXP_BOX in df.columns:
        #     dic_data[CAT_EXP_BOX] = df[CAT_EXP_BOX].tolist()

    return dic_data


@log_execution_time()
def gen_dic_data_cat_exp_from_df(
    dic_proc_cfgs: Dict[int, CfgProcess],
    df: DataFrame,
    graph_param: DicParam,
    dic_cfg_cat_exps,
    max_graph=None,
):
    is_graph_limited = False
    dic_data = defaultdict(dict)
    if not len(df):
        return dic_data, is_graph_limited

    cat_exp_cols = gen_cat_exp_names(graph_param.common.cat_exp)
    for cat_exp_col, cat_exp_label in zip(graph_param.common.cat_exp, cat_exp_cols):
        if cat_exp_label not in df.columns:
            cfg_cat_exp: CfgProcessColumn = dic_cfg_cat_exps[cat_exp_col]
            sql_label = gen_sql_label(cfg_cat_exp.id, cfg_cat_exp.column_name)
            df[cat_exp_label] = df[sql_label]

    df_group = df.groupby(cat_exp_cols, dropna=False)
    dic_df_group = {}
    for key, df_sub in df_group:
        dic_df_group[key] = df_sub
    # dic_df_group = {key: df_sub for key, df_sub in df_group}
    # dic_df_group = dict(df_group) # error

    blank_vals = [None] * len(df)
    graph_count = 0
    for proc in graph_param.array_formval:
        if max_graph and graph_count >= max_graph:
            is_graph_limited = True
            break

        datetime_cols = [dic_proc_cfgs[proc.proc_id].get_cols_by_data_type(DataType.DATETIME, column_name_only=False)]
        dic_none_idxs = defaultdict(list)
        dic_cat_exp_names = defaultdict(list)
        time_col_alias = '{}_{}'.format(TIME_COL, proc.proc_id)
        dic_data[proc.proc_id][TIME_COL] = df.get(time_col_alias, pd.Series())

        for col_id, col_name in zip(proc.col_ids, proc.col_names):
            if max_graph and graph_count >= max_graph:
                is_graph_limited = True
                break

            sql_label = gen_sql_label(col_id, col_name)
            if sql_label not in df.columns:
                dic_data[proc.proc_id][col_id] = blank_vals
                dic_none_idxs[col_id].append(list(range(len(df))))
                dic_cat_exp_names[col_id].append(NA_STR)
                continue

            plots = []
            # for cat_exp_val, idxs in df_group.groups.items():
            for cat_exp_val, df_sub in dic_df_group.items():
                if graph_count >= MAX_GRAPH_COUNT:
                    break

                idxs = df_sub.index
                if not len(idxs):
                    continue

                series = pd.Series(blank_vals, index=df.index)
                temp_series: Series = df_sub[sql_label]
                if col_id in datetime_cols:
                    temp_series = pd.to_datetime(temp_series)
                    temp_series.sort_values(inplace=True)
                    temp_series = temp_series.diff().dt.total_seconds()
                    temp_series.sort_index(inplace=True)

                nan_idxs = temp_series.isnull()
                nan_series = temp_series[nan_idxs]
                # for show empty facet group -> comment
                # if len(temp_series) == len(nan_series):
                #     continue

                series[idxs] = temp_series
                if len(nan_series):
                    series[nan_series.index] = None

                plots.append(series)
                dic_none_idxs[col_id].append(nan_series.index.tolist())
                dic_cat_exp_names[col_id].append(NA_STR if cat_exp_val is None or pd.isna(cat_exp_val) else cat_exp_val)

                graph_count += 1

            if plots:
                dic_data[proc.proc_id][col_id] = plots
                dic_data[proc.proc_id][CAT_EXP_BOX] = dic_cat_exp_names
                dic_data[proc.proc_id][NONE_IDXS] = dic_none_idxs

    return dic_data, is_graph_limited


@log_execution_time()
def gen_cat_exp_names(cat_exps):
    if cat_exps:
        return [gen_sql_label(CAT_EXP_BOX, level) for level, _ in enumerate(cat_exps, 1)]

    return None


def gen_group_filter_list(df, graph_param, dic_param, others=[]):
    limit = 100_000
    if df.empty:
        return dic_param
    cate_col_ids = []
    for proc in graph_param.common.cate_procs or []:
        cate_col_ids += proc.col_ids

    filter_cols = sorted(set(cate_col_ids + graph_param.common.cat_exp + others))
    filter_sensors = graph_param.get_col_cfgs(filter_cols)

    filter_labels = []
    actual_filter_cols = []
    for col in filter_sensors:
        sql_label = gen_sql_label(RANK_COL, col.id, col.column_name)
        if sql_label not in df.columns:
            sql_label = gen_sql_label(col.id, col.column_name)
        if sql_label in df.columns and df[sql_label].value_counts().size <= limit:
            filter_labels.append(sql_label)
            actual_filter_cols.append(col.id)

    if len(filter_labels) <= 1:
        return dic_param

    group_list = list(df.groupby(filter_labels).groups.keys())

    sorted_filter_cols = [col.id for col in filter_sensors if col.id in actual_filter_cols]

    group_df = pd.DataFrame(columns=sorted_filter_cols, data=group_list)

    dic_filter = {}
    for col in sorted_filter_cols:
        other_cols = list(sorted_filter_cols)
        other_cols.remove(col)

        dic_filter[col] = {
            vals[0]: dict(zip(other_cols, vals[1:])) for vals in group_df.groupby(col).agg(set).to_records().tolist()
        }

    dic_param['dic_filter'] = dic_filter

    return dic_param


def customize_dic_param_for_reuse_cache(dic_param):
    use_expired_cache = False
    for name in (
        DIC_CAT_FILTERS,
        TEMP_CAT_EXP,
        TEMP_CAT_PROCS,
        TEMP_X_OPTION,
        TEMP_SERIAL_PROCESS,
        TEMP_SERIAL_COLUMN,
        TEMP_SERIAL_ORDER,
        MATRIX_COL,
        COLOR_ORDER,
    ):
        if name in dic_param[COMMON]:
            use_expired_cache = True
            break
    dic_cat_filters = (
        json.loads(dic_param[COMMON].get(DIC_CAT_FILTERS, {}))
        if isinstance(dic_param[COMMON].get(DIC_CAT_FILTERS, {}), str)
        else dic_param[COMMON].get(DIC_CAT_FILTERS, {})
    )
    cat_exp = [int(id) for id in dic_param[COMMON].get(TEMP_CAT_EXP, []) if id]
    cat_procs = dic_param[COMMON].get(TEMP_CAT_PROCS, [])
    for name in (DIC_CAT_FILTERS, TEMP_CAT_EXP, TEMP_CAT_PROCS):
        if name in dic_param[COMMON]:
            dic_param[COMMON].pop(name)
    (
        dic_param,
        temp_x_option,
        temp_serial_process,
        temp_serial_column,
        temp_serial_order,
    ) = prepare_temp_x_option(dic_param)

    matrix_col = dic_param[COMMON].get(MATRIX_COL)
    if matrix_col and isinstance(matrix_col, (list, tuple)):
        matrix_col = matrix_col[0]

    if matrix_col:
        matrix_col = int(matrix_col)

    # set default for color order ( default : data value )
    color_order = dic_param[COMMON].get(COLOR_ORDER)
    color_order = ColorOrder(int(color_order)) if color_order else ColorOrder.DATA

    return (
        dic_param,
        cat_exp,
        cat_procs,
        dic_cat_filters,
        use_expired_cache,
        temp_serial_column,
        temp_serial_order,
        temp_serial_process,
        temp_x_option,
        matrix_col,
        color_order,
    )


@log_execution_time()
def filter_cat_dict_common(
    df,
    dic_param,
    cat_exp,
    cat_procs,
    graph_param,
    has_na=False,
    label_col_id=None,
):
    """
        filter cat_exp_box function common
    :param label_col_id:
    :param df:
    :param dic_param;
    :param cat_exp:
    :param cat_procs:
    :param graph_param:
    :param has_na:
    :return:
    """

    if COMMON in dic_param:
        if cat_exp:
            for i, val in enumerate(cat_exp):
                cat_label = f'{CAT_EXP_BOX}{i + 1}'
                if cat_label in dic_param[COMMON]:
                    dic_param[COMMON][cat_label] = val
        if cat_procs:
            dic_param[COMMON][CATE_PROCS] = cat_procs

    dic_param = gen_cat_label_unique(df, dic_param, graph_param, has_na, label_col_id)

    return dic_param


@log_execution_time()
def gen_cat_label_unique(
    df,
    dic_param,
    graph_param,
    has_na=False,
    label_col_id=None,
):
    cat_and_div = []
    if DIV_BY_CAT in dic_param[COMMON] and dic_param[COMMON][DIV_BY_CAT]:
        cat_and_div.append(int(dic_param[COMMON][DIV_BY_CAT]))

    dic_proc_cfgs = graph_param.dic_proc_cfgs

    cat_and_div += graph_param.common.cat_exp
    cat_exp_list = gen_unique_data(df, dic_proc_cfgs, cat_and_div, has_na)
    dic_param[CAT_EXP_BOX] = list(cat_exp_list.values())

    color_vals = []
    if graph_param.common.color_var:
        color_vals.append(graph_param.common.color_var)
    if graph_param.common.agp_color_vars:
        agp_color_vals = list({int(color) for color in graph_param.common.agp_color_vars.values()})
        color_vals += agp_color_vals

    exclude_col_id = cat_and_div
    exclude_col_id += label_col_id or graph_param.common.cate_col_ids

    exclude_col_id += color_vals

    sensor_ids = [col for col in graph_param.common.sensor_cols if col not in exclude_col_id]
    dic_param[CAT_ON_DEMAND] = list(gen_unique_data(df, dic_proc_cfgs, sensor_ids, True).values())
    dic_param[UNIQUE_COLOR] = list(gen_unique_data(df, dic_proc_cfgs, color_vals, False).values())
    dic_param[FILTER_DATA] = list(gen_unique_data(df, dic_proc_cfgs, graph_param.common.cate_col_ids, True).values())

    dic_param = gen_group_filter_list(df, graph_param, dic_param)

    return dic_param


def prepare_temp_x_option(dic_param):
    params = [TEMP_X_OPTION, TEMP_SERIAL_PROCESS, TEMP_SERIAL_COLUMN, TEMP_SERIAL_ORDER]
    temp_x_option = dic_param[COMMON].get(TEMP_X_OPTION, '')
    temp_serial_process = as_list(dic_param[COMMON].get(TEMP_SERIAL_PROCESS))
    temp_serial_column = as_list(dic_param[COMMON].get(TEMP_SERIAL_COLUMN))
    temp_serial_order = as_list(dic_param[COMMON].get(TEMP_SERIAL_ORDER))

    for param in params:
        if param in dic_param[COMMON]:
            dic_param[COMMON].pop(param)

    return dic_param, temp_x_option, temp_serial_process, temp_serial_column, temp_serial_order


@log_execution_time()
@abort_process_handler()
def gen_df(root_graph_param, dic_param, dic_filter=None, add_dt_col=False, rank_value=False, use_expired_cache=False):
    """tracing data to show graph
    1 start point x n end point
    filter by condition point
    """
    graph_param_with_cate = bind_dic_param_to_class(
        root_graph_param.dic_proc_cfgs,
        root_graph_param.trace_graph,
        root_graph_param.dic_card_orders,
        dic_param,
    )
    graph_param_with_cate.add_cate_procs_to_array_formval()

    graph_param = bind_dic_param_to_class(
        root_graph_param.dic_proc_cfgs,
        root_graph_param.trace_graph,
        root_graph_param.dic_card_orders,
        dic_param,
    )

    dic_proc_cfgs = root_graph_param.dic_proc_cfgs

    if add_dt_col:
        for proc in graph_param.array_formval:
            get_date = dic_proc_cfgs[proc.proc_id].get_date_col(column_name_only=False).id
            proc.add_cols(get_date, append_first=True)

    # get order columns
    if graph_param.common.x_option == 'INDEX':
        for proc_id, col_id in zip(graph_param.common.serial_processes, graph_param.common.serial_columns):
            if proc_id and col_id:
                graph_param.add_proc_to_array_formval(int(proc_id), int(col_id))

    # get data from database
    df, actual_record_number, unique_serial = get_data_from_db(
        graph_param,
        dic_filter,
        use_expired_cache=use_expired_cache,
    )

    # rank categorical cols
    if rank_value:
        df, dic_param = rank_categorical_cols(df, root_graph_param, dic_param)

    # check filter match or not ( for GUI show )
    (
        matched_filter_ids,
        unmatched_filter_ids,
        not_exact_match_filter_ids,
    ) = main_check_filter_detail_match_graph_data(root_graph_param, df)

    # matched_filter_ids, unmatched_filter_ids, not_exact_match_filter_ids
    dic_param[MATCHED_FILTER_IDS] = matched_filter_ids
    dic_param[UNMATCHED_FILTER_IDS] = unmatched_filter_ids
    dic_param[NOT_EXACT_MATCH_FILTER_IDS] = not_exact_match_filter_ids

    # flag to show that trace result was limited
    dic_param[DATA_SIZE] = int(df.memory_usage(deep=True).sum())
    dic_param[UNIQUE_SERIAL] = unique_serial
    dic_param[ACTUAL_RECORD_NUMBER] = actual_record_number

    return dic_param, df, graph_param_with_cate


@log_execution_time()
def rank_categorical_cols(df, graph_param, dic_param):
    # string columns and int <= 128
    df, str_cols, dic_str_cols = rank_str_cols(df, graph_param)
    dic_param[STRING_COL_IDS] = str_cols
    dic_param[DIC_STR_COLS] = dic_str_cols
    return df, dic_param


@log_execution_time()
@abort_process_handler()
def gen_dic_param(
    graph_param,
    df,
    dic_param,
    dic_data,
    dic_cates=None,
    dic_org_cates=None,
    is_get_chart_infos=True,
):
    if df is None or not len(df):
        dic_param[ARRAY_PLOTDATA] = []
        return dic_param

    # get chart infos
    chart_infos = None
    original_graph_configs = None
    if TIME_COL in df.columns:
        times = df[TIME_COL]

        if is_get_chart_infos:
            chart_infos, original_graph_configs = get_chart_infos(graph_param, dic_data, times)

        dic_param[TIMES] = times

    dic_param[ARRAY_FORMVAL], dic_param[ARRAY_PLOTDATA] = gen_plotdata_fpp(
        graph_param,
        dic_data,
        chart_infos,
        original_graph_configs,
    )
    dic_param[CATEGORY_DATA] = gen_category_data(graph_param, dic_cates or dic_data, dic_org_cates)

    dic_param[CYCLE_IDS] = []
    if ROWID in df.columns:
        dic_param[CYCLE_IDS] = df[ROWID]

    return dic_param


@log_execution_time()
def rank_str_cols(df: DataFrame, graph_param: DicParam):
    dic_str_cols = get_str_cols_in_end_procs(graph_param, df)
    str_cols = []
    sensors = graph_param.common.sensor_cols

    for sql_label, (before_rank_label, _, col_id, _, _) in dic_str_cols.items():
        if sql_label not in df.columns:
            continue

        if str(col_id) == str(graph_param.common.judge_var) and col_id in sensors:
            # if sensor is judge_var but not target sensor
            continue

        df[before_rank_label] = df[sql_label]
        df[sql_label] = pd.Series(
            np.where(df[sql_label].isnull(), df[sql_label], df[sql_label].astype('category').cat.codes + 1),
            dtype=pd.Int32Dtype.name,
        )

        df[sql_label] = df[sql_label].convert_dtypes()
        str_cols.append(col_id)

    return df, str_cols, dic_str_cols


@log_execution_time()
def get_str_cols_in_end_procs(graph_param: DicParam, df=None):
    dic_output = {}
    for proc in graph_param.array_formval:
        proc_cfg = graph_param.dic_proc_cfgs[proc.proc_id]
        for col_id, col_name, show_name in zip(proc.col_ids, proc.col_names, proc.col_show_names):
            cfg_col = proc_cfg.get_col(col_id)

            if cfg_col is None or cfg_col.data_type == DataType.REAL.value:
                continue

            if not is_categorical_col(cfg_col):
                continue

            rank_col_name = gen_sql_label(col_id, col_name)
            before_rank_col_name = gen_sql_label(RANK_COL, rank_col_name)
            dic_output[rank_col_name] = (
                before_rank_col_name,
                proc.proc_id,
                col_id,
                col_name,
                show_name,
            )

    return dic_output


@log_execution_time()
def is_categorical_col(col_cfg):
    return col_cfg.is_category or col_cfg.data_type in [DataType.DATE.value, DataType.TIME.value, DataType.TEXT.value]


@log_execution_time()
def gen_before_rank_dict(df: DataFrame, dic_str_cols):
    dic_output = {}
    for sql_label, (before_rank_label, _, col_id, _, _) in dic_str_cols.items():
        if before_rank_label in df.columns:
            df_rank = df[[sql_label, before_rank_label]].drop_duplicates().dropna()
            dic_output[col_id] = dict(zip(df_rank[sql_label], df_rank[before_rank_label].astype(str)))

    return dic_output


@log_execution_time()
def set_str_rank_to_dic_param(dic_param, dic_ranks, dic_str_cols, dic_full_array_y=None, is_stp=False):
    plotdata = dic_param[ARRAY_PLOTDATA]
    org_ranks = get_org_rank(dic_ranks, dic_str_cols)
    if not is_stp or isinstance(plotdata, list):
        gen_ranking_to_dic_param(plotdata, dic_full_array_y, dic_ranks, org_ranks, is_stp)
    else:
        for sensor_id, sensor_dat in plotdata.items():
            gen_ranking_to_dic_param(sensor_dat, dic_full_array_y, dic_ranks, org_ranks, is_stp)


@log_execution_time()
def get_org_rank(dic_ranks, dic_str_cols):
    origin_ranks = {}
    for i, (_, _, col_id, col_name, show_name) in dic_str_cols.items():
        if col_id in dic_ranks:
            ranks = {}
            # convert key to string
            for r in dic_ranks[col_id]:
                ranks[str(r)] = dic_ranks[col_id][r]
            origin_ranks[show_name] = ranks
    return origin_ranks


@log_execution_time()
def cat_exp_box_to_org_name(plot, org_rank, is_stp=False):
    if CAT_EXP_BOX in plot:
        cat_exp_box = plot[CAT_EXP_BOX]
        if isinstance(cat_exp_box, tuple):
            cat_exp_box = list(cat_exp_box)
        if isinstance(cat_exp_box, int):
            cat_exp_box = [cat_exp_box]
        if is_stp and isinstance(cat_exp_box, str):
            cat_exp_box = list(cat_exp_box.split(' | '))

        if not isinstance(cat_exp_box, list):
            cat_exp_box = [cat_exp_box]

        for i, name in enumerate(plot[CAT_EXP_BOX_NAME]):
            cat_box_id = str(cat_exp_box[i])
            if name in org_rank and cat_box_id in org_rank[name]:
                cat_exp_box[i] = org_rank[name][cat_box_id]

        #  reassign cat_exp_box
        if is_stp:
            if isinstance(cat_exp_box, list):
                cat_exp_box = [str(cat_box) for cat_box in cat_exp_box]
                cat_exp_box = ' | '.join(cat_exp_box)
        else:
            cat_exp_box = tuple(cat_exp_box)
        plot[CAT_EXP_BOX] = cat_exp_box


@log_execution_time()
def gen_ranking_to_dic_param(sensor_dat, dic_full_array_y, dic_ranks, org_ranks, is_stp=False):
    for i, plot in enumerate(sensor_dat):
        cat_exp_box_to_org_name(plot, org_ranks, is_stp)

        col_id = plot.get(END_COL_ID) if not is_stp else plot.get(END_COL)
        dic_col_ranks = dic_ranks.get(col_id)
        if not dic_col_ranks:
            continue

        if is_stp:
            # category variable
            p_array_y = pd.Series(plot[ARRAY_Y]).dropna()
            cat_size = 0
            if len(p_array_y):
                cat_size = np.unique(p_array_y).size
            plot[CAT_TOTAL] = cat_size
            plot[IS_CAT_LIMITED] = len(dic_col_ranks.keys()) >= MAX_CATEGORY_SHOW

        # enc col (show graph)
        # plot[ARRAY_Y] = reduce_stepped_chart_data(plot.get(ARRAY_Y))
        plot[ARRAY_Y_MIN] = None
        plot[ARRAY_Y_MAX] = None

        category_distributed = {}
        full_dat = dic_full_array_y[i] if dic_full_array_y else plot.get(ARRAY_Y)
        none_idxs = plot.get(ORG_NONE_IDXS)
        total_counts = 0

        dic_cat_counter = pd.value_counts(full_dat, dropna=False).to_dict()
        dic_cat_counter = {key: dic_cat_counter[key] for key in list(dic_cat_counter.keys())[:MAX_CATEGORY_SHOW]}
        ranks = []
        before_ranks = []
        for rank_val, cat_count in dic_cat_counter.items():
            if rank_val is None:
                continue

            cat_name = dic_col_ranks.get(rank_val)
            if cat_name is None:
                continue

            ranks.append(rank_val)
            before_ranks.append(cat_name)

            short_name = gen_abbr_name(cat_name)
            category_distributed[cat_name] = {
                'counts': cat_count,
                'short_name': short_name,
                'counts_org': cat_count,
                'pctg': 0,
            }
            total_counts += cat_count

        plot[RANK_COL] = [ranks, before_ranks]

        for k, cat in category_distributed.items():
            cat_dist = signify_digit(cat['counts'] * 100 / total_counts) if total_counts else 0
            category_distributed[k]['pctg'] = cat_dist
            category_distributed[k]['counts'] = '{} ({}%)'.format(cat['counts'], cat_dist)

        # show end col summary info
        series = pd.Series(full_dat)
        if none_idxs is None:
            pass
        elif none_idxs:
            series = series[(series.notnull()) | (series.index.isin(none_idxs))]
        else:
            series.dropna(inplace=True)

        n_total = len(series)
        non_na_count = len(series.dropna())
        na_count = n_total - non_na_count

        step_chart_summary = {
            N_TOTAL: n_total,
            N: non_na_count,
            N_PCTG: signify_digit(100 * non_na_count / n_total) if n_total else 0,
            N_NA: na_count,
            N_NA_PCTG: signify_digit(100 * na_count / n_total) if n_total else 0,
        }
        plot[CAT_DISTRIBUTE] = category_distributed
        plot[CAT_SUMMARY] = step_chart_summary
        plot[IS_CATEGORY] = True


@log_execution_time()
def check_and_order_data(
    df,
    dic_proc_cfgs: Dict[int, CfgProcess],
    x_option='TIME',
    serial_processes=[],
    serial_cols=[],
    serial_orders=[],
):
    if df is None or not len(df):
        return df

    if x_option.upper() == 'TIME':
        df = df.sort_values(TIME_COL, ascending=True)
        return df

    cols = []
    orders = []
    for proc_id, col_id, order in zip(serial_processes, serial_cols, serial_orders):
        if not proc_id or not col_id:
            continue

        proc_cfg: CfgProcess = dic_proc_cfgs.get(int(proc_id))
        if not proc_cfg:
            continue

        order_cols: List[CfgProcessColumn] = proc_cfg.get_order_cols(column_name_only=False)
        if not order_cols:
            continue

        dic_order_cols = {col.id: gen_sql_label(col.id, col.column_name) for col in order_cols}

        col_label = dic_order_cols.get(int(col_id))
        if col_label and col_label in df.columns and col_label not in cols:
            cols.append(dic_order_cols.get(int(col_id)))
            orders.append(bool(int(order)))

    if cols:
        df = df.sort_values(cols, ascending=orders)

    return df


def gen_blank_df_end_cols(procs: List[EndProc]):
    params = {}
    for proc in procs:
        params.update({gen_sql_label(col_id, proc.col_names[idx]): [] for idx, col_id in enumerate(proc.col_ids)})
        params.update({'{}{}'.format(TIME_COL, create_rsuffix(proc.proc_id)): []})
    params.update({ID: [], TIME_COL: []})

    df = pd.DataFrame(params)
    df = df.append(pd.Series(), ignore_index=True)
    return df.replace({np.nan: ''})


def create_rsuffix(proc_id):
    return '_{}'.format(proc_id)


@log_execution_time()
@MessageAnnouncer.notify_progress(30)
@memoize(is_save_file=True, cache_type=CacheType.TRANSACTION_DATA)
def gen_graph_df(
    dic_edges,
    start_tm,
    end_tm,
    end_procs: list[EndProc],
    cond_procs: list[ConditionProc],
    common_paths,
    short_procs,
    duplicate_serial_show: DuplicateSerialShow = None,
    duplicated_serials_count: DuplicateSerialCount = None,
    _use_expired_cache=False,
):
    with BridgeStationModel.get_db_proxy() as db_instance:
        res = gen_trace_procs_df(
            db_instance,
            start_tm,
            end_tm,
            cond_procs,
            end_procs,
            dic_edges,
            common_paths,
            short_procs,
            duplicate_serial_show,
            duplicated_serials_count,
        )

    df, actual_record_number, unique_serial = res
    if df is None:
        return pd.DataFrame(), 0, 0

    # get equation data
    for end_proc in end_procs:
        df = get_equation_data(df, end_proc)

    return df, actual_record_number, unique_serial


@log_execution_time()
def is_show_duplicated_serials(duplicate_serial_show, duplicated_serial_count, actual_total_record):
    for_count = actual_total_record <= THIN_DATA_COUNT

    if duplicated_serial_count == DuplicateSerialCount.CHECK:
        for_count = True
    if duplicated_serial_count == DuplicateSerialCount.SILENT:
        for_count = False

    if duplicate_serial_show == DuplicateSerialShow.SHOW_BOTH:
        duplicate_serial_show = DuplicateSerialShow.SHOW_FIRST
    else:
        duplicate_serial_show = DuplicateSerialShow.SHOW_BOTH

    return duplicate_serial_show, for_count


def gen_enable_postgresql_option(nestloop: bool = True, mergejoin: bool = True, hashjoin: bool = True) -> str:
    return f'''
set enable_nestloop = {str(nestloop).lower()};
set enable_mergejoin = {str(mergejoin).lower()};
set enable_hashjoin = {str(hashjoin).lower()};
    '''


def get_postgresql_options(duplicate_serial_show: DuplicateSerialShow, has_filter: bool):
    """Check whether we should enable nestloop, mergejoin or hashjoin for postgresql
    - Nestloop join should be enabled **only and only if** no temporary table, because
      temporary tables do not have index. Nestloop join running on join query without
      index is really slow. Hence, we disable nestloop join when:
        - We have filter enable (this will create temporary table)
        - We drop duplicates (this will create temporary table)
    - Mergejoin should be enabled when we have already sorted table (in case drop duplicates)
    - Hashjoin should be enabled when we have no sorted table

    **Currently, we do not care about mergejoin and hashjoin, we let database optimizer decide when to use them**
    """

    def should_disable_nest_loop():
        return duplicate_serial_show in [DuplicateSerialShow.SHOW_FIRST, DuplicateSerialShow.SHOW_LAST] or has_filter

    nestloop = True
    mergejoin = True
    hashjoin = True

    if should_disable_nest_loop():
        nestloop = False

    return gen_enable_postgresql_option(nestloop=nestloop, mergejoin=mergejoin, hashjoin=hashjoin)


@log_execution_time()
def use_legacy_way_to_gen_trace_procs_df(
    list_sql_objs: List[List[SqlProcLink]],
    cond_procs: List[ConditionProc],
    duplicate_serial_show: DuplicateSerialShow,
) -> bool:
    if duplicate_serial_show not in (DuplicateSerialShow.SHOW_FIRST, DuplicateSerialShow.SHOW_LAST):
        return False

    cond_proc_ids = {proc.proc_id for proc in cond_procs}
    for sql_objs in list_sql_objs:
        for sql_obj in sql_objs:
            if sql_obj.process_id in cond_proc_ids:
                return True

    return False


def can_calculate_unique_records_by_dataframe(
    duplicate_serial_show: DuplicateSerialShow,
):
    """Determine if we can calculate unique records using dataframe instead of querying to database again
    Since we drop duplicate records directly inside sql, we can only calculate if we don't drop duplicate,
    it means we use SHOW_BOTH
    """
    return duplicate_serial_show == DuplicateSerialShow.SHOW_BOTH


@log_execution_time()
def gen_trace_procs_df_with_legacy_option(
    db_instance,
    list_sql_objs: List[List[SqlProcLink]],
    cond_procs: List[ConditionProc],
    duplicate_serial_show: DuplicateSerialShow,
    legacy: bool = True,
) -> Tuple[DataFrame, int, int]:
    df = None
    total_non_unique_link_keys_records = 0
    total_unique_link_keys_records = 0

    postgres_option = get_postgresql_options(duplicate_serial_show, has_filter=len(cond_procs) > 0)

    for sql_objs in list_sql_objs:
        sql, params = gen_proc_link_from_sql(
            sql_objs,
            cond_procs,
            duplicate_serial_show,
            use_row_number=legacy,
        )
        sql = postgres_option + sql
        # TODO: remove this test_sql after implementing
        # test_params = {k: f"'{v}'" for k, v in params.items()}
        # _test_sql = sql % test_params
        func_log = log_exec_time_inside_func('SQL', 'SHOW GRAPH')
        cols, rows = db_instance.run_sql(sql, params=params, row_is_dict=False)
        func_log()

        _df = pd.DataFrame(rows, columns=cols)
        # datetime from DB is UTC, set UCT for column datetime
        for col in _df.columns:
            if pd.api.types.is_datetime64_any_dtype(_df[col]):
                _df[col] = pd.to_datetime(_df[col], utc=True)

        _df = DropDuplicatesTraceProcs.drop_duplicates_by_ids(_df, sql_objs)

        total_non_unique_link_keys_records += len(_df)
        dropped_duplicates_df = DropDuplicatesTraceProcs.drop_duplicates_by_link_keys(
            _df,
            sql_objs,
            duplicate_serial_show,
        )
        total_unique_link_keys_records += len(dropped_duplicates_df)

        if duplicate_serial_show is not DuplicateSerialShow.SHOW_BOTH:
            _df = dropped_duplicates_df

        if df is None:
            df = _df
            continue
        _df_cols = _df.columns.difference(df.columns).tolist()
        df = df.merge(_df[[TransactionData.id_col_name] + _df_cols], on=TransactionData.id_col_name)

    if df is None:
        return pd.DataFrame(), 0, 0

    return df, total_non_unique_link_keys_records, total_unique_link_keys_records


@log_execution_time()
def gen_trace_procs_df(
    db_instance,
    start_tm,
    end_tm,
    cond_procs: List[ConditionProc],
    end_procs,
    dic_edges,
    common_paths: List[Tuple[List[int], bool]],
    short_procs,
    duplicate_serial_show: DuplicateSerialShow,
    duplicated_serials_count: DuplicateSerialCount,
):
    """Use two different ways to get dataframe from database
    The old (legacy) way: calculating using row_numbers, distinct. Drop them by sql.
    The new way: calculating without using row_numbers, distinct. Drop them by pandas.
    The only way to use legacy is: we have filter enable and we show first/last.
    """
    if not len(common_paths):
        return pd.DataFrame(), 0, 0

    list_sql_objs = []
    time_cols = set()
    for path, is_trace_forward in common_paths:
        sql_objs = gen_trace_procs_sqls(
            path if is_trace_forward else list(reversed(path)),
            dic_edges,
            start_tm,
            end_tm,
            end_procs,
            short_procs,
        )
        list_sql_objs.append(sql_objs)
        time_cols.update(sql_obj.gen_proc_time_label(is_start_proc=idx == 0) for idx, sql_obj in enumerate(sql_objs))

    use_legacy = use_legacy_way_to_gen_trace_procs_df(list_sql_objs, cond_procs, duplicate_serial_show)
    df, non_unique_records_count, unique_records_count = gen_trace_procs_df_with_legacy_option(
        db_instance,
        list_sql_objs,
        cond_procs,
        duplicate_serial_show,
        use_legacy,
    )

    if df.empty:
        return df, 0, 0

    # Sort by time before emitting out df, so the result will be the same with edge server
    df = df.sort_values(sorted(time_cols))

    actual_record_number = len(df)
    # TODO: how to calc duplicate : on start proc, all procs , or end procs ?
    duplicated_option, for_count = is_show_duplicated_serials(
        duplicate_serial_show,
        duplicated_serials_count,
        actual_record_number,
    )
    if not for_count:
        return df, actual_record_number, None

    # We calculated it already
    if can_calculate_unique_records_by_dataframe(duplicate_serial_show):
        actual_record_number = non_unique_records_count
        unique_record_number = unique_records_count
    else:
        unique_record_number = calculate_unique_ids_using_sql(
            db_instance,
            dic_edges,
            start_tm,
            end_tm,
            end_procs,
            short_procs,
            duplicated_option,
            cond_procs,
            common_paths,
        )
        if duplicate_serial_show is not DuplicateSerialShow.SHOW_BOTH:
            actual_record_number, unique_record_number = unique_record_number, actual_record_number

    return df, actual_record_number, unique_record_number


@log_execution_time()
def calculate_unique_ids_using_sql(
    db_instance,
    dic_edges,
    start_tm,
    end_tm,
    end_procs,
    short_procs,
    duplicate_serial_show: DuplicateSerialShow,
    cond_procs: List[ConditionProc],
    common_paths: List[Tuple[List[int], bool]],
) -> int:
    if not cond_procs:
        paths = [common_paths[0]]
    else:
        paths = []
        for cond in cond_procs:
            selected_paths = (path for path in common_paths if cond.proc_id in path[0])
            path = next(selected_paths, None)
            if path is not None:
                paths.append(path)
    unique_ids: Set[int] = set()
    for path, is_trace_forward in paths:
        sql_objs = gen_trace_procs_sqls(
            path if is_trace_forward else list(reversed(path)),
            dic_edges,
            start_tm,
            end_tm,
            end_procs,
            short_procs,
        )
        sql, params = gen_proc_link_from_sql(sql_objs, cond_procs, duplicate_serial_show, for_count=True)
        cols, rows = db_instance.run_sql(sql, params=params, row_is_dict=False)
        unique_ids.update(r[0] for r in rows)
    return len(unique_ids)


@log_execution_time()
def reduce_graph(end_procs, graph, start_proc_id):
    # get all paths between start and end procs
    paths = []
    dic_end_proc_cols = {}
    for end_proc_obj in end_procs:
        end_proc = end_proc_obj.proc_id
        dic_end_proc_cols[end_proc] = (end_proc_obj.col_ids, end_proc_obj.col_names)
        end_paths = []
        # trace forward vs trace backward
        for _start_proc, _end_proc in ((start_proc_id, end_proc), (end_proc, start_proc_id)):
            is_trace_forward = _start_proc == start_proc_id
            _paths = graph.get_all_paths(_start_proc, _end_proc)
            _paths = [(_path, is_trace_forward) for _path in _paths if len(_path) and _path[-1] == _end_proc]
            if _paths:
                end_paths.extend(_paths)
                break

        if not end_paths:
            _paths = graph.get_all_paths(start_proc_id, end_proc, undirected_graph=True)
            _paths = [(_path, True) for _path in _paths if len(_path) and _path[-1] == end_proc]
            if _paths:
                end_paths.extend(_paths)

        paths.extend(end_paths)
    # remove middle nodes
    short_procs = set()
    for path, is_trace_forward in paths:
        short_path = graph.remove_middle_nodes(path)
        short_procs.update(short_path)

    common_paths = get_common_longest_paths(paths)
    return common_paths, dic_end_proc_cols, short_procs


def get_common_longest_paths(paths):
    if not paths:
        return []

    sorted_paths = sorted(paths, key=lambda x: len(x[0]), reverse=True)
    output_paths = [sorted_paths[0]]
    for path_obj in sorted_paths[1:]:
        path, is_trace_forward = path_obj
        is_new_branch = True
        for output_path, output_is_trace_forward in output_paths:
            if is_trace_forward == output_is_trace_forward:
                output_sub_path = output_path[: len(path)] if is_trace_forward else output_path[-len(path) :]

                if path == output_sub_path:
                    is_new_branch = False
                    break

        if is_new_branch and len(path) > 1:
            output_paths.append(path_obj)

    return output_paths


@log_execution_time()
def cast_df_number(df: DataFrame, graph_param: DicParam) -> DataFrame:
    int_sql_labels = []
    real_sql_labels = []
    text_sql_labels = []
    for proc in graph_param.array_formval:
        cfg_proc = graph_param.dic_proc_cfgs[proc.proc_id]
        for col in cfg_proc.columns:
            if col.data_type == DataType.INTEGER.value:
                int_sql_labels.append(gen_sql_label(col.id, col.column_name))
            elif col.data_type == DataType.REAL.value:
                real_sql_labels.append(gen_sql_label(col.id, col.column_name))
            elif col.data_type == DataType.TEXT.value:
                text_sql_labels.append(gen_sql_label(col.id, col.column_name))

    df = cast_df_integer(df, int_sql_labels)
    df = cast_df_real(df, real_sql_labels)
    df = cast_df_string(df, text_sql_labels)
    return df


@log_execution_time()
def validate_abnormal_count(df, sensor_labels):
    number_cols = df.select_dtypes(include=['integer', 'float']).columns.tolist()
    for col in number_cols:
        if col not in sensor_labels:
            continue

        abnormal_vals = detect_abnormal_count_values(df[col].dropna())
        df[col] = df[col].replace(abnormal_vals, pd.NA).astype(df[col].dtypes)

    return df


@log_execution_time()
def validate_data(df: DataFrame):
    if len(df) > THIN_DATA_COUNT:
        df_before = get_sample_df(df)
        df_after = validate_data_with_regex(df_before.copy(deep=True))
        checked_cols, dic_abnormal = get_changed_value_after_validate(df_before, df_after)
        df = validate_data_with_simple_searching(df, checked_cols, dic_abnormal)
    else:
        df = validate_data_with_regex(df)
    return df


def add_format_function(df, cfg_cols):
    for col in cfg_cols:
        format_padding = get_format_padding(col.format)
        if format_padding:
            add_zero_padding_df(df, col.gen_sql_label(), format_padding)


def cast_df_integer(df: DataFrame, int_sql_labels) -> DataFrame:
    for col_name in int_sql_labels:
        if col_name in df.columns:
            df[col_name] = df[col_name].astype('Int32')
    return df


def cast_df_real(df: DataFrame, real_sql_labels) -> DataFrame:
    for col_name in real_sql_labels:
        if col_name in df.columns:
            df[col_name] = df[col_name].astype('Float64')
    return df


def cast_df_string(df: DataFrame, string_sql_labels) -> DataFrame:
    for col_name in string_sql_labels:
        if col_name in df.columns:
            non_na_series = df[df[col_name].notnull()][col_name]
            non_na_series = non_na_series.astype(pd.StringDtype.name)
            df[col_name] = non_na_series
            df[col_name] = df[col_name].astype(pd.StringDtype.name)
    return df


@log_execution_time()
def remove_show_graph_duplicates(graph_param, df, serial_labels):
    serial_labels = serial_labels or []
    serial_labels = [label for label in serial_labels if label in df.columns]
    if serial_labels:
        if graph_param.common.duplicate_serial_show is DuplicateSerialShow.SHOW_FIRST:
            df = df.drop_duplicates(subset=serial_labels, keep='first')
            unique_serial = len(df)
        elif graph_param.common.duplicate_serial_show is DuplicateSerialShow.SHOW_LAST:
            df = df.drop_duplicates(subset=serial_labels, keep='last')
            unique_serial = len(df)
        else:
            unique_serial = len(df.drop_duplicates(subset=serial_labels))
    else:
        unique_serial = len(df)
    return df, unique_serial


@log_execution_time()
def get_sample_df(df):
    sample_df = df.head(THIN_DATA_COUNT)
    number_cols = df.select_dtypes(include=['integer', 'float']).columns.tolist()
    for col in number_cols:
        if not check_validate_target_column(col):
            continue
        try:
            # TODO: remove Exception handler after pandas version up
            # https://github.com/pandas-dev/pandas/issues/41696
            # pandas 1.2 does not work with Float64, Int64
            min_idx = df[col].idxmin()
            max_idx = df[col].idxmax()
            sample_df = pd.concat([sample_df, df[df.index.isin([min_idx, max_idx])]], ignore_index=True)
        except Exception:
            pass

    return sample_df


@log_execution_time()
def get_fmt_str_from_dic_data(graph_param, dic_data):
    fmt = {}
    for end_proc, data in dic_data.items():
        for sensor, array_y in data.items():
            if sensor in [TIME_COL]:
                continue
            col_cfg = graph_param.get_col_cfg(sensor)
            if col_cfg.data_type == DataType.INTEGER.value:
                fmt[sensor] = convert_to_d3_format(col_cfg.format)
            elif col_cfg.data_type == DataType.REAL.value:
                fmt[sensor] = convert_to_d3_format(col_cfg.format) or get_fmt_from_array(array_y)
            else:
                fmt[sensor] = ''

    return fmt


@log_execution_time()
def gen_dic_serial_data_from_df_thin(df: DataFrame, dic_param, dic_datetime_serial_cols, dic_ranks):
    dic_param[COMMON_INFO] = {}

    for plot in dic_param[ARRAY_PLOTDATA]:
        col_id = plot[END_COL_ID]

        proc_id = plot[END_PROC_ID]
        col_name = plot[END_COL_NAME]
        cat_exp = plot.get(CAT_EXP_BOX)
        datetime_col, serial_cols = dic_datetime_serial_cols.get(proc_id, (None, None))
        if datetime_col:
            dic_param[COMMON_INFO][proc_id] = {
                DATETIME_COL: datetime_col.shown_name,
                SERIAL_COLUMNS: [serial_col.shown_name for serial_col in serial_cols],
            }

        if col_id in dic_ranks:
            continue

        sql_label = gen_sql_label(col_id, col_name, cat_exp)
        sql_label = gen_sql_label(SERIAL_DATA, sql_label)
        if sql_label in df.columns:
            plot[SERIAL_DATA] = df[sql_label]
        else:
            plot[SERIAL_DATA] = pd.Series()


# TODO(khanhdq): add start_tm and end_tm
@log_execution_time()
def get_cond_data(proc: ConditionProc, with_limit=None):
    """generate subquery for every condition procs"""
    if not proc.dic_col_id_filters:
        return None

    conds = []
    # for filter_sensor in filter_sensors:
    for col_name, filters in proc.dic_col_name_filters.items():
        sql, params = gen_sql_condition_per_column(proc.proc_id, col_name, filters, with_limit)
        conds.append((sql, params))

    return conds


# TODO(khanhdq): add start_tm and end_tm
def gen_sql_condition_per_column(table_alias, column_name, filters, with_limit):
    and_conditions = gen_conditions_per_column(filters)
    params = []
    and_sqls = []
    col_name = f'{table_alias}.{column_name}'
    for in_vals, like_vals, regex_vals in and_conditions:
        or_sql = ''

        # IN
        if in_vals:
            if or_sql:
                or_sql += ' OR'

            or_sql += f" {col_name} IN ({','.join([PARAM_SYMBOL] * len(in_vals))})"
            params.extend(in_vals)

        # LIKE
        if like_vals:
            for like_val in like_vals:
                if or_sql:
                    or_sql += ' OR'
                or_sql += f' {col_name} LIKE {PARAM_SYMBOL}'
                params.append(like_val)

        # REGEX
        if regex_vals:
            for regex_val in regex_vals:
                if or_sql:
                    or_sql += ' OR'
                # or_sql += f' {col_name} {SQL_REGEXP_FUNC} {PARAM_SYMBOL}'
                or_sql += f' {col_name} ~ {PARAM_SYMBOL}'
                params.append(regex_val)

        if or_sql:
            and_sqls.append(f'({or_sql})')

    sql = ' AND '.join(and_sqls)

    if with_limit:
        sql += f' LIMIT {with_limit}'

    return sql, params


def gen_conditions_per_column(filters):
    ands = []
    for cfg_filter in filters:
        comp_ins = []
        comp_likes = []
        comp_regexps = []
        # cfg_filter_detail: CfgFilterDetail
        for cfg_filter_detail in cfg_filter.cfg_filter_details:
            val = cfg_filter_detail.filter_condition
            if cfg_filter_detail.filter_function == FilterFunc.REGEX.name:
                comp_regexps.append(val)
            elif not cfg_filter_detail.filter_function or cfg_filter_detail.filter_function == FilterFunc.MATCHES.name:
                comp_ins.append(val)
            else:
                comp_likes.extend(
                    gen_sql_like_value(
                        val,
                        FilterFunc[cfg_filter_detail.filter_function],
                        position=cfg_filter_detail.filter_from_pos,
                    ),
                )

        ands.append((comp_ins, comp_likes, comp_regexps))
        # ands.append(
        #     or_(
        #         sensor_val.value.in_(comp_ins),
        #         *[sensor_val.value.op(SQL_REGEXP_FUNC)(val) for val in comp_regexps if val is not None],
        #         *[sensor_val.value.like(val) for val in comp_likes if val is not None],
        #     )

    return ands


def create_graph_config(cfgs=[]):
    if not cfgs:
        return [
            {
                THRESH_HIGH: None,
                THRESH_LOW: None,
                Y_MAX: None,
                Y_MIN: None,
                PRC_MAX: None,
                PRC_MIN: None,
                ACT_FROM: None,
                ACT_TO: None,
                'type': None,
                'name': None,
            },
        ]

    list_cfgs = []
    for cfg in cfgs:
        list_cfgs.append(
            {
                THRESH_HIGH: cfg.ucl,
                THRESH_LOW: cfg.lcl,
                Y_MAX: cfg.ymax,
                Y_MIN: cfg.ymin,
                PRC_MAX: cfg.upcl,
                PRC_MIN: cfg.lpcl,
                ACT_FROM: cfg.act_from,
                ACT_TO: cfg.act_to,
                'type': cfg.filter_column.shown_name if cfg.filter_column else None,
                'name': cfg.filter_detail.name if cfg.filter_detail else None,
                'eng_name': cfg.filter_column.name_en if cfg.filter_column else None,  # -> name_en
            },
        )
    return list_cfgs


def get_default_graph_config(dic_proc_cfgs: Dict[int, CfgProcess], col_id, start_tm, end_tm):
    # get sensor default cfg chart info
    sensor_default_cfg = []
    for cfg_proc in dic_proc_cfgs.values():
        for cfg_col in cfg_proc.columns:
            if cfg_col.id == col_id:
                sensor_default_cfg = cfg_proc.get_sensor_default_chart_info(col_id, start_tm, end_tm)
                break

    # sensor_default_cfg: List[CfgVisualization] = (
    #         CfgVisualization.get_sensor_default_chart_info(col_id, start_tm, end_tm) or []
    # )
    return create_graph_config(sensor_default_cfg)


def get_col_graph_configs(dic_proc_cfgs: Dict[int, CfgProcess], col_id, filter_detail_ids, start_tm, end_tm):
    if not filter_detail_ids:
        return get_default_graph_config(dic_proc_cfgs, col_id, start_tm, end_tm)
    graph_configs = []
    for cfg_proc in dic_proc_cfgs.values():
        for cfg_col in cfg_proc.columns:
            if cfg_col.id == col_id:
                graph_configs = cfg_proc.get_by_control_n_filter_detail_ids(col_id, filter_detail_ids, start_tm, end_tm)
                break
    # graph_configs = CfgVisualization.get_by_control_n_filter_detail_ids(col_id, filter_detail_ids, start_tm, end_tm)
    if graph_configs:
        return create_graph_config(graph_configs)

    return get_default_graph_config(dic_proc_cfgs, col_id, start_tm, end_tm)


def convert_chart_info_time_range(
    chart_config,
    start_proc_times,
    end_proc_times,
    query_start_tm,
    query_end_tm,
    df=None,
):
    act_from = chart_config.get(ACT_FROM)
    act_to = chart_config.get(ACT_TO)
    query_start_tm = convert_time(query_start_tm, format_str=DATE_FORMAT_STR_POSTGRES, return_string=False)
    query_end_tm = convert_time(query_end_tm, format_str=DATE_FORMAT_STR_POSTGRES, return_string=False)
    if act_from:
        act_from = convert_time(act_from, format_str=DATE_FORMAT_STR_POSTGRES, return_string=False)
    if act_to:
        act_to = convert_time(act_to, format_str=DATE_FORMAT_STR_POSTGRES, return_string=False)

    if df is None:
        df = pd.DataFrame({START_TIME_COL: pd.Series(start_proc_times), END_TIME_COL: pd.Series(end_proc_times)})

    for col in df.columns:
        if not is_datetime64_ns_dtype(df[col].dtype):
            df[col] = pd.to_datetime(df[col])

    converted_act_from = None
    converted_act_to = None
    mask = None
    if act_from and act_to:
        mask = (df[END_TIME_COL] >= act_from) & (df[END_TIME_COL] <= act_to)
    elif act_from:
        with contextlib.suppress(Exception):
            mask = df[END_TIME_COL] >= act_from

    elif act_to:
        mask = df[END_TIME_COL] <= act_to

    if mask is not None and mask.any():
        if act_from:
            converted_act_from = query_start_tm if mask.iloc[0] else df[mask][START_TIME_COL].iloc[0]
        if act_to:
            converted_act_to = query_end_tm if mask.iloc[-1] else df[mask][START_TIME_COL].iloc[-1]

    if converted_act_from is not None:
        # if converted_act_from == start_proc_times[0]:
        #     converted_act_from = query_start_tm
        converted_act_from = convert_time(converted_act_from, format_str=DATE_FORMAT_STR_POSTGRES)

    if converted_act_to is not None:
        # if converted_act_to == start_proc_times[-1]:
        #     converted_act_to = query_end_tm
        converted_act_to = convert_time(converted_act_to, format_str=DATE_FORMAT_STR_POSTGRES)

    return converted_act_from, converted_act_to


@log_execution_time()
def get_chart_infos(graph_param: DicParam, dic_data=None, start_proc_times=None, no_convert=False):
    graph_configs = {}
    original_graph_configs = {}
    start_proc = graph_param.common.start_proc
    threshold_filter_detail_ids = graph_param.common.threshold_boxes
    for proc in graph_param.array_formval:
        graph_configs[proc.proc_id] = {}
        original_graph_configs[proc.proc_id] = {}

        start_tm = start_of_minute(graph_param.common.start_date, graph_param.common.start_time)
        end_tm = end_of_minute(graph_param.common.end_date, graph_param.common.end_time)
        end_proc = proc.proc_id
        end_proc_times = dic_data[proc.proc_id].get(TIME_COL, pd.Series()) if dic_data else pd.Series()
        df = pd.DataFrame({START_TIME_COL: start_proc_times, END_TIME_COL: end_proc_times})
        for col_id in proc.col_ids:
            orig_graph_cfg, graph_cfg = get_chart_info_detail(
                graph_param.dic_proc_cfgs,
                end_proc_times,
                col_id,
                threshold_filter_detail_ids,
                end_proc,
                start_proc,
                start_tm,
                end_tm,
                start_proc_times,
                df=df,
                no_convert=no_convert,
            )
            original_graph_configs[proc.proc_id][col_id] = orig_graph_cfg
            graph_configs[proc.proc_id][col_id] = graph_cfg

    return graph_configs, original_graph_configs


@log_execution_time()
def get_chart_info_detail(
    dic_proc_cfgs,
    end_proc_times,
    end_col,
    threshold_filter_detail_ids,
    end_proc=None,
    start_proc=None,
    start_tm=None,
    end_tm=None,
    start_proc_times=None,
    df=None,
    no_convert=False,
):
    start_tm = convert_time(start_tm)
    end_tm = convert_time(end_tm)
    query_start_tm = start_tm
    query_end_tm = end_tm
    if len(end_proc_times):
        ori_end_proc_times = end_proc_times.copy()  # Keep length of series to map with start proc calculate later
        end_proc_times = end_proc_times[end_proc_times.notna()]
        if len(end_proc_times):
            start_tm = end_proc_times.min()
            end_tm = end_proc_times.max()

        end_proc_times = ori_end_proc_times

    # get chart thresholds for each sensor
    col_graph_configs = get_col_graph_configs(dic_proc_cfgs, end_col, threshold_filter_detail_ids, start_tm, end_tm)
    orig_graph_cfgs = deepcopy(col_graph_configs)

    if (
        len(end_proc_times)
        and start_proc
        and end_proc
        and start_proc != end_proc
        and not no_convert
        and len(start_proc_times)
    ):
        # convert thresholds
        for chart_config in col_graph_configs:
            act_from, act_to = convert_chart_info_time_range(
                chart_config,
                start_proc_times,
                end_proc_times,
                query_start_tm,
                query_end_tm,
                df=df,
            )
            if act_from or act_to:
                chart_config[ACT_FROM] = act_from
                chart_config[ACT_TO] = act_to

    return col_graph_configs, orig_graph_cfgs


@log_execution_time()
def order_end_proc_sensor(orig_graph_param: DicParam, reorder):
    dic_orders = orig_graph_param.dic_card_orders
    lst_proc_end_col = []
    for proc in orig_graph_param.array_formval:
        proc_id = proc.proc_id
        for col_id, col_name, col_show_name in zip(proc.col_ids, proc.col_names, proc.col_show_names):
            column = orig_graph_param.get_col_cfg(col_id)
            fmt = convert_to_d3_format(column.format)
            proc_order = dic_orders.get(proc_id) or {}
            order = proc_order.get(str(col_id)) or 999
            lst_proc_end_col.append((proc_id, col_id, col_name, col_show_name, fmt, order))

    if not reorder:
        return lst_proc_end_col

    return sorted(lst_proc_end_col, key=lambda x: x[-1])


@log_execution_time()
def gen_plotdata(
    orig_graph_param: DicParam,
    dic_data,
    chart_infos=None,
    original_graph_configs=None,
    reorder=True,
):
    # re-order proc-sensors to show to UI
    lst_proc_end_col = order_end_proc_sensor(orig_graph_param, reorder)

    cat_exp_box_cols = orig_graph_param.common.cat_exp or []
    cat_exp_box_proc_name = []
    dic_procs, dic_cols = get_cfg_proc_col_info(orig_graph_param.dic_proc_cfgs, cat_exp_box_cols)
    for col in cat_exp_box_cols:
        cat_exp_box_proc_name.append(dic_cols[col].shown_name)

    plotdatas = []
    array_formval = []
    for proc_id, col_id, col_name, col_show_name, fmt, _ in lst_proc_end_col:
        array_y = dic_data.get(proc_id, {}).get(col_id, [])
        array_x = dic_data.get(proc_id, {}).get(TIME_COL, [])

        proc_cfg = orig_graph_param.dic_proc_cfgs[proc_id]
        col_cfg = proc_cfg.get_col(col_id)

        plotdata = {
            ARRAY_Y: array_y,
            ARRAY_X: array_x,
            END_PROC_ID: proc_id,
            END_COL_ID: col_id,
            END_COL_NAME: col_name,
            END_COL_SHOW_NAME: col_show_name,
            END_COL_FORMAT: fmt,
            CAT_EXP_BOX_NAME: cat_exp_box_proc_name,
            COL_DATA_TYPE: col_cfg.data_type,
            END_PROC_NAME: proc_cfg.shown_name,
            DATA_GROUP_TYPE: col_cfg.column_type,
        }

        plotdatas.append(plotdata)

        array_formval.append({END_PROC: proc_id, GET02_VALS_SELECT: col_id})

        # add chart info
        if chart_infos:
            set_chart_infos_to_plotdata(col_id, chart_infos, original_graph_configs, plotdata)

    return array_formval, plotdatas


@log_execution_time()
def gen_plotdata_fpp(
    orig_graph_param: DicParam,
    dic_data,
    chart_infos=None,
    original_graph_configs=None,
    dic_cycle_ids=None,
    reorder=True,
):
    # re-order proc-sensors to show to UI
    lst_proc_end_col = order_end_proc_sensor(orig_graph_param, reorder)

    plotdatas = []
    array_formval = []
    dic_proc_name = gen_dict_procs([proc_id for proc_id, *_ in lst_proc_end_col])
    for proc_id, col_id, col_name, col_show_name, fmt, _ in lst_proc_end_col:
        if proc_id not in dic_data or col_id not in dic_data.get(proc_id):
            continue

        y_list = dic_data.get(proc_id, {}).get(col_id, pd.Series())
        array_x = dic_data.get(proc_id, {}).get(TIME_COL, pd.Series())
        ranks = dic_data[proc_id].get(RANK_COL, {}).get(col_id)
        if not isinstance(y_list, (list, tuple)):
            y_list = [y_list]

        cate_names = dic_data.get(proc_id, {}).get(CAT_EXP_BOX, {}).get(col_id)
        none_idxs = dic_data.get(proc_id, {}).get(NONE_IDXS, {}).get(col_id)

        cat_exp_box_cols = orig_graph_param.common.cat_exp or []
        cat_exp_box_proc_name = []
        dic_procs, dic_cols = get_cfg_proc_col_info(orig_graph_param.dic_proc_cfgs, cat_exp_box_cols)
        for col in cat_exp_box_cols:
            cat_exp_box_proc_name.append(dic_cols[col].shown_name)

        col_cfg = orig_graph_param.dic_proc_cfgs[proc_id].get_col(col_id)

        for idx, array_y in enumerate(y_list):
            if orig_graph_param.common.cat_exp and len(array_y) == 0:
                continue

            column: CfgProcessColumn = orig_graph_param.get_col_cfg(col_id)
            is_master_col = column.is_master_data_column()
            plotdata = {
                ARRAY_Y: array_y,
                ARRAY_X: array_x,
                END_PROC_ID: proc_id,
                END_PROC_NAME: dic_proc_name[proc_id].shown_name,
                END_COL_ID: col_id,
                END_COL_NAME: col_name,
                END_COL_SHOW_NAME: col_show_name,
                UNIT: column.unit,
                END_COL_FORMAT: fmt,
                CAT_EXP_BOX_NAME: cat_exp_box_proc_name,
                COL_DATA_TYPE: col_cfg.data_type,
                IS_MASTER_COL: is_master_col,
            }

            if cate_names:
                plotdata.update({CAT_EXP_BOX: cate_names[idx]})

            if none_idxs:
                plotdata.update({NONE_IDXS: none_idxs[idx]})
                plotdata.update({ORG_NONE_IDXS: none_idxs[idx]})

            if dic_cycle_ids:
                plotdata.update({CYCLE_IDS: dic_cycle_ids.get(proc_id, {}).get(col_id, [])})

            if ranks:
                plotdata.update({RANK_COL: ranks[idx]})

            plotdatas.append(plotdata)

            array_formval.append({END_PROC: proc_id, GET02_VALS_SELECT: col_id})

            # add chart info
            if chart_infos:
                set_chart_infos_to_plotdata(col_id, chart_infos, original_graph_configs, plotdata)

    return array_formval, plotdatas


def set_chart_infos_to_plotdata(col_id, chart_infos, original_graph_configs, plotdata):
    """
    set chart config
    :param col_id:
    :param chart_infos:
    :param original_graph_configs:
    :param plotdata:
    :return:
    """
    if chart_infos is None:
        chart_infos = {}

    if original_graph_configs is None:
        original_graph_configs = {}

    chart_info = []
    original_graph_config = []
    for proc_id, dic_col in chart_infos.items():
        if col_id in dic_col:
            chart_info = dic_col[col_id]
            original_graph_config = original_graph_configs[proc_id][col_id]
            break

    plotdata[CHART_INFOS] = chart_info
    plotdata[CHART_INFOS_ORG] = original_graph_config


@log_execution_time()
def gen_category_data(graph_param: DicParam, dic_data, dic_org_cates=None):
    dic_proc_cfgs = graph_param.dic_proc_cfgs
    plotdatas = []
    cate_procs: List[CategoryProc] = graph_param.common.cate_procs
    dic_cates = dic_data.get(CATEGORY_DATA) or dic_data if graph_param.common.cat_exp else dic_data

    for proc in cate_procs:
        proc_id = proc.proc_id
        dic_proc = dic_cates.get(proc_id)
        if dic_proc is None:
            continue

        proc_cfg = dic_proc_cfgs[proc_id]

        for col_id, column_name, col_show_name in zip(proc.col_ids, proc.col_names, proc.col_show_names):
            col_cfg = proc_cfg.get_col(col_id)
            data = dic_proc.get(col_id)
            if len(data) == 0:
                continue

            array_y = data[0] if isinstance(data[0], (list, tuple, Series)) else data

            cate_summary = None
            if dic_org_cates:
                cate_summary = dic_org_cates[proc_id].get(col_id) if dic_org_cates.get(proc_id) else None

            column = graph_param.get_col_cfg(col_id)
            fmt = convert_to_d3_format(column.format)
            is_master_col = column.is_master_data_column()
            plotdata = {
                'proc_name': proc_id,
                'proc_master_name': proc_cfg.shown_name,
                'column_name': column_name,
                'column_master_name': col_show_name,
                'data': array_y,
                'origin_data': array_y,
                'summary': cate_summary,
                'column_id': col_id,
                'end_col_format': fmt,
                'is_master_col': is_master_col,
                'data_type': col_cfg.data_type,
            }
            plotdatas.append(plotdata)

    return plotdatas


@log_execution_time()
def clear_all_keyword(dic_param):
    """clear [All] keyword in selectbox

    Arguments:
        dic_param {json} -- [params from client]
    """
    dic_common = dic_param[COMMON]
    cate_procs = dic_common.get(CATE_PROCS, [])
    dic_formval = dic_param[ARRAY_FORMVAL]
    for idx in range(len(dic_formval)):
        select_vals = dic_formval[idx][GET02_VALS_SELECT]
        if isinstance(select_vals, (list, tuple)):
            dic_formval[idx][GET02_VALS_SELECT] = [val for val in select_vals if val not in [SELECT_ALL, NO_FILTER]]
        else:
            dic_formval[idx][GET02_VALS_SELECT] = [select_vals]

    for idx in range(len(cate_procs)):
        select_vals = cate_procs[idx][GET02_CATE_SELECT]
        if isinstance(select_vals, (list, tuple)):
            cate_procs[idx][GET02_CATE_SELECT] = [val for val in select_vals if val not in [SELECT_ALL, NO_FILTER]]
        else:
            cate_procs[idx][GET02_CATE_SELECT] = [select_vals]

    # Need NO_FILTER keyword to decide filter or not , so we can not remove NO_FILTER keyword here.
    for cond in dic_common[COND_PROCS]:
        for key, value in cond.items():
            vals = value if isinstance(value, (list, tuple)) else [value]

            if NO_FILTER in vals:
                continue

            cond[key] = [val for val in vals if val != SELECT_ALL]


@log_execution_time()
def get_serials(trace, proc_name):
    return [s.split()[0] for s in trace.hist2_yaml.get_serial_col(proc_name) if s]


@log_execution_time()
def get_date_col(trace, proc_name):
    date_col = trace.hist2_yaml.get_date_col(proc_name)
    date_col = date_col.split()[0]
    return date_col


@log_execution_time()
def make_irregular_data_none(dic_param):
    use_list = [YType.NORMAL.value, YType.OUTLIER.value, YType.NEG_OUTLIER.value]
    none_list = [float('inf'), float('-inf')]
    array_plotdata = dic_param.get(ARRAY_PLOTDATA)
    for num, plotdata in enumerate(array_plotdata):
        array_y = plotdata.get(ARRAY_Y, pd.Series())
        array_y_type = plotdata.get(ARRAY_Y_TYPE, [])

        if array_y_type:  # use y_type to check for irregular data
            # TODO : save data as {(from,to): value} is better
            # array_plotdata[num][ARRAY_Y] = [None if array_y_type[idx] not in {YType.NORMAL.value, YType.OUTLIER.value,
            # YType.NEG_OUTLIER.value} else e for idx, e in enumerate(array_y)]
            df = pd.DataFrame({ARRAY_Y: array_y, ARRAY_Y_TYPE: array_y_type})
            df[ARRAY_Y] = np.where(df[ARRAY_Y_TYPE].isin(use_list), df[ARRAY_Y], None)
        else:  # or use value to check for irregular data directly
            # array_plotdata[num][ARRAY_Y] = [None if e == float('inf') or e == float('-inf') else e for e in array_y]
            df = pd.DataFrame({ARRAY_Y: array_y})
            df[ARRAY_Y] = np.where(df[ARRAY_Y].isin(none_list), None, df[ARRAY_Y])

        array_plotdata[num][ARRAY_Y] = df[ARRAY_Y]

    return dic_param


def get_min_max_of_all_chart_infos(chart_infos):
    vals = [chart.get(Y_MIN) for chart in chart_infos if chart.get(Y_MIN) is not None]
    vals += [chart.get(Y_MAX) for chart in chart_infos if chart.get(Y_MAX) is not None]
    y_min = None
    y_max = None
    if vals:
        y_min = min(vals)
        y_max = max(vals)

    return y_min, y_max


def get_threshold_min_max_chartinfo(chart_infos):
    threshold_lows = [chart.get(THRESH_LOW) for chart in chart_infos if chart.get(THRESH_LOW) is not None]
    threshold_high = [chart.get(THRESH_HIGH) for chart in chart_infos if chart.get(THRESH_HIGH) is not None]

    vals = threshold_lows + threshold_high
    y_min = None
    y_max = None
    if vals:
        y_min = min(vals) if threshold_lows else None
        y_max = max(vals) if threshold_high else None

    return y_min, y_max


def calc_upper_lower_range(array_y: Series):
    arr = array_y[array_y.notnull()]
    arr = arr[~arr.isin([float('inf'), float('-inf')])]
    # arr = [e for e in arr if e not in {None, float('inf'), float('-inf')} and not pd.isna(e)]
    if not len(arr):
        return None, None

    q1, q3 = quantile(arr, [0.25, 0.75], interpolation='midpoint')
    iqr = q3 - q1
    if iqr:
        lower_range = q1 - 2.5 * iqr
        upper_range = q3 + 2.5 * iqr
    else:
        lower_range = 0.9 * min(arr)
        upper_range = 1.1 * max(arr)
        if lower_range == upper_range:
            lower_range -= 1
            upper_range += 1

    return float(lower_range), float(upper_range)


@log_execution_time()
def apply_coef(df: DataFrame, graph_param: DicParam):
    dic_proc_cfgs = graph_param.dic_proc_cfgs
    for end_proc_info in graph_param.array_formval:
        proc_cfg = dic_proc_cfgs.get(end_proc_info.proc_id)
        if proc_cfg is None:
            continue

        end_cols = proc_cfg.get_cols(end_proc_info.col_ids) or []
        for end_col in end_cols:
            if not end_col.coef or not end_col.operator:
                continue

            label = gen_sql_label(end_col.id, end_col.column_name)
            if label not in df.columns:
                continue

            if end_col.operator == Operator.REGEX.value:
                df[label] = np.where(df[label].str.contains(end_col.coef), df[label], pd.NA)
            elif end_col.operator == Operator.PLUS.value:
                df[label] = df[label] + float(end_col.coef)
            elif end_col.operator == Operator.MINUS.value:
                df[label] = df[label] - float(end_col.coef)
            elif end_col.operator == Operator.PRODUCT.value:
                df[label] = df[label] * float(end_col.coef)
            elif end_col.operator == Operator.DEVIDE.value:
                df[label] = df[label] / float(end_col.coef)

    return df


# @log_execution_time()
# def apply_coef_text(df: DataFrame, graph_param: DicParam, dic_proc_cfgs: Dict[int, CfgProcess]):
#     for proc_id, proc_cfg in dic_proc_cfgs.items():
#         if graph_param.is_end_proc(proc_id):
#             end_col_ids = graph_param.get_end_cols(proc_id) or []
#             end_cols: List[CfgProcessColumn] = proc_cfg.get_cols(end_col_ids) or []
#             for end_col in end_cols:
#                 if (
#                         DataType(end_col.data_type) is DataType.TEXT
#                         and end_col.coef is not None
#                         and end_col.operator == Operator.REGEX.value
#                 ):
#                     col_label = gen_sql_label(end_col.id, end_col.column_name)
#                     if col_label in df.columns:
#                         df[col_label] = (
#                             df[col_label]
#                             .astype('object')
#                             .str.replace('^(?!{})'.format(end_col.coef), pd.NA, regex=True)
#                         )
#     return df


def get_filter_detail_ids(dic_proc_cfgs: Dict[int, CfgProcess], proc_ids, column_ids):
    """
    get filter detail ids to check if this filter matching dataset of graph
    :param dic_proc_cfgs:
    :param proc_ids:
    :param column_ids:
    :return:
    """
    not_exact_matches = []
    dic_col_filter_details = defaultdict(list)
    cfg_filters = []
    for proc_id in proc_ids:
        cfg_filters += dic_proc_cfgs[proc_id].get_filter_cfg_by_col_ids(column_ids)
    for cfg_filter in cfg_filters:
        cfg_column: CfgProcessColumn = cfg_filter.column
        for cfg_detail in cfg_filter.filter_details:
            if cfg_detail.filter_function == FilterFunc.MATCHES.name:
                dic_col_filter_details[cfg_column].append((cfg_detail.id, cfg_detail.filter_condition))
            else:
                not_exact_matches.append(cfg_detail.id)

    return dic_col_filter_details, not_exact_matches


@log_execution_time()
def gen_dic_uniq_value_from_df(df, col_names):
    dic_col_values = {}
    for column in col_names:
        cfg_column: CfgProcessColumn = CfgProcessColumn.from_dict(column.__dict__)
        if cfg_column.is_master_data_column():
            col = cfg_column.master_data_column_id_label()
        else:
            col = gen_sql_label(cfg_column.id, cfg_column.column_name)

        column_name = gen_sql_label(cfg_column.id, cfg_column.column_name)
        if col in df.columns:
            vals = set(df[col])
            vals = [str(val) for val in vals]
            dic_col_values[column_name] = set(vals)

    return dic_col_values


def check_filter_detail_match_graph_data(dic_col_filter_details, dic_col_values):
    matched_filter_ids = []
    unmatched_filter_ids = []
    for cfg_column, filter_details in dic_col_filter_details.items():
        col_name = gen_sql_label(cfg_column.id, cfg_column.column_name)
        vals = dic_col_values.get(col_name, [])
        for filter_detail_id, filter_condition in filter_details:
            if filter_condition in vals:
                matched_filter_ids.append(filter_detail_id)
            else:
                unmatched_filter_ids.append(filter_detail_id)

    return matched_filter_ids, unmatched_filter_ids


@log_execution_time()
@abort_process_handler()
def main_check_filter_detail_match_graph_data(graph_param: DicParam, df: DataFrame):
    cond_proc_ids = [cond.proc_id for cond in graph_param.common.cond_procs]
    cond_col_ids = graph_param.get_all_end_col_ids()
    dic_col_filter_details, not_exact_match_filter_ids = get_filter_detail_ids(
        graph_param.dic_proc_cfgs,
        cond_proc_ids,
        cond_col_ids,
    )
    dic_col_values = gen_dic_uniq_value_from_df(df, dic_col_filter_details)
    matched_filter_ids, unmatched_filter_ids = check_filter_detail_match_graph_data(
        dic_col_filter_details,
        dic_col_values,
    )

    return matched_filter_ids, unmatched_filter_ids, not_exact_match_filter_ids


@log_execution_time()
def reduce_data(df_orig: DataFrame, graph_param, dic_str_cols):
    """
    make data for thin  mode
    :param df_orig:
    :param graph_param:
    :param dic_str_cols:
    :return:
    """

    # end cols
    dic_end_col_names = {}
    rank_cols = []
    for proc in graph_param.array_formval:
        for col_id, col_name in zip(proc.col_ids, proc.col_names):
            sql_label = gen_sql_label(col_id, col_name)
            cols_in_df = [col for col in df_orig.columns if col.startswith(sql_label)]
            target_col_info = dic_str_cols.get(sql_label)
            if target_col_info:
                rank_cols += cols_in_df
            else:
                for col_in_df in cols_in_df:
                    dic_end_col_names[col_in_df] = (proc.proc_id, col_id, col_name)

    # category
    dic_cate_names = {}
    cat_exp_col = graph_param.common.cat_exp
    for proc in graph_param.common.cate_procs:
        for col_id, col_name in zip(proc.col_ids, proc.col_names):
            if cat_exp_col:
                sql_label = gen_sql_label(CATEGORY_DATA, col_id, col_name)
            else:
                sql_label = gen_sql_label(col_id, col_name)

            if sql_label in df_orig.columns:
                dic_cate_names[sql_label] = (proc.proc_id, col_id, col_name)

    all_cols = list(set([TIME_COL] + list(dic_end_col_names) + list(dic_cate_names) + rank_cols))
    group_col = '__group_col__'
    index_col = '__index_col__'
    all_cols = [col for col in all_cols if col in df_orig.columns]
    df = df_orig[all_cols]
    x_option = graph_param.common.x_option or 'TIME'
    if x_option.upper() == 'TIME':
        df[group_col] = pd.to_datetime(df[TIME_COL]).values.astype(float)
        min_epoc_time = df[group_col].min()
        max_epoc_time = df[group_col].max()
        count_per_group = calc_data_per_group(min_epoc_time, max_epoc_time)
        df[group_col] = (df[group_col] - min_epoc_time) // count_per_group
        df[group_col] = df[group_col].astype(int)
    else:
        count_per_group = ceil(len(df) / THIN_DATA_CHUNK)
        df[group_col] = df.index // count_per_group

    # count element in one group
    group_counts = df[group_col].value_counts().tolist()

    df[index_col] = df.index
    df.set_index(group_col, inplace=True)
    total_group = len(group_counts)

    # get category mode(most common)
    df_blank = pd.DataFrame(index=range(total_group))
    dfs = [df_blank]
    str_cols = list(set(list(dic_cate_names) + rank_cols))
    df_cates = None
    if str_cols:
        df_temp = df[str_cols]
        df_temp = df_temp.groupby(group_col).agg(get_mode)

        for col in str_cols:
            if col not in df_temp.columns:
                df_temp[col] = None

        df_cates = pd.concat([df_blank, df_temp], axis=1)
        if rank_cols:
            dfs.append(df_cates[rank_cols])

    cols = []
    dic_min_med_max = {}

    # get from, to and count of each slot
    df_from_to_count = df.groupby(group_col)[TIME_COL].agg(['max', 'min', 'count'])

    df_not_na = df.replace([float('-inf'), float('inf'), None], np.nan).notna()

    # select all group which has all na value
    df_group_all_na = (~df_not_na).groupby(group_col).all()

    for sql_label, (proc_id, *_) in dic_end_col_names.items():
        # replace None to nan
        # cannot apply agg by None value
        df_temp = df[[TIME_COL, index_col, sql_label]].replace([None], np.nan)

        # get df remove -inf, inf and NA
        df_drop = df_temp[df_not_na[sql_label]]

        # get remaining group has only inf, -inf, NA
        remaining_df = df_temp[df_group_all_na[sql_label]]

        # calc min med max of 2 df and merge to one
        agg_methods = ['min', 'median', 'max']
        df_min_med_max_1 = pd.DataFrame(columns=agg_methods)
        df_min_med_max_2 = pd.DataFrame(columns=agg_methods)
        if not df_drop.empty:
            df_min_med_max_1 = df_drop.groupby(group_col)[sql_label].agg(agg_methods)
        if not remaining_df.empty:
            df_min_med_max_2 = remaining_df.groupby(group_col)[sql_label].agg(agg_methods)
        df_min_med_max = pd.concat([df_min_med_max_1, df_min_med_max_2]).sort_index()

        # get idxs of each group
        df_temp.drop(sql_label, axis=1, inplace=True)
        df_temp = df_temp.groupby(group_col).agg('min')
        df_temp = df_temp.rename(columns={index_col: sql_label})
        if len(df_temp) == 0:
            blank_vals = [None] * total_group
            df_temp[sql_label] = blank_vals

        dfs.append(df_temp)
        cols.append(sql_label)
        dic_min_med_max[sql_label] = df_min_med_max

    df_box = pd.concat(dfs, axis=1)

    # add time
    # start_tm = start_of_minute(graph_param.common.start_date, graph_param.common.start_time)
    # end_tm = end_of_minute(graph_param.common.end_date, graph_param.common.end_time)
    # times = pd.date_range(start=start_tm, end=end_tm, periods=THIN_DATA_CHUNK)
    # df_box[TIME_COL] = times
    # df_box[TIME_COL] = df_box[TIME_COL].astype('datetime64[s]')

    # remove blanks
    if x_option.upper() == 'TIME':
        df_box.dropna(how='all', subset=cols + rank_cols, inplace=True)

    # in case of select category sensors only, it should be add "time" into df
    if TIME_COL not in df_box.columns.tolist():
        df_box[TIME_COL] = df_from_to_count['min']

    # remove blank category
    dic_cates = defaultdict(dict)
    dic_org_cates = defaultdict(dict)
    for sql_label, (proc_id, col_id, _) in dic_cate_names.items():
        if df_cates is not None and sql_label in df_cates:
            dic_cates[proc_id][col_id] = df_cates.loc[df_box.index, sql_label]
        dic_org_cates[proc_id][col_id] = get_available_ratio(df[sql_label])

    return df_box, dic_cates, dic_org_cates, group_counts, df_from_to_count, dic_min_med_max


def calc_data_per_group(min_val, max_val, box=THIN_DATA_CHUNK):
    dif_val = max_val - min_val + 1
    ele_per_box = dif_val / box
    return ele_per_box


@log_execution_time()
def calc_raw_common_scale_y(plots, string_col_ids=None, y_col=ARRAY_Y):
    """
    calculate y min max in common scale
    :param y_col:
    :param plots:
    :param string_col_ids:
    :return:
    """
    y_commons = []
    min_max_list = []
    for plot in plots:
        s = pd.Series(plot[y_col])

        s = s[s.notnull()]
        if not len(s):
            min_max_list.append((None, None))
            continue

        s = convert_series_to_number(s)
        s_without_inf = s[np.isfinite(s)]

        min_val = s_without_inf.min()
        max_val = s_without_inf.max()
        if pd.isna(min_val):
            min_val = None
        if pd.isna(max_val):
            max_val = None

        min_max_list.append((min_val, max_val))

        if string_col_ids and plot[END_COL_ID] in string_col_ids:
            continue

        if min_val is not None:
            y_commons.append(min_val)

        if max_val is not None:
            y_commons.append(max_val)

    all_graph_min = None
    all_graph_max = None
    if y_commons:
        all_graph_min = min(y_commons)
        all_graph_max = max(y_commons)

    return min_max_list, all_graph_min, all_graph_max


def detect_abnormal_data(series_x, series_y, none_idxs=None):
    nones = none_idxs
    if none_idxs is None:
        nones = series_y[series_y.isnull()].index.tolist()

    return {
        UNLINKED_IDXS: series_x[series_x.isnull()].index.tolist(),
        NONE_IDXS: nones,
        INF_IDXS: series_y[series_y == float('inf')].index.tolist(),
        NEG_INF_IDXS: series_y[series_y == float('-inf')].index.tolist(),
    }


def calc_auto_scale_y(plotdata, series_y, force_outlier=False):
    series_y = convert_series_to_number(series_y)
    notna_series_y = series_y[series_y.notna()]
    if not len(notna_series_y) or series_y.dtypes == object:
        return {Y_MIN: 0, Y_MAX: 1, LOWER_OUTLIER_IDXS: [], UPPER_OUTLIER_IDXS: []}

    summaries = plotdata.get(SUMMARIES) or []
    lower_range = None
    upper_range = None
    if isinstance(summaries, dict):  # Sometime summaries variable is dict, must convert to list to avoid error
        summaries = [summaries]
    for summary in summaries:
        dic_non_param = summary.get('non_parametric')
        if dic_non_param:
            lower_range = dic_non_param.get('lower_range_org')
            upper_range = dic_non_param.get('upper_range_org')

    if lower_range is None:
        p25, p75 = np.percentile(notna_series_y, [25, 75])
        iqr = p75 - p25
        lower_range = p25 - 2.5 * iqr
        upper_range = p75 + 2.5 * iqr

    # auto scale for color variable will ignore outlier check
    if force_outlier:
        lower_range, upper_range = series_y.min(), series_y.max()

    lower_outlier_idxs = series_y[series_y < lower_range].index.tolist() if lower_range is not None else []
    upper_outlier_idxs = series_y[series_y > upper_range].index.tolist() if upper_range is not None else []

    if upper_range == lower_range:
        lower_outlier_idxs = []
        upper_outlier_idxs = []

    if lower_range and series_y.min() >= 0:
        lower_range = max(0, lower_range)

    if upper_range and series_y.max() <= 0:
        upper_range = min(0, upper_range)

    lower_range, upper_range = extend_min_max(lower_range, upper_range)

    return {
        Y_MIN: lower_range,
        Y_MAX: upper_range,
        LOWER_OUTLIER_IDXS: lower_outlier_idxs,
        UPPER_OUTLIER_IDXS: upper_outlier_idxs,
    }


def calc_setting_scale_y(plotdata, series_y):
    series_y = convert_series_to_number(series_y)
    # calculate upper/lower limit
    chart_infos = plotdata.get(CHART_INFOS)
    dic_scale_auto = plotdata.get(SCALE_AUTO, {})
    if not chart_infos:
        if dic_scale_auto:
            return dic_scale_auto

        return {Y_MIN: series_y.min(), Y_MAX: series_y.max()}

    ymin, ymax = get_min_max_of_all_chart_infos(chart_infos)
    if ymin is None and ymax is None:
        if dic_scale_auto:
            return dic_scale_auto

        return {Y_MIN: series_y.min(), Y_MAX: series_y.max()}

    if ymin is None:
        ymin = dic_scale_auto.get(Y_MIN)
        lower_outlier_idxs = dic_scale_auto.get(LOWER_OUTLIER_IDXS)
    else:
        lower_outlier_idxs = series_y[series_y < ymin].index.tolist()

    if ymax is None:
        ymax = dic_scale_auto.get(Y_MAX)
        upper_outlier_idxs = dic_scale_auto.get(UPPER_OUTLIER_IDXS)
    else:
        upper_outlier_idxs = series_y[series_y > ymax].index.tolist()

    ymin, ymax = extend_min_max(ymin, ymax)

    return {
        Y_MIN: float(ymin),
        Y_MAX: float(ymax),
        LOWER_OUTLIER_IDXS: lower_outlier_idxs,
        UPPER_OUTLIER_IDXS: upper_outlier_idxs,
    }


def calc_threshold_scale_y(plotdata, series_y):
    series_y = convert_series_to_number(series_y)
    # calculate upper/lower limit
    chart_infos = plotdata.get(CHART_INFOS)
    dic_scale_auto = plotdata.get(SCALE_AUTO, {})
    if not chart_infos:
        return dic_scale_auto

    thresh_low, thresh_high = get_threshold_min_max_chartinfo(chart_infos)
    if thresh_low is None and thresh_high is None:
        return dic_scale_auto

    lower_range, upper_range = calc_upper_lower_range(series_y)
    if thresh_low is None:
        thresh_low = dic_scale_auto.get(THRESH_LOW) or lower_range

    if thresh_high is None:
        thresh_high = dic_scale_auto.get(THRESH_HIGH) or upper_range

    thresh_high = 0 if thresh_high is None else thresh_high
    thresh_low = 0 if thresh_low is None else thresh_low
    margin = (thresh_high - thresh_low) * 0.1  # margin 10%
    upper_margin_val = thresh_high + margin
    lower_margin_val = thresh_low - margin

    lower_outlier_idxs = series_y[series_y < lower_margin_val].index.tolist()
    upper_outlier_idxs = series_y[series_y > upper_margin_val].index.tolist()

    return {
        Y_MIN: thresh_low,
        Y_MAX: thresh_high,
        LOWER_OUTLIER_IDXS: lower_outlier_idxs,
        UPPER_OUTLIER_IDXS: upper_outlier_idxs,
    }


@log_execution_time()
def calc_scale_info(
    dic_proc_cfgs: Dict[int, CfgProcess],
    array_plotdata,
    min_max_list,
    all_graph_min,
    all_graph_max,
    string_col_ids=None,
    has_val_idxs=None,
    end_col_id=END_COL_ID,
    y_col=ARRAY_Y,
    force_outlier=False,
):
    dic_datetime_cols = {}
    for idx, plotdata in enumerate(array_plotdata):
        # datetime column
        proc_id = plotdata.get(END_PROC_ID)
        col_id = plotdata.get(END_COL_ID)
        if proc_id and proc_id not in dic_datetime_cols:
            dic_datetime_cols[proc_id] = {
                cfg_col.id: cfg_col
                for cfg_col in dic_proc_cfgs[proc_id].get_cols_by_data_type(DataType.DATETIME, column_name_only=False)
            }

        is_datetime_col = col_id in dic_datetime_cols.get(proc_id, {})

        y_min = min_max_list[idx][0] if min_max_list else None
        y_min = all_graph_min if y_min is None else y_min
        y_max = min_max_list[idx][1] if min_max_list else None
        y_max = all_graph_max if y_max is None else y_max

        y_min, y_max = extend_min_max(y_min, y_max)
        all_graph_min, all_graph_max = extend_min_max(all_graph_min, all_graph_max)

        array_y = plotdata.get(ARRAY_Y)
        array_x = plotdata.get(ARRAY_X)

        series_x = array_x.reset_index(drop=True)
        series_y = array_y.reset_index(drop=True)

        # don't do with all blank idxs
        if has_val_idxs is not None:
            series_x = series_x.loc[has_val_idxs]
            series_y = series_y.loc[has_val_idxs]

        none_idxs = plotdata.get(NONE_IDXS)
        dic_abnormal_data = detect_abnormal_data(series_x, series_y, none_idxs)
        plotdata.update(dic_abnormal_data)
        if (not len(array_y)) or (not len(array_x)) or (string_col_ids and plotdata[END_COL_ID] in string_col_ids):
            dic_base_scale = {
                Y_MIN: float(y_min),
                Y_MAX: float(y_max),
                LOWER_OUTLIER_IDXS: [],
                UPPER_OUTLIER_IDXS: [],
            }
            plotdata[SCALE_AUTO] = dic_base_scale
            plotdata[SCALE_SETTING] = dic_base_scale
            plotdata[SCALE_THRESHOLD] = dic_base_scale
            plotdata[SCALE_COMMON] = dic_base_scale
            plotdata[SCALE_FULL] = dic_base_scale
            continue

        for _idxs in dic_abnormal_data.values():
            series_y.loc[_idxs] = None

        series_y = pd.Series(array_y).reset_index(drop=True)
        if has_val_idxs is not None:
            series_y = series_y.loc[has_val_idxs]

        series_y = convert_series_to_number(series_y)
        plotdata[SCALE_AUTO] = calc_auto_scale_y(plotdata, series_y, force_outlier=force_outlier)
        if is_datetime_col:
            plotdata[SCALE_AUTO][Y_MIN] = y_min

        plotdata[SCALE_SETTING] = calc_setting_scale_y(plotdata, series_y)
        plotdata[SCALE_THRESHOLD] = calc_threshold_scale_y(plotdata, series_y)
        plotdata[SCALE_COMMON] = {
            Y_MIN: all_graph_min,
            Y_MAX: all_graph_max,
            LOWER_OUTLIER_IDXS: [],
            UPPER_OUTLIER_IDXS: [],
        }
        plotdata[SCALE_FULL] = {
            Y_MIN: y_min,
            Y_MAX: y_max,
            LOWER_OUTLIER_IDXS: [],
            UPPER_OUTLIER_IDXS: [],
        }
        if is_datetime_col:
            plotdata[SCALE_FULL][Y_MIN] = 0

    return True


@log_execution_time()
def gen_kde_data_trace_data(dic_param, full_arrays=None):
    array_plotdata = dic_param.get(ARRAY_PLOTDATA)
    for num, plotdata in enumerate(array_plotdata):
        full_array_y = full_arrays[num] if full_arrays else None
        kde_list = calculate_kde_trace_data(plotdata, full_array_y=full_array_y)
        (
            plotdata[SCALE_SETTING][KDE_DATA],
            plotdata[SCALE_COMMON][KDE_DATA],
            plotdata[SCALE_THRESHOLD][KDE_DATA],
            plotdata[SCALE_AUTO][KDE_DATA],
            plotdata[SCALE_FULL][KDE_DATA],
        ) = kde_list

    calculate_histogram_count_common(array_plotdata)

    return dic_param


def extend_min_max(y_min, y_max):
    if y_max is None:
        y_max = y_min * 1.2 if y_min is not None else 1

    if y_min is None:
        y_min = y_max * 0.8

    if y_min == y_max:
        y_min *= 0.8
        y_max *= 1.2

    if y_min == 0 and y_max == 0:
        y_min = -1
        y_max = 1

    return y_min, y_max


@log_execution_time()
def copy_dic_param_to_thin_dic_param(dic_param, dic_thin_param):
    ignore_keys = [COMMON, ARRAY_FORMVAL, ARRAY_PLOTDATA, CYCLE_IDS, SERIAL_DATA]
    for key, val in dic_param.items():
        if key in ignore_keys:
            continue

        dic_thin_param[key] = dic_param[key]

    return True


@log_execution_time()
def gen_thin_df_cat_exp(dic_param):
    df = pd.DataFrame()
    dic_end_cols = {}

    # df['index'] = list(range(len(dic_param[TIMES])))
    # TODO: get cycle_ids for fpp ( plot view function will use this am
    if CYCLE_IDS in dic_param:
        df[ID] = dic_param[CYCLE_IDS]
    df[TIME_COL] = dic_param[TIMES]

    for plot in dic_param[ARRAY_PLOTDATA] or []:
        time_sql_label = f'time_{plot[END_PROC_ID]}'
        if time_sql_label not in df.columns:
            array_x = plot[ARRAY_X]
            if len(array_x):
                df[time_sql_label] = array_x

        sql_label = gen_sql_label(plot[END_COL_ID], plot[END_COL_NAME], plot.get(CAT_EXP_BOX))
        dic_end_cols[sql_label] = (plot[END_COL_ID], plot[END_COL_NAME], plot.get(CAT_EXP_BOX), plot.get(NONE_IDXS))
        df[sql_label] = plot[ARRAY_Y]

    # serials
    add_serials_to_thin_df(dic_param, df)

    # categories
    add_categories_to_thin_df(dic_param, df)

    return df, dic_end_cols


def get_available_ratio(series: Series):
    n_total = series.size
    # na_counts = series.isnull().sum().sum()
    non_na_counts = len(series.dropna())
    na_counts = n_total - non_na_counts
    na_percentage = (100 * na_counts / n_total) if n_total else 0
    non_na_percentage = 100 - na_percentage
    return {'nTotal': n_total, 'nonNACounts': non_na_counts, 'nonNAPercentage': signify_digit(non_na_percentage)}


@log_execution_time()
def add_serials_to_thin_df(dic_param, df):
    for plot in dic_param[ARRAY_PLOTDATA] or []:
        proc_id = plot[END_PROC_ID]
        sql_label = gen_sql_label(SERIAL_DATA, proc_id)
        if sql_label in df.columns:
            continue

        serials = dic_param.get(SERIAL_DATA, {}).get(proc_id)
        if serials is not None and len(serials):
            df[sql_label] = serials


@log_execution_time()
def add_categories_to_thin_df(dic_param, df):
    for dic_cate in dic_param.get(CATEGORY_DATA) or []:
        col_id = dic_cate.get('column_id')
        col_name = dic_cate.get('column_name')
        data = dic_cate.get('data')
        sql_label = gen_sql_label(CATEGORY_DATA, col_id, col_name)
        if sql_label in df.columns:
            continue

        df[sql_label] = data


@log_execution_time()
def get_serials_and_date_col(graph_param: DicParam):
    dic_output = {}
    for proc in graph_param.array_formval:
        proc_cfg = graph_param.dic_proc_cfgs[proc.proc_id]
        serial_cols = proc_cfg.get_serials(column_name_only=False)
        datetime_col = proc_cfg.get_date_col(column_name_only=False)
        dic_output[proc.proc_id] = (datetime_col, serial_cols)

    return dic_output


@log_execution_time()
def gen_unique_data(df, dic_proc_cfgs: Dict[int, CfgProcess], col_ids, has_na=False):
    if not col_ids:
        return {}

    dic_unique_cate = {}

    # dic_cols = {col.id: col for col in CfgProcessColumn.get_by_ids(col_ids)}
    _, dic_cols = get_cfg_proc_col_info(dic_proc_cfgs, col_ids)

    for col_id in col_ids:
        cfg_col = dic_cols.get(col_id)
        col_name = cfg_col.column_name
        master_name = cfg_col.shown_name
        proc_id = cfg_col.process_id
        col_type = cfg_col.data_type
        if col_type not in [DataType.TEXT.value, DataType.INTEGER.value]:
            continue
        sql_label = gen_sql_label(RANK_COL, col_id, col_name)
        if sql_label not in df.columns:
            sql_label = gen_sql_label(col_id, col_name)

        unique_data = pd.Series()
        if sql_label in df.columns:
            unique_data = df[sql_label].drop_duplicates()
            if not has_na:
                unique_data = unique_data.dropna()

        cfg_proc_name = dic_proc_cfgs[proc_id].shown_name
        column = get_col_cfg(dic_proc_cfgs, col_id)
        fmt = convert_to_d3_format(column.format)
        format_padding = get_format_padding(column.format)
        if column.data_type == DataType.INTEGER.value and format_padding:
            unique_data = add_zero_padding_series(unique_data, format_padding)

        unique_data = {
            'proc_name': proc_id,
            'proc_master_name': cfg_proc_name,
            'column_name': col_name,
            'column_master_name': master_name,
            'column_id': col_id,
            'end_col_format': fmt,
            UNIQUE_CATEGORIES: unique_data.tolist(),
            COL_DATA_TYPE: cfg_col.data_type,
        }

        dic_unique_cate[col_id] = unique_data

    return dic_unique_cate


@log_execution_time()
def filter_df(dic_proc_cfgs: Dict[int, CfgProcess], df, dic_filter):
    if not dic_filter:
        return df

    # dic_names = {col.id: col for col in CfgProcessColumn.get_by_ids(dic_filter)}
    _, dic_names = get_cfg_proc_col_info(dic_proc_cfgs, list(dic_filter))
    for col_id, _vals in dic_filter.items():
        vals = _vals
        if not vals:
            continue

        if not isinstance(vals, (list, tuple)):
            vals = [vals]

        if NO_FILTER in vals:
            continue

        vals = [val for val in vals if val not in [SELECT_ALL, NO_FILTER]]
        if not vals:
            continue

        cfg_col = dic_names.get(col_id, None)
        if cfg_col is None:
            continue

        sql_label = gen_sql_label(RANK_COL, col_id, cfg_col.column_name)
        if sql_label not in df.columns:
            sql_label = gen_sql_label(col_id, cfg_col.column_name)

        is_filter_nan = False
        if None in vals:
            is_filter_nan = True
            vals.remove(None)
        dtype_name = cfg_col.data_type
        if dtype_name == DataType.INTEGER.value:
            vals = [int(val) for val in vals]
        elif dtype_name == DataType.REAL.value:
            vals = [float(val) for val in vals]
        elif dtype_name == DataType.TEXT.value:
            vals = [str(val) for val in vals]
            df[sql_label] = df[sql_label].astype(str)
        else:
            pass

        if is_filter_nan:
            vals += [np.nan, np.inf, -np.inf, 'nan', '<NA>', np.NAN, pd.NA]

        df = df[df[sql_label].isin(vals)]

        df[sql_label] = df[sql_label].replace(['nan', '<NA>'], np.nan)

    df.reset_index(drop=True, inplace=True)
    return df


@log_execution_time()
def gen_category_info(dic_param, dic_ranks):
    for plot in dic_param[ARRAY_PLOTDATA]:
        if plot[END_COL_ID] in dic_ranks:
            # category variable
            p_array_y = pd.Series(plot[ARRAY_Y]).dropna()
            cat_size = 0
            if len(p_array_y):
                cat_size = np.unique(p_array_y).size
            plot[CAT_TOTAL] = cat_size
            plot[IS_CAT_LIMITED] = cat_size >= MAX_CATEGORY_SHOW
    return dic_param


def get_cfg_proc_col_info(dic_proc_cfgs: Dict[int, CfgProcess], col_ids):
    dic_procs = {}
    dic_cols = {}
    for proc_id, proc in dic_proc_cfgs.items():
        _dic_cols = proc.get_dic_cols_by_ids(col_ids)
        if _dic_cols:
            dic_cols.update(_dic_cols)
            dic_procs[proc_id] = proc

    return dic_procs, dic_cols


@log_execution_time()
def calculate_histogram_count_common(array_plotdata):
    x_min = {}
    x_max = {}
    for scale in [SCALE_SETTING, SCALE_COMMON, SCALE_THRESHOLD, SCALE_AUTO, SCALE_FULL]:
        x_min[scale] = 0
        x_max[scale] = 0
        fmt = {}
        for num, plotdata in enumerate(array_plotdata):
            hist_count = plotdata[scale][KDE_DATA][RL_HIST_COUNTS]
            x_min[scale] = min(x_min[scale], min(hist_count, default=0))
            x_max[scale] = max(x_max[scale], max(hist_count, default=0))

            col_id = plotdata.get(END_COL_ID) or plotdata.get(END_COL)
            hist_labels = plotdata[scale][KDE_DATA][RL_HIST_LABELS]
            hist_labels = hist_labels.tolist() if len(hist_labels) > 0 else []
            if col_id not in fmt:
                fmt[col_id] = hist_labels
            else:
                fmt[col_id] += hist_labels

        for col, full_array in fmt.items():
            fmt[col] = get_fmt_from_array(full_array)

        for num, plotdata in enumerate(array_plotdata):
            col_id = plotdata.get(END_COL_ID) or plotdata.get(END_COL)
            # convert to int instead of np.int64 in some cases
            plotdata[scale]['x-min'] = int(x_min[scale])
            plotdata[scale]['x-max'] = int(x_max[scale])
            plotdata[scale]['label_fmt'] = fmt[col_id]


@log_execution_time()
def get_outlier_info(remove_outlier_type):
    percent = 0.25  # default p25-p75
    actual_type = RemoveOutlierType.O6M
    distance = 0
    if remove_outlier_type in [
        RemoveOutlierType.O6M,
        RemoveOutlierType.O6I,
        RemoveOutlierType.O6U,
        RemoveOutlierType.O6L,
    ]:
        distance = 2.5  # +- 2.5iqr

    if remove_outlier_type in [
        RemoveOutlierType.O4M,
        RemoveOutlierType.O4I,
        RemoveOutlierType.O4U,
        RemoveOutlierType.O4L,
    ]:
        distance = 1.5  # +- 1.5iqr

    if remove_outlier_type in [RemoveOutlierType.OP1]:
        percent = 0.01  # p1-p99

    if remove_outlier_type in [RemoveOutlierType.OP5]:
        percent = 0.05  # p5-p95

    if remove_outlier_type in [
        RemoveOutlierType.O4M,
        RemoveOutlierType.O6M,
        RemoveOutlierType.OP1,
        RemoveOutlierType.OP5,
    ]:
        actual_type = RemoveOutlierType.Majority

    if remove_outlier_type in [RemoveOutlierType.O4I, RemoveOutlierType.O6I]:
        actual_type = RemoveOutlierType.Minority

    if remove_outlier_type in [RemoveOutlierType.O4U, RemoveOutlierType.O6U]:
        actual_type = RemoveOutlierType.Upper

    if remove_outlier_type in [RemoveOutlierType.O6L, RemoveOutlierType.O4L]:
        actual_type = RemoveOutlierType.Lower

    return actual_type, percent, distance


@log_execution_time()
def remove_outlier(df: DataFrame, sensor_labels, graph_param):
    if df.empty:
        return df
    remove_outlier_type = graph_param.common.remove_outlier_type
    is_real_only = graph_param.common.remove_outlier_real_only

    actual_type, percent, distance = get_outlier_info(remove_outlier_type)
    upper_limit = 1 - percent
    lower_limit = percent

    valid_cols = [col for col in df.columns if col in sensor_labels]

    # limits to a (float), b (int) and e (timedelta)
    if is_real_only:
        numeric_cols = df.loc[:, valid_cols].select_dtypes('float').columns
    else:
        numeric_cols = df.loc[:, valid_cols].select_dtypes('number').columns
    df_sub = df.loc[:, numeric_cols]
    for target_col in numeric_cols:
        series = df_sub[target_col].replace(dict.fromkeys([pd.NA], np.nan))
        lower, upper, q1, q3 = series.quantile([lower_limit, upper_limit, 0.25, 0.75])
        # do not apply outlier of q1 = q3
        if q1 == q3:
            continue

        iqr = upper - lower
        lower_whisker = lower - distance * iqr
        upper_whisker = upper + distance * iqr
        condition = None

        if actual_type == RemoveOutlierType.Majority:
            condition = (df_sub[target_col] < lower_whisker) | (df_sub[target_col] > upper_whisker)
        elif actual_type == RemoveOutlierType.Minority:
            condition = (df_sub[target_col] > lower_whisker) & (df_sub[target_col] < upper_whisker)
        elif actual_type == RemoveOutlierType.Upper:
            condition = df_sub[target_col] < upper_whisker
        elif actual_type == RemoveOutlierType.Lower:
            condition = df_sub[target_col] > lower_whisker

        df.loc[condition, target_col] = pd.NA

    return df


@log_execution_time()
def get_serial_and_datetime_data(df, graph_param, dic_proc_cfgs: Dict[int, CfgProcess]):
    serials = []
    date_times = pd.Series()
    start_proc_name = ''
    for proc in dic_proc_cfgs:
        if proc == graph_param.common.start_proc:
            start_proc = dic_proc_cfgs[proc]
            if start_proc:
                start_proc_name = start_proc.shown_name
                serial_cols = start_proc.get_serials(column_name_only=False)
                datetime_col = start_proc.get_date_col(column_name_only=False)
                datetime_id = datetime_col.id
                datetime_label = gen_sql_label(datetime_id, datetime_col.column_name)
                if datetime_label not in df.columns:
                    datetime_label = f'time_{proc}'
                date_times = df[datetime_label]
                for serial_col in serial_cols:
                    serial_label = gen_sql_label(serial_col.id, serial_col.column_name)
                    if serial_label in df.columns:
                        serials.append(df[serial_label])

    return serials, date_times, start_proc_name


def gen_sql_proc_link_key_from_trace_keys(
    edge: CfgTrace,
) -> Tuple[List[SqlProcLinkKey], List[SqlProcLinkKey]]:
    self_sensor_keys: List[SqlProcLinkKey] = []
    target_sensor_keys: List[SqlProcLinkKey] = []
    for key in edge.trace_keys:
        self_key = SqlProcLinkKey(
            id=key.self_column.id,
            name=key.self_column.bridge_column_name,
            substr_from=key.self_column_substr_from,
            substr_to=key.self_column_substr_to,
            delta_time=key.delta_time,
            cut_off=key.cut_off,
        )
        self_sensor_keys.append(self_key)

        target_key = SqlProcLinkKey(
            id=key.target_column.id,
            name=key.target_column.bridge_column_name,
            substr_from=key.target_column_substr_from,
            substr_to=key.target_column_substr_to,
            delta_time=None,
            cut_off=None,
        )
        target_sensor_keys.append(target_key)

    return self_sensor_keys, target_sensor_keys


@log_execution_time()
@grpc_api()
def tracing_data_common(
    graph_param,
    start_proc_id=None,
    start_tm=None,
    end_tm=None,
    is_save_df_to_file=False,
    _use_expired_cache=False,
    __FORCE_OFFLINE_MODE__=False,
    __IF_DISCONNECT_RUN_OFFLINE_MODE__=True,
):
    if not start_proc_id:
        start_proc_id = graph_param.common.start_proc

    if not start_tm:
        start_tm = start_of_minute(graph_param.common.start_date, graph_param.common.start_time)

    if not end_tm:
        end_tm = end_of_minute(graph_param.common.end_date, graph_param.common.end_time)

    duplicate_serial_show = graph_param.common.duplicate_serial_show
    duplicated_serials_count = graph_param.common.duplicated_serials_count

    common_paths, dic_end_proc_cols, short_procs = reduce_graph(
        graph_param.array_formval,
        graph_param.trace_graph,
        graph_param.common.start_proc,
    )
    df, actual_record_number, unique_serial_number = gen_graph_df(
        graph_param.trace_graph.dic_edges,
        start_tm,
        end_tm,
        graph_param.array_formval,
        graph_param.common.cond_procs,
        common_paths,
        short_procs,
        duplicate_serial_show=duplicate_serial_show,
        duplicated_serials_count=duplicated_serials_count,
        _use_expired_cache=_use_expired_cache,
    )
    # save log
    if is_save_df_to_file or graph_param.common.is_export_mode:
        save_df_to_file(df)

    # fill missing columns
    for proc in graph_param.array_formval:
        for col_id, col_name in zip(proc.col_ids, proc.col_names):
            label = gen_sql_label(col_id, col_name)
            # put assert here to make sure sql generated correctly
            # assert label in df.columns, f"{label} should exist in df.columns, if this is not the case, " \
            #                             f"generated-sql must be wrong"
            if label not in df.columns:
                df[label] = None

    # reset index
    df.reset_index(drop=True, inplace=True)

    df = cast_df_number(df, graph_param)

    _, is_show_duplicated = is_show_duplicated_serials(
        duplicate_serial_show,
        duplicated_serials_count,
        actual_record_number,
    )
    if not is_show_duplicated:
        unique_serial_number = None

    return df, actual_record_number, unique_serial_number


def add_equation_column_to_df(df, function_detail, graph_config_data: ShowGraphConfigData):
    cfg_col = graph_config_data.get_col(function_detail.process_column_id)
    equation_class = get_function_class_by_id(function_detail.function_id)
    equation = equation_class.from_kwargs(**function_detail.as_dict())

    # get column_x and column_y
    cfg_col_x = graph_config_data.get_col(function_detail.var_x)
    cfg_col_y = graph_config_data.get_col(function_detail.var_y)

    column_out = gen_sql_label(cfg_col.id, cfg_col.column_name)
    column_x = gen_sql_label(cfg_col_x.id, cfg_col_x.column_name) if cfg_col_x else None
    column_y = gen_sql_label(cfg_col_y.id, cfg_col_y.column_name) if cfg_col_y else None

    x_dtype = cfg_col_x.raw_data_type if cfg_col_x else None
    y_dtype = cfg_col_y.raw_data_type if cfg_col_y else None

    return equation.evaluate(df, out_col=column_out, x_col=column_x, y_col=column_y, x_dtype=x_dtype, y_dtype=y_dtype)


def sorted_function_details(cfg_process_columns: list[CfgProcessColumn]) -> list[CfgProcessFunctionColumn]:
    cfg_function_cols = itertools.chain.from_iterable(cfg_col.function_details for cfg_col in cfg_process_columns)
    return sorted(cfg_function_cols, key=lambda col: col.order)


def get_equation_data(df, end_proc: EndProc):
    sorted_cfg_function_cols = sorted_function_details(end_proc.cfg_proc.get_cols(col_ids=end_proc.col_ids))
    for cfg_func_col in sorted_cfg_function_cols:
        df = add_equation_column_to_df(df, cfg_func_col, end_proc.cfg_proc)

    for col in df.columns:
        if '__SHOW_NAME__' in col:
            df.drop(columns=col, inplace=True)

    # cast equation function to string if its `data-type` is string
    for cfg_func_col in sorted_cfg_function_cols:
        cfg_col = end_proc.cfg_proc.get_col(cfg_func_col.process_column_id)
        if cfg_col.data_type == DataType.TEXT.value:
            label = gen_sql_label(cfg_col.id, cfg_col.column_name)

            original_type = df[label].dtype
            df[label] = df[label].astype(pd.StringDtype())

            # handle for case of boolean True False -> true false
            if pd.api.types.is_bool_dtype(original_type):
                df[label] = df[label].str.lower()

    return df


def gen_proc_link_temp_table_sql(table_name, from_sql):
    sql = f'CREATE TEMP TABLE IF NOT EXISTS {table_name} AS {from_sql}'
    return sql


def gen_temp_table_name(proc_id, from_proc_id, to_proc_id):
    return f'{SHOW_GRAPH_TEMP_TABLE_NAME}_{from_proc_id}_{proc_id}_{to_proc_id}'


@log_execution_time(SQL_GENERATOR_PREFIX)
def gen_proc_link_from_sql(
    sql_objs: List[SqlProcLink],
    cond_procs: List[ConditionProc],
    duplicated_serial_show: DuplicateSerialShow,
    for_count: bool = False,
    use_row_number: bool = True,
) -> Tuple[str, Dict[str, str]]:
    cte_proc_list = []

    dict_cond_procs: Dict[int, List[ConditionProc]] = {}
    for cond in cond_procs:
        if cond.proc_id not in dict_cond_procs:
            dict_cond_procs[cond.proc_id] = [cond]
        else:
            dict_cond_procs[cond.proc_id].append(cond)

    # TODO: this should not happen here
    for idx, sql_obj in enumerate(sql_objs):
        sql_objs[idx].condition_procs = dict_cond_procs.get(sql_obj.process_id, [])
        sql_objs[idx].is_start_proc = idx == 0

    for idx, sql_obj in enumerate(sql_objs):
        is_start_proc = idx == 0

        cte_proc = sql_obj.gen_cte(
            idx=idx,
            duplicated_serial_show=duplicated_serial_show,
            is_start_proc=is_start_proc,
            use_row_number=use_row_number,
        )

        cte_proc_list.append(cte_proc)

    for i, cte_proc in enumerate(cte_proc_list):
        # TODO: filter for category?

        cte = cte_proc
        cte = gen_cte_show_category(
            cte=cte,
            sql_obj=sql_objs[i],
        )
        cte_proc_list[i] = cte

    cte_tracing = gen_tracing_cte(
        tracing_table_alias='cte_tracing',
        cte_proc_list=cte_proc_list,
        sql_objs=sql_objs,
        duplicated_serial_show=duplicated_serial_show,
        dict_cond_procs=dict_cond_procs,
    )

    cte_tracing_delta_time_cut_off = gen_tracing_cte_with_delta_time_cut_off(cte_tracing=cte_tracing, sql_objs=sql_objs)

    if for_count:
        stmt = gen_id_stmt(cte_tracing=cte_tracing_delta_time_cut_off)
    else:
        stmt = gen_show_stmt(cte_tracing=cte_tracing_delta_time_cut_off, sql_objs=sql_objs)

    sql, params = gen_sql_and_params(stmt)
    return sql, params


def get_selected_cate_column_ids(dic_param: dict):
    """
    Get all selected category column

    Args:
        dic_param: a dictionary that contain all information and values for showing graph

    Returns:
        list: a list of column ids
    """
    cate_column_ids = []
    for cate_proc in dic_param[COMMON][CATE_PROCS]:
        for column_ids in [cate_proc[GET02_CATE_SELECT]]:
            cate_column_ids.extend(column_ids)
    return [int(column_id) for column_id in set(cate_column_ids)]


@log_execution_time()
def retrieve_order_setting(dic_proc_cfgs: Dict[int, CfgProcess], dic_param):
    dic_orders_columns = {}
    selected_cate_column_ids = get_selected_cate_column_ids(dic_param)
    for proc_id, proc_cfg in dic_proc_cfgs.items():
        order_cols = proc_cfg.get_order_cols(column_name_only=False, column_id_only=True)
        if len(order_cols):
            dic_orders_columns[proc_id] = [
                order_id
                for order_id in order_cols
                if order_id in dic_param[COMMON][DF_ALL_COLUMNS] or order_id in selected_cate_column_ids
            ]
    dic_param[COMMON][AVAILABLE_ORDERS] = dic_orders_columns
    return dic_param


@log_execution_time()
def sort_df_by_x_option(
    df,
    dic_param,
    graph_param,
    dic_proc_cfgs,
    temp_x_option,
    temp_serial_process,
    temp_serial_column,
    temp_serial_order,
):
    if temp_x_option:
        df = check_and_order_data(
            df,
            dic_proc_cfgs,
            temp_x_option,
            temp_serial_process,
            temp_serial_column,
            temp_serial_order,
        )
        dic_param[COMMON][X_OPTION] = temp_x_option
        dic_param[COMMON][SERIAL_PROCESS] = temp_serial_process
        dic_param[COMMON][SERIAL_COLUMN] = temp_serial_column
        dic_param[COMMON][SERIAL_ORDER] = temp_serial_order
    else:
        x_option = graph_param.common.x_option or 'TIME'
        serial_processes = graph_param.common.serial_processes or []
        serial_cols = graph_param.common.serial_columns or []
        serial_orders = graph_param.common.serial_orders or []
        df = check_and_order_data(df, dic_proc_cfgs, x_option, serial_processes, serial_cols, serial_orders)

    return df, dic_param


@log_execution_time()
def discretize_float_data_equally(x, num_bins=10):
    """Discretize an 1D-NumpyArray with dtype=float
    Each value of x is replaced with the average value of its corresponding bin.
    For example, if x = np.array([0.0, 1.0, 2.0, 3.0]) and num_bins=2,
    this function will return [0.5, 0.5, 2.5, 2.5].
    """

    # NOTE:
    # 1. This function returns error when np.nan/np.inf exists
    # 2. x_discretized must have the same digits as x
    #    (If x is 123.45, x_discretized must be like 120.12)

    nuniq = len(np.unique(x))
    if nuniq <= num_bins:
        return x

    # get edges to categorize the data
    # +1 on the last elements ensures that the maximum value is included in the last bin
    bin_edges = np.histogram_bin_edges(x, bins=num_bins)
    bin_edges[-1] = bin_edges[-1] + 1
    assert len(bin_edges) == num_bins + 1

    # np.digitize returns bin number, starting from 1
    # (-1 is to let the bin number start from 0)
    bin_edges = np.array(bin_edges.tolist())
    x_discretized = np.digitize(x, bins=bin_edges)

    # bincount does not accept minus number
    if x_discretized.min() > 0:
        x_discretized = x_discretized - 1

    # calculate sample size and sum of each x in each bin, then get averages
    counts = np.bincount(x_discretized)
    sums = np.bincount(x_discretized, weights=x)
    averages = sums / counts

    # replace the values of x to the average of its corresponding bin
    x_discretized = averages[x_discretized]
    return x_discretized


@MessageAnnouncer.notify_progress(60)
def gen_graph(
    graph_param,
    dic_param,
    max_graph=None,
    rank_value=False,
    use_export_df=False,
    __FORCE_OFFLINE_MODE__=False,
    __IF_DISCONNECT_RUN_OFFLINE_MODE__=True,
):
    # TODO: move to rlp page , because only this page use this function
    (
        dic_param,
        cat_exp,
        cat_procs,
        dic_cat_filters,
        use_expired_cache,
        *_,
    ) = customize_dic_param_for_reuse_cache(dic_param)
    dic_param, df, graph_param_with_cate = gen_df(
        graph_param,
        dic_param,
        dic_cat_filters,
        add_dt_col=True,
        rank_value=rank_value,
        use_expired_cache=use_expired_cache,
    )

    origin_graph_param = bind_dic_param_to_class(
        graph_param.dic_proc_cfgs,
        graph_param.trace_graph,
        graph_param.dic_card_orders,
        dic_param,
    )
    dic_proc_cfgs = graph_param.dic_proc_cfgs

    dic_param = filter_cat_dict_common(df, dic_param, cat_exp, cat_procs, graph_param)

    export_df = df.copy()
    convert_datetime_to_ct(df, graph_param)
    dic_data, is_graph_limited = gen_dic_data(dic_proc_cfgs, df, origin_graph_param, graph_param_with_cate, max_graph)
    dic_param = gen_dic_param(origin_graph_param, df, dic_param, dic_data)
    dic_param[IS_GRAPH_LIMITED] = is_graph_limited

    if use_export_df:
        return dic_param, export_df, graph_param_with_cate
    return dic_param


@log_execution_time()
def add_master_information_for_hovering(master_df, dic_param, graph_param):
    """
    :param graph_param:
    :param master_df: dataframe received by sql from database.
        This dataframe must store master id if we select master columns
    :param dic_param:
    :return:
        dic_param with new keys added:
            - `master_info`: dictionary in which keys as master column name, values as selected rows
            - `array_master_ids`: ids of master tables, this is stored inside `array_plot_data`
    """
    master_infos = {}
    for array_data in dic_param[ARRAY_PLOTDATA]:
        cfg_col: CfgProcessColumn = graph_param.get_col_cfg(array_data[END_COL_ID])
        assert cfg_col is not None

        master_key = array_data[END_COL_NAME]

        if not cfg_col.is_master_data_column():
            array_data[ARRAY_MASTER_IDS] = []
            continue

        master_label = cfg_col.master_data_column_id_label()
        if master_label not in master_df:
            array_data[ARRAY_MASTER_IDS] = []
            continue

        array_master_ids = master_df[master_label]
        unique_array_master_ids = [int(x) for x in master_df[master_label].dropna().unique()]

        # get table that datagroup type belong
        data_group_type = DataGroupType(cfg_col.column_type)
        column_meta_data = MasterColumnMetaCatalog.instance(data_group_type)
        hover_model = column_meta_data.hover_model

        if master_key not in master_infos:
            master_infos[master_key] = {}

        with make_session() as db_session:
            hover_datas = hover_model.get_hover_by_ids(db_session=db_session, ids=unique_array_master_ids)
            for hover in hover_datas:
                master_infos[master_key].update({hover.id: hover.model_dump(mode='json')})

        array_data[ARRAY_MASTER_IDS] = array_master_ids

    # add for label
    for category_data in dic_param.get(FILTER_ON_DEMAND, []).get(RL_CATEGORY, []):
        cfg_col: CfgProcessColumn = graph_param.get_col_cfg(category_data[COL_ID])
        assert cfg_col is not None

        master_key = category_data[COL_NAME]

        if not cfg_col.is_master_data_column():
            continue

        master_label = cfg_col.master_data_column_id_label()
        unique_array_master_ids = [int(x) for x in master_df[master_label].dropna().unique()]

        # get table that datagroup type belong
        data_group_type = DataGroupType(cfg_col.column_type)
        column_meta_data = MasterColumnMetaCatalog.instance(data_group_type)
        hover_model = column_meta_data.hover_model

        if master_key not in master_infos:
            master_infos[master_key] = {}

        with make_session() as db_session:
            hover_datas = hover_model.get_hover_by_ids(db_session=db_session, ids=unique_array_master_ids)
            for hover in hover_datas:
                master_infos[master_key].update({hover.id: hover.model_dump(mode='json')})

    dic_param[MASTER_INFO] = master_infos


@log_execution_time()
def add_master_information_for_hovering_scp(master_df, dic_param, graph_param):
    """
    :param master_df: dataframe received by sql from database.
        This dataframe must store master id if we select master columns
    :param dic_param:
    :return:
        dic_param with new keys added:
            - `master_info`: dictionary in which keys as master column name, values as selected rows
            - `array_master_ids`: ids of master tables, this is stored inside `array_plot_data`
    """
    master_infos = {}
    # add for label
    category_data_ids = []
    for key1, key2, key3 in zip([X_IS_MASTER, Y_IS_MASTER], [X_ID, Y_ID], [X_LABEL, Y_LABEL]):
        if dic_param.get(key1):
            category_data_ids.append(
                {
                    COL_ID: dic_param.get(key2),
                    COL_NAME: dic_param.get(key3),
                },
            )

    for category_data in category_data_ids:
        cfg_col: CfgProcessColumn = graph_param.get_col_cfg(category_data[COL_ID])
        assert cfg_col is not None

        master_key = cfg_col.column_name

        if not cfg_col.is_master_data_column():
            category_data[CATEGORY_MASTER_IDS] = []
            continue

        master_label = cfg_col.master_data_column_id_label()
        if master_label not in master_df:
            category_data[CATEGORY_MASTER_IDS] = []
            continue

        array_master_ids = master_df[master_label]
        category_data[CATEGORY_MASTER_IDS] = array_master_ids
        unique_array_master_ids = [int(x) for x in master_df[master_label].dropna().unique()]

        # get table that datagroup type belong
        data_group_type = DataGroupType(cfg_col.column_type)
        column_meta_data = MasterColumnMetaCatalog.instance(data_group_type)
        hover_model = column_meta_data.hover_model

        if master_key not in master_infos:
            master_infos[master_key] = {}

        with make_session() as db_session:
            hover_datas = hover_model.get_hover_by_ids(db_session=db_session, ids=unique_array_master_ids)
            for hover in hover_datas:
                master_infos[master_key].update({hover.id: hover.model_dump(mode='json')})

    dic_param[MASTER_INFO] = master_infos


@log_execution_time()
def extract_master_id_from_df(df: pd.DataFrame, graph_param) -> tuple[pd.DataFrame, pd.DataFrame]:
    master_id_cols_name = set()
    for proc in graph_param.array_formval:
        for col_id in proc.col_ids:
            cfg_col: CfgProcessColumn = graph_param.get_col_cfg(col_id)
            if not cfg_col.is_master_data_column():
                continue
            col_name = cfg_col.master_data_column_id_label()
            if col_name in df.columns:
                master_id_cols_name.add(col_name)

    for proc in graph_param.common.cate_procs:
        for col_id in proc.col_ids:
            cfg_col: CfgProcessColumn = graph_param.get_col_cfg(col_id)
            if not cfg_col.is_master_data_column():
                continue
            col_name = cfg_col.master_data_column_id_label()
            if col_name in df.columns:
                master_id_cols_name.add(col_name)

    master_df = df[list(master_id_cols_name)]
    df = df.drop(columns=list(master_id_cols_name))
    return df, master_df


@log_execution_time()
def gen_rank_cols_to_master_ids_dict(df: pd.DataFrame, master_df: pd.DataFrame, graph_param: DicParam):
    dic_rank_cols_to_master_ids = {}
    col_ids = []
    for end_proc in graph_param.array_formval:
        col_ids = col_ids + end_proc.get_col_ids()

    for cate_proc in graph_param.common.cate_procs:
        col_ids = col_ids + cate_proc.col_ids

    col_ids = set(col_ids)
    for col_id in col_ids:
        cfg_col: CfgProcessColumn = graph_param.get_col_cfg(col_id)
        if cfg_col is None or not cfg_col.is_master_data_column():
            continue
        dic_rank_cols_to_master_ids[cfg_col.id] = (
            pd.Series(
                master_df[cfg_col.master_data_column_id_label()].values,
                index=df[cfg_col.gen_sql_label()].convert_dtypes(),
            )
            .dropna()
            .drop_duplicates()
            .to_dict()
        )
    return dic_rank_cols_to_master_ids


@log_execution_time()
def add_master_mapping_for_rank_values(
    dic_param: dict[str, Any],
    dic_rank_cols_to_master_ids: dict[int, Any],
) -> None:
    for array_data in dic_param[ARRAY_PLOTDATA]:
        cfg_col: Optional[CfgProcessColumn] = CfgProcessColumn.get_by_id(array_data[END_COL_ID])
        if cfg_col is None:
            raise AssertionError(f'{array_data[END_COL_ID]} must exist.')
        if cfg_col.id not in dic_rank_cols_to_master_ids:
            continue
        array_data[RANK_COLS_TO_MASTER_IDS] = dic_rank_cols_to_master_ids[cfg_col.id]
    dic_param[RANK_COLS_TO_MASTER_IDS] = dic_rank_cols_to_master_ids


@log_execution_time()
def update_draw_data_trace_log(dataset_id, exe_time):
    trace_logs = DataTraceLog.get_dataset_id(dataset_id, EventAction.DRAW.value)
    with make_session() as meta_session:
        for trace in trace_logs:
            if trace.exe_time == 0:
                trace.exe_time = exe_time
            insert_or_update_config(meta_session, trace)
            meta_session.commit()

    return True


@log_execution_time()
def convert_datetime_to_ct(df: DataFrame, graph_param, target_vars=[]):
    """
    Convert sensor that is datetime type to cycle time
    :param dic_proc_cfgs:
    :param df:
    :param graph_param:
    :param target_vars:
    :return:
    """

    if df.empty:
        return

    if not target_vars:
        target_vars = graph_param.common.sensor_cols
        color_id = graph_param.common.color_var
        if color_id:
            target_vars += [color_id]

    dt_labels = set()
    for target_var in target_vars:
        general_col_info = graph_param.get_col_info_by_id(target_var)
        if general_col_info[COL_DATA_TYPE] == DataType.DATETIME.value:
            dt_labels.add(gen_sql_label(target_var, general_col_info[END_COL_NAME]))
        elif general_col_info[COL_DATA_TYPE] in [DataType.DATE.value, DataType.TIME.value]:
            target_col = gen_sql_label(target_var, general_col_info[END_COL_NAME])
            df[target_col] = df[df[target_col].notnull()][target_col].astype('string')

    if not dt_labels:
        return

    facet_cols = graph_param.common.cat_exp or []
    if graph_param.common.div_by_cat:
        facet_cols.append(graph_param.common.div_by_cat)

    dic_cycle_times = defaultdict(list)
    # in case facet (level1, level2)
    if facet_cols:
        facet_labels = []
        for facet in facet_cols:
            col_cfg = graph_param.get_col_cfg(facet)
            facet_labels.append(gen_sql_label(facet, col_cfg.column_name))
        df_group = df.groupby(facet_labels, dropna=False)
        for idx, (name, group) in enumerate(df_group, start=1):
            # if idx > MAX_GRAPH_COUNT:
            #     break

            for dt_col in dt_labels:
                series = group[dt_col]
                series = calc_cycle_time_of_list(series)
                dic_cycle_times[dt_col].append(series)

    else:
        for dt_col in dt_labels:
            series = df[dt_col]
            series = calc_cycle_time_of_list(series)
            dic_cycle_times[dt_col].append(series)

    # assign cycle time to original df
    for dt_col, series_groups in dic_cycle_times.items():
        df[dt_col] = np.nan
        for series in series_groups:
            df[dt_col].update(series.astype('Float64'))

    return True


@log_execution_time()
def calc_cycle_time_of_list(series: Series):
    UPPER_LIM_MARGIN = 5

    series = pd.to_datetime(series)
    # sort datetime target col
    series = series.sort_values()
    # compute elapsed time, periods is "-1" mean (i1 - i2,)
    # then add minus to get (i2 - i1)
    series = -series.diff(periods=-1).dt.total_seconds()
    # filter CT not zero/ replace zero to NA
    series = series.replace(0, np.nan)
    # compute upper lim
    upper_lim = UPPER_LIM_MARGIN * series.median()
    # convert outliers (>= 5*median) to NA
    series = series.where(series < upper_lim, np.nan)
    return series


@log_execution_time()
def get_filter_on_demand_data(dic_param, remove_filter_data=False):
    dic_param[FILTER_ON_DEMAND] = {
        'facet': dic_param.get(CAT_EXP_BOX, []),
        'system': dic_param.get(CAT_ON_DEMAND, []),
        'color': dic_param.get(UNIQUE_COLOR, []),
        'div': dic_param.get(UNIQUE_DIV, []),
        'category': dic_param.get(CATEGORY_DATA, []),
        'filter': [] if remove_filter_data else dic_param.get(FILTER_DATA, []),
    }
    filter_key = [CAT_EXP_BOX, CATEGORY_DATA, FILTER_DATA, UNIQUE_COLOR, UNIQUE_DIV, CAT_ON_DEMAND]
    for key in filter_key:
        if key in dic_param:
            dic_param.pop(key)

    save_odf_data_of_request(dic_param)

    return dic_param


@MessageAnnouncer.notify_progress(40)
@abort_process_handler()
@log_execution_time()
@trace_log((TraceErrKey.ACTION, TraceErrKey.TARGET), (EventAction.READ, Target.DATABASE))
def get_data_from_db(
    graph_param: DicParam,
    dic_filter={},
    start_proc_id=None,
    start_tm=None,
    end_tm=None,
    is_save_df_to_file=False,
    use_expired_cache=False,
    with_categorized_real=False,
):
    # the system is busy
    dic_request_info[LAST_REQUEST_TIME] = datetime.utcnow()

    # DEBUG Function
    if get_debug_data(DebugKey.IS_DEBUG_MODE.name):
        df = get_debug_data(DebugKey.GET_DATA_FROM_DB.name)
        return df, df.index.size, None

    # start proc
    if not start_tm:
        start_tm = start_of_minute(graph_param.common.start_date, graph_param.common.start_time)
        # start_tm = convert_time(start_tm, return_string=False)

    if not end_tm:
        end_tm = end_of_minute(graph_param.common.end_date, graph_param.common.end_time)
        # end_tm = convert_time(end_tm, return_string=False)

    # add category
    graph_param.add_cate_procs_to_array_formval()

    # add cat exp
    graph_param.add_cat_exp_to_array_formval()

    # add NG column
    graph_param.add_ng_condition_to_array_formval()

    # add column in cfg_equation
    graph_param.add_function_cols_to_sensor_cols()

    query_offline = True
    # TODO: open when run in Bridge station mode
    # query_offline = FactoryImport.is_range_time_imported(graph_param.common.start_proc, start_tm, end_tm)
    df, actual_record_number, unique_serial_number = tracing_data_common(
        graph_param,
        start_proc_id=start_proc_id,
        start_tm=start_tm,
        end_tm=end_tm,
        is_save_df_to_file=is_save_df_to_file,
        _use_expired_cache=use_expired_cache,
        __FORCE_OFFLINE_MODE__=query_offline,
        __IF_DISCONNECT_RUN_OFFLINE_MODE__=True,
    )

    # check empty
    if df is None or not len(df):
        return df, 0, 0

    if graph_param.common.is_validate_data:
        df = validate_data(df)
        df = cast_df_number(df, graph_param)  # TODO: we might not need this.

    cfg_cols = graph_param.get_cfg_cols_(graph_param.common.sensor_cols)
    sensor_labels = [col.gen_sql_label() for col in cfg_cols]

    if with_categorized_real:
        for i, col in enumerate(cfg_cols):
            if (
                col.data_type
                in [
                    DataType.REAL.name,
                    DataType.INTEGER.name,
                    DataType.DATETIME.name,
                ]
                and sensor_labels[i] in df.columns
            ):
                col_name = sensor_labels[i]
                categorized_col_name = sensor_labels[i] + CATEGORIZED_SUFFIX
                df[categorized_col_name] = df[col_name]
                df[categorized_col_name] = df[categorized_col_name].replace(
                    dict.fromkeys([np.inf, -np.inf, float('nan')], np.nan),
                )
                is_real_col = col.data_type == DataType.REAL.name
                is_datetime_col = col.data_type == DataType.DATETIME.name
                if is_real_col or (not col.is_int_category and not is_datetime_col):
                    categorized_data = discretize_float_data_equally(
                        df[categorized_col_name][df[categorized_col_name].notna()],
                    )
                    # convert column to float
                    df[categorized_col_name] = df[categorized_col_name].astype(float)
                    df[categorized_col_name][df[categorized_col_name].notna()] = categorized_data

                if is_datetime_col:
                    df[col_name] = calc_cycle_time_of_list(df[col_name])
                    df[categorized_col_name] = df[col_name].astype(float)

    # on-demand filter
    if dic_filter:
        df = filter_df(graph_param.dic_proc_cfgs, df, dic_filter)

    if graph_param.common.abnormal_count:
        df = validate_abnormal_count(df, sensor_labels)

    if graph_param.common.is_remove_outlier:
        df = remove_outlier(df, sensor_labels, graph_param)

    if graph_param.common.remove_outlier_objective_var:
        objective_id = graph_param.common.objective_var
        sensor_labels = [col.gen_sql_label() for col in cfg_cols if col.id == objective_id]
        df = remove_outlier(df, sensor_labels, graph_param)

    if graph_param.common.remove_outlier_explanatory_var:
        objective_id = graph_param.common.objective_var
        sensor_labels = [col.gen_sql_label() for col in cfg_cols if col.id != objective_id]
        df = remove_outlier(df, sensor_labels, graph_param)

    df = cast_df_number(df, graph_param)

    all_col_ids = []
    for end_col in graph_param.array_formval:
        all_col_ids = all_col_ids + end_col.col_ids
    all_cfg_cols = [graph_param.get_col_cfg(col_id) for col_id in all_col_ids]
    add_format_function(df, all_cfg_cols)
    return df, actual_record_number, unique_serial_number


def get_distinct_facet(plots):
    """ """
    facet_vals = []
    end_cols = []
    for plot in plots:
        col_id = plot[END_COL_ID]
        if col_id not in end_cols:
            end_cols.append(col_id)
        cats = plot.get(CAT_EXP_BOX, None)
        if cats:
            facet_vals.append(tuple(str(cate) for cate in cats))

    if facet_vals:
        facet_vals = sorted(set(facet_vals))

    return end_cols, facet_vals


def add_facet_position_to_dic_param(dic_param):
    plots = dic_param[ARRAY_PLOTDATA]
    end_cols, facet_vals = get_distinct_facet(plots)
    row = 1
    if len(facet_vals) > 1 and len(end_cols) > 1:
        row = len(end_cols)

    dic_param[FACET_ROW] = row
    dic_param[SENSORS] = end_cols

    return dic_param


def reduce_dic_proc_cfgs(dic_proc_cfgs, short_procs):
    dic_reduce = {proc_id: dic_proc_cfgs[proc_id] for proc_id in short_procs}
    return dic_reduce


def reduce_trace_graph(dic_edges, short_procs):
    dic_reduce = {}
    for proc_ids, edge in dic_edges.items():
        from_proc_id, to_proc_id = proc_ids
        if from_proc_id in short_procs or to_proc_id in short_procs:
            dic_reduce[proc_ids] = edge

    return dic_reduce


def gen_trace_procs_sqls(path, dic_edges, start_tm, end_tm, end_procs: List[EndProc], short_procs):
    sql_objs: List[SqlProcLink] = []
    end_proc_start_tm, end_proc_end_tm = gen_end_proc_start_end_time(start_tm, end_tm)
    start_proc = path[0]
    dic_processes = {proc_id: TransactionData(proc_id) for proc_id in path}
    # create table if not exist
    with BridgeStationModel.get_db_proxy() as db_instance:
        for proc_id, trans_data in dic_processes.items():
            if not trans_data.is_table_exist(db_instance):
                trans_data.create_table(db_instance)
                db_instance.connection.commit()

    if len(short_procs) == 1:
        proc_id = list(short_procs)[0]
        trans_data: TransactionData = dic_processes[proc_id]
        proc_link_sql = SqlProcLink(transaction_data=trans_data)

        end_proc = None
        for end_proc in end_procs:
            if end_proc.proc_id == proc_id:
                break

        proc_link_sql.select_col_ids = end_proc.col_ids
        proc_link_sql.select_col_names = [trans_data.get_column_name(_id) for _id in end_proc.col_ids]
        proc_link_sql.link_keys = []
        proc_link_sql.next_link_keys = []
        proc_link_sql.start_tm = start_tm
        proc_link_sql.end_tm = end_tm if proc_id == start_proc else end_proc_end_tm
        sql_objs.append(proc_link_sql)
        return sql_objs

    for from_proc, to_proc in zip(path[:-1], path[1:]):
        is_trace_forward = True
        edge_id = (from_proc, to_proc)
        edge = dic_edges.get(edge_id)
        if not edge:
            is_trace_forward = False
            edge_id = (to_proc, from_proc)
            edge = dic_edges.get(edge_id)

        self_sensor_keys, target_sensor_keys = gen_sql_proc_link_key_from_trace_keys(edge)

        from_link_keys = None
        if self_sensor_keys and all(self_sensor_keys):
            from_link_keys = self_sensor_keys

        to_link_keys = None
        if target_sensor_keys and all(target_sensor_keys):
            to_link_keys = target_sensor_keys

        if is_trace_forward:
            edge_cols = (from_link_keys, to_link_keys)
        else:
            edge_id = tuple(reversed(edge_id))
            edge_cols = (to_link_keys, from_link_keys)

        for idx, proc_id in enumerate(edge_id):
            if short_procs and proc_id not in short_procs:
                continue

            link_keys = edge_cols[idx]
            if not link_keys:
                continue

            # same proc will join by cycle id ( as a bridge to another process)
            trans_data: TransactionData = dic_processes[proc_id]
            if sql_objs and sql_objs[-1].process_id == proc_id:
                if sql_objs[-1].link_keys != link_keys:
                    sql_objs[-1].next_link_keys = link_keys
            else:
                end_proc = None
                for proc in end_procs:
                    if proc.proc_id == proc_id:
                        end_proc = proc
                        break

                proc_link_sql = SqlProcLink(transaction_data=trans_data)
                proc_link_sql.select_col_ids = end_proc.col_ids if end_proc else []
                proc_link_sql.select_col_names = (
                    [trans_data.get_column_name(_id) for _id in end_proc.col_ids] if end_proc else []
                )
                proc_link_sql.link_keys = link_keys
                proc_link_sql.next_link_keys = []
                proc_link_sql.start_tm = start_tm if proc_id == start_proc else end_proc_start_tm
                proc_link_sql.end_tm = end_tm if proc_id == start_proc else end_proc_end_tm
                sql_objs.append(proc_link_sql)

    return sql_objs


def gen_exists_query(prev_table_name, prev_col):
    table_name = 't_proc_link'
    col = 'link_value'
    sql = f' AND EXISTS (SELECT 1 FROM {prev_table_name} WHERE {prev_table_name}.{prev_col} = {table_name}.{col})'
    return sql


def gen_sensor_time_range(db_instance, temp_table_name):
    time_ranges = []
    sql = f"SELECT DISTINCT date_trunc('day', time) FROM {temp_table_name}"
    _, rows = db_instance.run_sql(sql, row_is_dict=False)
    if not rows:
        return time_ranges

    time_col = 'time'
    df = pd.DataFrame(rows, columns=[time_col])

    count_col = 'count_col'
    df[count_col] = 0
    # df.set_index(time_col, inplace=True)

    # df = df.groupby(pd.Grouper(key=time_col, freq='M'))[count_col].sum().reset_index()
    # min_val = int(df[count_col].min())
    # max_val = int(df[count_col].max())
    dic_days = df.groupby(pd.Grouper(key=time_col, freq='MS', sort=True))[time_col].apply(list).to_dict()
    for month, days in dic_days.items():
        for day in days:
            from_dt = convert_time(day)
            to_dt = convert_time(add_days(day, days=1))
            if time_ranges and time_ranges[-1][1] == from_dt:
                time_ranges[-1] = (time_ranges[-1][0], to_dt)
            else:
                time_ranges.append((from_dt, to_dt))

    return time_ranges


@log_execution_time()
def gen_trace_procs_df_detail(
    db_instance,
    list_sql_objs: List[List[SqlProcLink]],
    cond_procs: List[ConditionProc],
    duplicate_serial_show: DuplicateSerialShow,
    for_count: bool = False,
) -> Tuple[DataFrame, int, int]:
    df = None

    for sql_objs in list_sql_objs:
        sql, params = gen_proc_link_from_sql(sql_objs, cond_procs, duplicate_serial_show, for_count=for_count)
        cols, rows = db_instance.run_sql(sql, params=params, row_is_dict=False)
        _df = pd.DataFrame(rows, columns=cols)
        keep = 'last'
        if duplicate_serial_show is DuplicateSerialShow.SHOW_FIRST:
            keep = 'first'

        _filter_subset = [TransactionData.id_col_name] if TransactionData.id_col_name in _df.columns else ['marker_0']
        _df.drop_duplicates(subset=_filter_subset, keep=keep, inplace=True)

        if duplicate_serial_show is not DuplicateSerialShow.SHOW_BOTH and not for_count:
            # TODO: drop_duplicates_by_link_keys MUST delete per end proc
            dropped_duplicates_df = DropDuplicatesTraceProcs.drop_duplicates_by_link_keys(
                _df,
                sql_objs,
                duplicate_serial_show,
            )
            _df = dropped_duplicates_df

        if df is None:
            df = _df
            continue

        _df_cols = _df.columns.difference(df.columns).tolist()
        if TransactionData.id_col_name in df.columns:
            df = df.merge(_df[[TransactionData.id_col_name] + _df_cols], on=TransactionData.id_col_name)

    if df is None:
        return pd.DataFrame(), 0, 0

    return df


class DropDuplicatesTraceProcs:
    @staticmethod
    @log_execution_time()
    def drop_duplicates_by_ids(df: DataFrame, sql_objs: List[SqlProcLink]) -> DataFrame:
        end_sql_obj = sql_objs[-1]
        end_time_col = end_sql_obj.gen_proc_time_label(is_start_proc=len(sql_objs) == 1)
        return df.sort_values(end_time_col).drop_duplicates(subset=[TransactionData.id_col_name], keep='last')

    @staticmethod
    @log_execution_time()
    def drop_duplicates_by_link_keys(
        df: DataFrame,
        sql_objs: List[SqlProcLink],
        duplicate_serial_show: DuplicateSerialShow,
    ) -> DataFrame:
        # sort by times
        time_cols = [sql_obj.gen_proc_time_label(is_start_proc=idx == 0) for idx, sql_obj in enumerate(sql_objs)]
        link_cols = set()
        for sql_obj in sql_objs:
            link_cols.update(sql_obj.all_link_keys_labels)
        link_cols = link_cols & set(df.columns)

        if not link_cols:
            return df

        keep = 'last' if duplicate_serial_show is DuplicateSerialShow.SHOW_LAST else 'first'
        return df.sort_values(time_cols).drop_duplicates(subset=link_cols, keep=keep)


@log_execution_time()
def is_nominal_check(col_id, graph_param):
    if graph_param.common.nominal_vars is not None:
        return col_id in graph_param.common.nominal_vars
    else:
        return graph_param.common.is_nominal_scale


@log_execution_time()
@memoize(cache_type=CacheType.CONFIG_DATA)
def check_path_exist(end_proc_id, start_proc_id):
    trace_graph = get_trace_configs()
    directed_paths = trace_graph.get_all_paths(start_proc_id, end_proc_id)
    undirected_paths = trace_graph.get_all_paths(start_proc_id, end_proc_id, undirected_graph=True)
    return len(directed_paths) or len(undirected_paths)


@memoize(cache_type=CacheType.CONFIG_DATA)
def get_trace_configs():
    proc_trace_schema = TraceSchema(many=True)
    cfg_traces = CfgTrace.get_all()
    traces = proc_trace_schema.dump(cfg_traces, many=True)
    traces = [DictToClass(**trace) for trace in traces]
    trace_graph = TraceGraph(traces)
    return trace_graph
