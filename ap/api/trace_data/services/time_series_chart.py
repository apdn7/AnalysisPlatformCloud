from __future__ import annotations

from copy import deepcopy
from math import ceil

import numpy as np
import pandas as pd
from pandas import DataFrame

from ap.api.common.services.show_graph_services import (
    add_master_information_for_hovering,
    add_master_mapping_for_rank_values,
    add_serials_to_thin_df,
    calc_raw_common_scale_y,
    calc_scale_info,
    convert_datetime_to_ct,
    copy_dic_param_to_thin_dic_param,
    customize_dic_param_for_reuse_cache,
    extract_master_id_from_df,
    filter_cat_dict_common,
    gen_before_rank_dict,
    gen_category_info,
    gen_df,
    gen_dic_data,
    gen_dic_data_from_df,
    gen_dic_param,
    gen_dic_serial_data_from_df_thin,
    gen_kde_data_trace_data,
    gen_rank_cols_to_master_ids_dict,
    gen_thin_df_cat_exp,
    gen_unique_data,
    get_chart_info_detail,
    get_filter_on_demand_data,
    get_serials_and_date_col,
    reduce_data,
    retrieve_order_setting,
    set_str_rank_to_dic_param,
    sort_df_by_x_option,
)
from ap.common.common_utils import (
    end_of_minute,
    gen_sql_label,
    start_of_minute,
)
from ap.common.constants import (
    ARRAY_PLOTDATA,
    ARRAY_X,
    ARRAY_Y,
    ARRAY_Y_MAX,
    ARRAY_Y_MIN,
    CAT_EXP_BOX,
    CAT_TOTAL,
    CAT_UNIQUE_LIMIT,
    CATE_PROCS,
    CATEGORY_DATA,
    CHART_INFOS,
    CHART_INFOS_ORG,
    COL_ID,
    COMMON,
    COMMON_INFO,
    CYCLE_ID,
    CYCLE_IDS,
    DATETIME_COL,
    DF_ALL_COLUMNS,
    DF_ALL_PROCS,
    DIC_STR_COLS,
    END_COL_ID,
    END_COL_NAME,
    END_PROC_ID,
    ID,
    INDEX_ORDER_COLS,
    IS_CAT_LIMITED,
    IS_GRAPH_LIMITED,
    IS_OVER_UNIQUE_LIMIT,
    MAX_CATEGORY_SHOW,
    NONE_IDXS,
    RANK_COL,
    ROWID,
    SERIAL_COLUMNS,
    SERIAL_DATA,
    SLOT_COUNT,
    SLOT_FROM,
    SLOT_TO,
    STRING_COL_IDS,
    SUMMARIES,
    THIN_DATA_CHUNK,
    THIN_DATA_COUNT,
    THIN_DATA_GROUP_COUNT,
    TIME_COL,
    TIMES,
    UNIQUE_CATEGORIES,
    CacheType,
)
from ap.common.logger import log_execution_time
from ap.common.memoize import memoize
from ap.common.services.form_env import bind_dic_param_to_class
from ap.common.services.request_time_out_handler import (
    abort_process_handler,
    request_timeout_handling,
)
from ap.common.services.statistics import calc_summaries
from ap.common.trace_data_log import (
    EventAction,
    EventType,
    Target,
    TraceErrKey,
    trace_log,
)
from ap.trace_data.schemas import DicParam

FPP_MAX_GRAPH = 20


# @grpc_api(show_graph=True)
def show_graph_fpp(dic_param, __FORCE_OFFLINE_MODE__=False, __IF_DISCONNECT_RUN_OFFLINE_MODE__=True):
    return gen_graph_fpp(dic_param, FPP_MAX_GRAPH)


@log_execution_time('[TRACE DATA]')
@request_timeout_handling()
@abort_process_handler()
@trace_log(
    (TraceErrKey.TYPE, TraceErrKey.ACTION, TraceErrKey.TARGET),
    (EventType.FPP, EventAction.PLOT, Target.GRAPH),
    send_ga=True,
)
@memoize(is_save_file=True, cache_type=CacheType.TRANSACTION_DATA)
def gen_graph_fpp(graph_param, dic_param, max_graph=None, df=None):
    (
        dic_param,
        cat_exp,
        cat_procs,
        dic_cat_filters,
        use_expired_cache,
        temp_serial_column,
        temp_serial_order,
        temp_serial_process,
        temp_x_option,
        *_,
    ) = customize_dic_param_for_reuse_cache(dic_param)

    dic_proc_cfgs = graph_param.dic_proc_cfgs

    # in case of jump from other page
    if df is None:
        dic_param, df, graph_param_with_cate = gen_df(
            graph_param,
            dic_param,
            dic_cat_filters,
            rank_value=True,
            use_expired_cache=use_expired_cache,
        )
    else:
        graph_param_with_cate = graph_param

    convert_datetime_to_ct(df, graph_param)

    # use for enable and disable index columns
    all_procs = []
    all_cols = []
    for proc in graph_param.array_formval:
        all_procs.append(proc.proc_id)
        all_cols.extend(proc.col_ids)

    dic_param[COMMON][DF_ALL_PROCS] = all_procs
    dic_param[COMMON][DF_ALL_COLUMNS] = all_cols

    # for ondemand filter
    if cat_exp:
        for i, val in enumerate(cat_exp):
            dic_param[COMMON][f'{CAT_EXP_BOX}{i + 1}'] = val
    if cat_procs:
        dic_param[COMMON][CATE_PROCS] = cat_procs

    # order index with other param
    df, dic_param = sort_df_by_x_option(
        df,
        dic_param,
        graph_param,
        dic_proc_cfgs,
        temp_x_option,
        temp_serial_process,
        temp_serial_column,
        temp_serial_order,
    )

    dic_param = filter_cat_dict_common(df, dic_param, cat_exp, cat_procs, graph_param, True)

    graph_param = bind_dic_param_to_class(
        graph_param.dic_proc_cfgs,
        graph_param.trace_graph,
        graph_param.dic_card_orders,
        dic_param,
    )

    dic_unique_cate = gen_unique_data(df, dic_proc_cfgs, graph_param.common.cate_col_ids, True)

    # reset index (keep sorted position)
    df.reset_index(inplace=True, drop=True)

    df, master_df = extract_master_id_from_df(df, graph_param)

    str_cols = dic_param.get(STRING_COL_IDS)
    dic_str_cols = dic_param.get(DIC_STR_COLS, {})
    dic_ranks = gen_before_rank_dict(df, dic_str_cols)
    dic_rank_cols_to_master_ids = gen_rank_cols_to_master_ids_dict(df, master_df, graph_param)

    dic_data, is_graph_limited = gen_dic_data(dic_proc_cfgs, df, graph_param, graph_param_with_cate, max_graph)
    dic_param[IS_GRAPH_LIMITED] = is_graph_limited

    is_thin_data = False
    # 4000 chunks x 3 values(min,median,max)
    dic_thin_param = None
    if len(df) > THIN_DATA_COUNT:
        is_thin_data = True
        dic_thin_param = deepcopy(dic_param)

    dic_param = gen_dic_param(graph_param, df, dic_param, dic_data)
    gen_dic_serial_data_from_df(df, dic_proc_cfgs, dic_param)

    # calculate_summaries
    calc_summaries(dic_param)

    # calc common scale y min max
    min_max_list, all_graph_min, all_graph_max = calc_raw_common_scale_y(dic_param.get(ARRAY_PLOTDATA, []), str_cols)

    # get min max order columns
    output_orders = []
    x_option = graph_param.common.x_option
    if x_option == 'INDEX' and graph_param.common.serial_columns:
        group_col = '__group_col__'
        dic_cfg_cols = {cfg_col.id: cfg_col for cfg_col in graph_param.get_col_cfgs(graph_param.common.serial_columns)}
        dic_order_cols = {}
        for order_col_id in graph_param.common.serial_columns:
            cfg_col = dic_cfg_cols.get(order_col_id)
            if not cfg_col:
                continue

            sql_label = gen_sql_label(RANK_COL, cfg_col.id, cfg_col.column_name)
            if sql_label not in df.columns:
                sql_label = gen_sql_label(cfg_col.id, cfg_col.column_name)
                if sql_label not in df.columns:
                    continue

            dic_order_cols[sql_label] = cfg_col

        df_order = df[dic_order_cols]
        if is_thin_data:
            count_per_group = ceil(len(df_order) / THIN_DATA_CHUNK)
            df_order[group_col] = df_order.index // count_per_group
            df_order = df_order.dropna().groupby(group_col).agg(['min', 'max'])
            for sql_label, col in dic_order_cols.items():
                output_orders.append(
                    {
                        'name': col.shown_name,
                        'min': df_order[(sql_label, 'min')].tolist(),
                        'max': df_order[(sql_label, 'max')].tolist(),
                        'id': col.id,
                    },
                )
        else:
            for sql_label, col in dic_order_cols.items():
                output_orders.append({'name': col.shown_name, 'value': df_order[sql_label].tolist(), 'id': col.id})

    full_arrays = None
    if is_thin_data:
        full_arrays = make_str_full_array_y(dic_param)
        list_summaries = get_summary_infos(dic_param)
        dic_cat_exp_labels = None
        if graph_param.common.cat_exp:
            df, dic_cat_exp_labels = gen_thin_df_cat_exp(dic_param)
        else:
            add_serials_to_thin_df(dic_param, df)

        copy_dic_param_to_thin_dic_param(dic_param, dic_thin_param)
        dic_param = gen_thin_dic_param(graph_param, df, dic_thin_param, dic_cat_exp_labels, dic_ranks)
        dic_param['is_thin_data'] = is_thin_data

        for i, plot in enumerate(dic_param[ARRAY_PLOTDATA]):
            plot[SUMMARIES] = list_summaries[i]
    else:
        dic_param = gen_category_info(dic_param, dic_ranks)
    set_str_rank_to_dic_param(dic_param, dic_ranks, dic_str_cols, full_arrays)
    set_str_category_data(dic_param, dic_ranks)

    calc_scale_info(
        dic_proc_cfgs,
        dic_param[ARRAY_PLOTDATA],
        min_max_list,
        all_graph_min,
        all_graph_max,
        str_cols,
    )

    # kde
    gen_kde_data_trace_data(dic_param, full_arrays)

    # add unique category values
    for dic_cate in dic_param.get(CATEGORY_DATA) or []:
        col_id = dic_cate[COL_ID]
        dic_cate[UNIQUE_CATEGORIES] = dic_unique_cate[col_id][UNIQUE_CATEGORIES] if dic_unique_cate.get(col_id) else []
        if len(dic_cate.get('data', pd.Series()).unique()) > CAT_UNIQUE_LIMIT:
            dic_cate[IS_OVER_UNIQUE_LIMIT] = True
        else:
            dic_cate[IS_OVER_UNIQUE_LIMIT] = False

    # dic_param[CAT_EXP_BOX] = cat_exp_list
    dic_param[INDEX_ORDER_COLS] = output_orders
    dic_param['proc_name'] = {k: proc.shown_name for (k, proc) in dic_proc_cfgs.items()}

    # get order column data
    retrieve_order_setting(dic_proc_cfgs, dic_param)

    dic_param = get_filter_on_demand_data(dic_param, remove_filter_data=True)

    add_master_information_for_hovering(master_df, dic_param, graph_param)
    add_master_mapping_for_rank_values(dic_param, dic_rank_cols_to_master_ids)

    return dic_param


@log_execution_time()
def set_str_category_data(dic_param, dic_ranks):
    for dic_cate in dic_param.get(CATEGORY_DATA, []):
        col_id = dic_cate.get(COL_ID)
        if col_id not in dic_ranks:
            continue

        dic_cate['data'] = pd.Series(dic_cate.get('data')).map(dic_ranks[col_id])


@log_execution_time()
def gen_thin_dic_param(graph_param, df, dic_param, dic_cat_exp_labels=None, dic_ranks=None):
    # bind dic_param
    dic_datetime_serial_cols = get_serials_and_date_col(graph_param)
    dic_str_cols = dic_param.get(DIC_STR_COLS, {})
    (
        df_thin,
        dic_cates,
        dic_org_cates,
        group_counts,
        df_from_to_count,
        dic_min_med_max,
    ) = reduce_data(df, graph_param, dic_str_cols)

    # create output data
    df_cat_exp = gen_df_thin_values(df, graph_param, df_thin, dic_str_cols, df_from_to_count, dic_min_med_max)
    dic_data = gen_dic_data_from_df(
        df_cat_exp,
        graph_param,
        cat_exp_mode=True,
        dic_cat_exp_labels=dic_cat_exp_labels,
    )
    dic_param = gen_dic_param(
        graph_param,
        df_cat_exp,
        dic_param,
        dic_data,
        dic_cates,
        dic_org_cates,
        is_get_chart_infos=False,
    )
    gen_dic_serial_data_from_df_thin(df_cat_exp, dic_param, dic_datetime_serial_cols, dic_ranks)

    # get start proc time
    start_tm = start_of_minute(graph_param.common.start_date, graph_param.common.start_time)
    end_tm = end_of_minute(graph_param.common.end_date, graph_param.common.end_time)
    threshold_filter_detail_ids = graph_param.common.threshold_boxes

    # gen min max for thin data
    for plot in dic_param[ARRAY_PLOTDATA]:
        sql_label = gen_sql_label(plot[END_COL_ID], plot[END_COL_NAME], plot.get(CAT_EXP_BOX))
        time_label = gen_sql_label(TIMES, sql_label)
        min_label = gen_sql_label(ARRAY_Y_MIN, sql_label)
        max_label = gen_sql_label(ARRAY_Y_MAX, sql_label)
        cycle_label = gen_sql_label(CYCLE_IDS, sql_label)
        sql_label_from = gen_sql_label(SLOT_FROM, sql_label)
        sql_label_to = gen_sql_label(SLOT_TO, sql_label)
        sql_label_count = gen_sql_label(SLOT_COUNT, sql_label)

        if time_label in df_cat_exp.columns:
            plot[ARRAY_X] = df_cat_exp[time_label]
            # get chart infos
            plot[CHART_INFOS_ORG], plot[CHART_INFOS] = get_chart_info_detail(
                graph_param.dic_proc_cfgs,
                plot[ARRAY_X],
                plot[END_COL_ID],
                threshold_filter_detail_ids,
                plot[END_PROC_ID],
                graph_param.common.start_proc,
                start_tm,
                end_tm,
                dic_param[TIMES],
            )

        if min_label in df_cat_exp.columns:
            plot[ARRAY_Y_MIN] = df_cat_exp[min_label].replace(
                {pd.NA: 'NA', float('inf'): 'inf', float('-inf'): '-inf', np.nan: 'NA'},
            )

        if max_label in df_cat_exp.columns:
            plot[ARRAY_Y_MAX] = df_cat_exp[max_label].replace(
                {pd.NA: 'NA', float('inf'): 'inf', float('-inf'): '-inf', np.nan: 'NA'},
            )

        if cycle_label in df_cat_exp.columns:
            plot[CYCLE_IDS] = df_cat_exp[cycle_label]

        if sql_label_from in df_cat_exp.columns:
            plot[SLOT_FROM] = df_cat_exp[sql_label_from]

        if sql_label_to in df_cat_exp.columns:
            plot[SLOT_TO] = df_cat_exp[sql_label_to]

        if sql_label_count in df_cat_exp.columns:
            plot[SLOT_COUNT] = df_cat_exp[sql_label_count]

        if plot[END_COL_ID] in dic_ranks:
            # category variable
            p_array_y = pd.Series(plot[ARRAY_Y]).dropna()
            cat_size = 0
            if len(p_array_y):
                cat_size = np.unique(p_array_y).size
            plot[CAT_TOTAL] = cat_size
            plot[IS_CAT_LIMITED] = cat_size >= MAX_CATEGORY_SHOW

        # ignore show none value in thin mode
        plot[NONE_IDXS] = None

    # group count
    dic_param[THIN_DATA_GROUP_COUNT] = group_counts

    return dic_param


def make_str_full_array_y(dic_param):
    return [plot[ARRAY_Y] for plot in dic_param[ARRAY_PLOTDATA]]


def get_summary_infos(dic_param):
    return [plot[SUMMARIES] for plot in dic_param[ARRAY_PLOTDATA]]


@log_execution_time()
def gen_df_thin_values(df: DataFrame, graph_param: DicParam, df_thin, dic_str_cols, df_from_to_count, dic_min_med_max):
    thin_idxs_len = len(df_thin)
    thin_boxes = [None] * thin_idxs_len
    df_cat_exp = pd.DataFrame({TIME_COL: thin_boxes}, index=df_thin.index)

    df_cat_exp[TIME_COL] = df_thin[TIME_COL]
    if CAT_EXP_BOX in df_thin.columns:
        df_cat_exp[CAT_EXP_BOX] = df_thin[CAT_EXP_BOX]

    is_time_blank = True
    series = pd.Series(thin_boxes, index=df_thin.index)
    for proc in graph_param.array_formval:
        orig_sql_label_serial = gen_sql_label(SERIAL_DATA, proc.proc_id)
        time_col_alias = '{}_{}'.format(TIME_COL, proc.proc_id)

        for col_id, col_name in zip(proc.col_ids, proc.col_names):
            col_id_name = gen_sql_label(col_id, col_name)
            cols_in_df = [col for col in df_thin.columns if col.startswith(col_id_name)]
            target_col_info = dic_str_cols.get(col_id_name)
            for sql_label in cols_in_df:
                sql_label_min = gen_sql_label(ARRAY_Y_MIN, sql_label)
                sql_label_max = gen_sql_label(ARRAY_Y_MAX, sql_label)
                sql_label_cycle = gen_sql_label(CYCLE_IDS, sql_label)
                sql_label_serial = gen_sql_label(SERIAL_DATA, sql_label)
                sql_label_time = gen_sql_label(TIMES, sql_label)
                sql_label_from = gen_sql_label(SLOT_FROM, sql_label)
                sql_label_to = gen_sql_label(SLOT_TO, sql_label)
                sql_label_count = gen_sql_label(SLOT_COUNT, sql_label)
                idxs = df_thin[sql_label].notnull()

                if not len(idxs) or not len(df_thin[idxs]):
                    df_cat_exp[sql_label] = thin_boxes
                    df_cat_exp[sql_label_min] = thin_boxes
                    df_cat_exp[sql_label_max] = thin_boxes
                    continue

                # before rank
                if target_col_info:
                    rows = df_thin[sql_label]
                    df_cat_exp[sql_label] = rows
                    df_cat_exp[sql_label_min] = thin_boxes
                    df_cat_exp[sql_label_max] = thin_boxes
                    continue

                med_idxs = list(df_thin.loc[idxs, sql_label])
                df_cat_exp[sql_label] = dic_min_med_max[sql_label]['median']

                # flag
                is_time_blank = False

                # time start proc
                if TIME_COL in df.columns:
                    series[:] = None
                    series[idxs] = df.loc[med_idxs, TIME_COL].values
                    df_cat_exp[TIME_COL] = np.where(series.isnull(), df_cat_exp[TIME_COL], series)

                # time end proc
                if time_col_alias in df.columns:
                    series[:] = None
                    series[idxs] = df.loc[med_idxs, time_col_alias].tolist()
                    df_cat_exp[sql_label_time] = series

                # cycle ids
                if ROWID in df.columns:
                    series[:] = None
                    series[idxs] = df.loc[med_idxs, ROWID].values
                    df_cat_exp[sql_label_cycle] = series

                # serial ids
                if orig_sql_label_serial in df.columns:
                    series[:] = None
                    series[idxs] = df.loc[med_idxs, orig_sql_label_serial].values
                    df_cat_exp[sql_label_serial] = series

                df_cat_exp[sql_label_min] = dic_min_med_max[sql_label]['min']

                df_cat_exp[sql_label_max] = dic_min_med_max[sql_label]['max']
                df_cat_exp[sql_label_from] = df_from_to_count['min']
                df_cat_exp[sql_label_to] = df_from_to_count['max']
                df_cat_exp[sql_label_count] = df_from_to_count['count']

    if is_time_blank:
        # time start proc
        if TIME_COL in df_thin.columns:
            df_cat_exp[TIME_COL] = df_thin[TIME_COL]

        # cycle ids
        if ID in df_thin.columns:
            df_cat_exp[CYCLE_IDS] = df_thin[ID]
        elif CYCLE_ID in df_thin.columns:
            df_cat_exp[CYCLE_IDS] = df_thin[CYCLE_ID]

    return df_cat_exp


@log_execution_time()
def gen_dic_serial_data_from_df(df: DataFrame, dic_proc_cfgs, dic_param):
    dic_param[SERIAL_DATA] = {}
    dic_param[COMMON_INFO] = {}
    for proc_id, proc_cfg in dic_proc_cfgs.items():
        serial_cols = proc_cfg.get_serials(column_name_only=False)
        datetime_col = proc_cfg.get_date_col(column_name_only=False)
        if datetime_col:
            datetime_col = datetime_col.shown_name
        sql_labels = [gen_sql_label(serial_col.id, serial_col.column_name) for serial_col in serial_cols]
        before_rank_sql_labels = [gen_sql_label(RANK_COL, sql_label) for sql_label in sql_labels]
        serial_cols = [serial_col.shown_name for serial_col in serial_cols]
        dic_param[COMMON_INFO][proc_id] = {
            DATETIME_COL: datetime_col or '',
            SERIAL_COLUMNS: serial_cols,
        }
        cols = []
        for sql_label, before_rank_label in zip(sql_labels, before_rank_sql_labels):
            if before_rank_label in df.columns:
                cols.append(before_rank_label)
            else:
                cols.append(sql_label)

        is_not_exist = set(cols) - set(df.columns)
        if not is_not_exist and cols:
            dic_param[SERIAL_DATA][proc_id] = df[cols].replace({np.nan: ''}).to_records(index=False).tolist()
        else:
            dic_param[SERIAL_DATA][proc_id] = []
