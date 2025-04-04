import math
import re
from collections import Counter
from copy import deepcopy
from typing import List

import numpy as np
import pandas as pd
from numpy import matrix
from pandas import DataFrame, Index, RangeIndex

from ap import max_graph_config
from ap.api.categorical_plot.services import (
    gen_dic_param_terms,
    gen_time_conditions,
    produce_cyclic_terms,
)
from ap.api.common.services.show_graph_services import (
    add_master_information_for_hovering_scp,
    calc_raw_common_scale_y,
    calc_scale_info,
    convert_datetime_to_ct,
    customize_dic_param_for_reuse_cache,
    extract_master_id_from_df,
    filter_cat_dict_common,
    gen_group_filter_list,
    get_chart_info_detail,
    get_data_from_db,
    get_filter_on_demand_data,
    get_serial_and_datetime_data,
    is_categorical_col,
    main_check_filter_detail_match_graph_data,
)
from ap.common.common_utils import (
    add_zero_padding_df,
    convert_to_d3_format,
    gen_sql_label,
    get_format_padding,
)
from ap.common.constants import (
    ACTUAL_RECORD_NUMBER,
    ARRAY_PLOTDATA,
    ARRAY_X,
    ARRAY_X_MASTER,
    ARRAY_Y,
    ARRAY_Y_MASTER,
    ARRAY_Z,
    CHART_INFOS,
    CHART_TYPE,
    COLOR,
    COLORS,
    COMMON,
    CYCLE_IDS,
    CYCLIC_DIV_NUM,
    DATETIME,
    ELAPSED_TIME,
    END_COL_ID,
    END_DATE,
    END_PROC_ID,
    END_TM,
    H_LABEL,
    HEATMAP_MATRIX,
    IS_DATA_LIMITED,
    IS_RESAMPLING,
    MATCHED_FILTER_IDS,
    N_TOTAL,
    NOT_EXACT_MATCH_FILTER_IDS,
    ORIG_ARRAY_Z,
    ROWID,
    SCALE_AUTO,
    SCALE_COLOR,
    SCALE_COMMON,
    SCALE_FULL,
    SCALE_SETTING,
    SCALE_THRESHOLD,
    SCALE_X,
    SCALE_Y,
    SERIALS,
    SORT_KEY,
    START_DATE,
    START_PROC,
    START_TM,
    SUMMARIES,
    TIME_COL,
    TIME_MAX,
    TIME_MIN,
    TIME_NUMBERINGS,
    TIMES,
    UNIQUE_SERIAL,
    UNMATCHED_FILTER_IDS,
    V_LABEL,
    VAR_TRACE_TIME,
    X_ID,
    X_IS_MASTER,
    X_LABEL,
    X_NAME,
    X_SERIAL,
    X_THRESHOLD,
    X_USER_FORMAT,
    Y_ID,
    Y_IS_MASTER,
    Y_LABEL,
    Y_NAME,
    Y_SERIAL,
    Y_THRESHOLD,
    Y_USER_FORMAT,
    CacheType,
    ChartType,
    ColorOrder,
    DataType,
    MaxGraphNumber,
)
from ap.common.logger import log_execution_time
from ap.common.memoize import memoize
from ap.common.services.ana_inf_data import resample_preserve_min_med_max
from ap.common.services.form_env import bind_dic_param_to_class
from ap.common.services.request_time_out_handler import (
    abort_process_handler,
    request_timeout_handling,
)
from ap.common.services.sse import MessageAnnouncer
from ap.common.services.statistics import calc_summary_elements
from ap.common.sigificant_digit import get_fmt_from_array
from ap.common.trace_data_log import EventAction, EventType, Target, TraceErrKey, trace_log
from ap.setting_module.models import CfgProcessColumn
from ap.trace_data.schemas import DicParam
from grpc_server.services.grpc_service_proxy import grpc_api

DATA_COUNT_COL = '__data_count_col__'
MATRIX = 7
SCATTER_PLOT_TOTAL_POINT = 50_000
SCATTER_PLOT_MAX_POINT = 10_000
HEATMAP_COL_ROW = 100
TOTAL_VIOLIN_PLOT = 200


@grpc_api(show_graph=True)
def show_scatter_plot(
    graph_param,
    dic_param,
    df=None,
    __FORCE_OFFLINE_MODE__=False,
    __IF_DISCONNECT_RUN_OFFLINE_MODE__=True,
):
    return gen_scatter_plot(graph_param, dic_param, df)


@log_execution_time('[SCATTER PLOT]')
@request_timeout_handling()
@abort_process_handler()
@MessageAnnouncer.notify_progress(60)
@trace_log(
    (TraceErrKey.TYPE, TraceErrKey.ACTION, TraceErrKey.TARGET),
    (EventType.SCP, EventAction.PLOT, Target.GRAPH),
    send_ga=True,
)
@memoize(is_save_file=True, cache_type=CacheType.TRANSACTION_DATA)
def gen_scatter_plot(root_graph_param: DicParam, dic_param, df=None):
    """tracing data to show graph
    1 start point x n end point
    filter by condition points that between start point and end_point
    """
    recent_flg = False
    for key in dic_param[COMMON]:
        if str(key).startswith(VAR_TRACE_TIME) and dic_param[COMMON][key] == 'recent':
            recent_flg = True
            break

    is_data_limited = False
    # for caching
    (
        dic_param,
        cat_exp,
        _,
        dic_cat_filters,
        use_expired_cache,
        temp_serial_column,
        temp_serial_order,
        *_,
        matrix_col,
        color_order,
    ) = customize_dic_param_for_reuse_cache(dic_param)
    matrix_col = matrix_col if matrix_col else MATRIX

    # cyclic
    terms = None
    if dic_param[COMMON].get(CYCLIC_DIV_NUM):
        produce_cyclic_terms(dic_param)
        terms = gen_dic_param_terms(dic_param)

    # get x,y,color, levels, cat_div information
    orig_graph_param = bind_dic_param_to_class(
        root_graph_param.dic_proc_cfgs,
        root_graph_param.trace_graph,
        root_graph_param.dic_card_orders,
        dic_param,
    )
    threshold_filter_detail_ids = orig_graph_param.common.threshold_boxes
    scatter_xy_ids = []
    scatter_xy_names = []
    scatter_proc_ids = []
    for proc in orig_graph_param.array_formval:
        scatter_proc_ids.append(proc.proc_id)
        scatter_xy_ids = scatter_xy_ids + proc.col_ids
        scatter_xy_names = scatter_xy_names + proc.col_names

    x_proc_id = scatter_proc_ids[0]
    y_proc_id = scatter_proc_ids[-1]
    x_id = scatter_xy_ids[0]
    y_id = scatter_xy_ids[-1]
    x_name = scatter_xy_names[0]
    y_name = scatter_xy_names[-1]
    x_label = gen_sql_label(x_id, x_name)
    y_label = gen_sql_label(y_id, y_name)
    # x_cfg_proc_column: CfgProcessColumn = CfgProcessColumn.get_by_id(x_id)
    x_cfg_proc_column: CfgProcessColumn = root_graph_param.get_col_cfg(x_id)
    # y_cfg_proc_column: CfgProcessColumn = CfgProcessColumn.get_by_id(y_id)
    y_cfg_proc_column: CfgProcessColumn = root_graph_param.get_col_cfg(y_id)
    x_is_master = x_cfg_proc_column.is_master_data_column()
    y_is_master = y_cfg_proc_column.is_master_data_column()
    x_master_col = x_cfg_proc_column.master_data_column_id_label()
    y_master_col = y_cfg_proc_column.master_data_column_id_label()
    x_user_fmt = x_cfg_proc_column.format
    y_user_fmt = y_cfg_proc_column.format

    color_id = orig_graph_param.common.color_var
    cat_div_id = orig_graph_param.common.div_by_cat
    level_ids = cat_exp if cat_exp else orig_graph_param.common.cat_exp
    col_ids = [col for col in list(set([x_id, y_id, color_id, cat_div_id] + level_ids)) if col]
    cfg_process_cols = root_graph_param.get_col_cfgs(col_ids)
    dic_cols = {cfg_col.id: cfg_col for cfg_col in cfg_process_cols}
    for cfg_col in cfg_process_cols:
        dic_cols[cfg_col.id] = cfg_col

    color_label = gen_sql_label(color_id, dic_cols[color_id].column_name) if color_id else None
    fmt_cfg_proc_col = root_graph_param.get_col_cfg(color_id)
    color_label_fmt = fmt_cfg_proc_col.format if fmt_cfg_proc_col else None
    level_labels = [gen_sql_label(id, dic_cols[id].column_name) for id in level_ids]
    cat_div_label = None
    cat_div_fmt = None
    if cat_div_id:
        cat_div_label = gen_sql_label(cat_div_id, dic_cols[cat_div_id].column_name)
        cat_div_fmt = convert_to_d3_format(dic_cols[cat_div_id].format)
    # cat_div_label = gen_sql_label(cat_div_id, dic_cols[cat_div_id].column_name) if cat_div_id else None
    # cat_div_fmt = CfgProcessColumn.get_fmt_by_id(cat_div_id) if cat_div_id else None

    chart_type = None
    x_category = False
    y_category = False
    if orig_graph_param.common.compare_type == 'directTerm':
        matched_filter_ids = []
        unmatched_filter_ids = []
        not_exact_match_filter_ids = []
        actual_record_number = 0
        unique_serial = 0

        dic_dfs = {}
        terms = gen_time_conditions(dic_param)
        if df is None:
            for term in terms:
                # create dic_param for each term from original dic_param
                term_dic_param = deepcopy(dic_param)
                term_dic_param[COMMON][START_DATE] = term[START_DATE]
                term_dic_param[COMMON][START_TM] = term[START_TM]
                term_dic_param[COMMON][END_DATE] = term[END_DATE]
                term_dic_param[COMMON][END_TM] = term[END_TM]
                h_keys = (term[START_DATE], term[START_TM], term[END_DATE], term[END_TM])

                # query data and gen df
                df_term, graph_param, record_number, _is_res_limited = gen_df(
                    orig_graph_param,
                    term_dic_param,
                    dic_cat_filters,
                    _use_expired_cache=use_expired_cache,
                )

                convert_datetime_to_ct(df_term, graph_param)

                df = df_term.copy() if df is None else pd.concat([df, df_term])

                chart_type, x_category, y_category = get_chart_type(df, x_id, y_id, dic_cols)

                if _is_res_limited is None:
                    unique_serial = None
                if unique_serial is not None:
                    unique_serial += _is_res_limited

                # check filter match or not ( for GUI show )
                filter_ids = main_check_filter_detail_match_graph_data(graph_param, df_term)

                # matched_filter_ids, unmatched_filter_ids, not_exact_match_filter_ids, actual records
                actual_record_number += record_number
                matched_filter_ids += filter_ids[0]
                unmatched_filter_ids += filter_ids[1]
                not_exact_match_filter_ids += filter_ids[2]

                dic_dfs[h_keys] = df_term
        else:
            for idx, term in enumerate(terms):
                h_keys = (term[START_DATE], term[START_TM], term[END_DATE], term[END_TM])
                dic_dfs[h_keys] = pd.DataFrame(df.iloc[idx]).T

            graph_param = root_graph_param
            actual_record_number = dic_param.get(ACTUAL_RECORD_NUMBER)
            unique_serial = dic_param.get(UNIQUE_SERIAL)

        dic_param = filter_cat_dict_common(df, dic_param, cat_exp, [], graph_param)

        # gen scatters
        output_graphs, output_times = gen_scatter_by_direct_term(
            root_graph_param.dic_proc_cfgs,
            matrix_col,
            dic_dfs,
            x_proc_id,
            y_proc_id,
            x_label,
            y_label,
            x_user_fmt,
            y_user_fmt,
            color_label,
            color_label_fmt,
            level_labels,
            chart_type,
        )
    else:
        # query data and gen df
        if df is None:
            df, graph_param, actual_record_number, unique_serial = gen_df(
                root_graph_param,
                dic_param,
                dic_cat_filters,
                _use_expired_cache=use_expired_cache,
            )
        else:
            graph_param = root_graph_param
            actual_record_number = dic_param.get(ACTUAL_RECORD_NUMBER)
            unique_serial = dic_param.get(UNIQUE_SERIAL)

        convert_datetime_to_ct(df, graph_param)

        chart_type, x_category, y_category = get_chart_type(df, x_id, y_id, dic_cols)

        dic_param = filter_cat_dict_common(df, dic_param, cat_exp, [], graph_param)

        # check filter match or not ( for GUI show )
        (
            matched_filter_ids,
            unmatched_filter_ids,
            not_exact_match_filter_ids,
        ) = main_check_filter_detail_match_graph_data(graph_param, df)

        if orig_graph_param.common.div_by_data_number:
            output_graphs, output_times = gen_scatter_data_count(
                root_graph_param.dic_proc_cfgs,
                matrix_col,
                df,
                x_proc_id,
                y_proc_id,
                x_label,
                y_label,
                x_user_fmt,
                y_user_fmt,
                orig_graph_param.common.div_by_data_number,
                color_label,
                color_label_fmt,
                level_labels,
                recent_flg,
                chart_type,
                cat_div_fmt,
            )
        elif orig_graph_param.common.cyclic_div_num:
            output_graphs, output_times = gen_scatter_by_cyclic(
                root_graph_param.dic_proc_cfgs,
                matrix_col,
                df,
                x_proc_id,
                y_proc_id,
                x_label,
                y_label,
                x_user_fmt,
                y_user_fmt,
                terms,
                color_label,
                color_label_fmt,
                level_labels,
                chart_type,
                cat_div_fmt,
            )
        else:
            output_graphs, output_times = gen_scatter_cat_div(
                root_graph_param.dic_proc_cfgs,
                matrix_col,
                df,
                x_proc_id,
                y_proc_id,
                x_label,
                y_label,
                x_user_fmt,
                y_user_fmt,
                cat_div_label,
                color_label,
                color_label_fmt,
                level_labels,
                chart_type,
                cat_div_fmt,
            )

    # check graphs
    if not output_graphs:
        return dic_param

    # get proc configs
    dic_proc_cfgs = root_graph_param.dic_proc_cfgs

    # ↓--- Hover master info ---↓
    def _add_base_info_hover_master(_graph: dict, ignore_keys: list[str] = None):
        _keys = [X_ID, Y_ID, X_NAME, Y_NAME, X_LABEL, Y_LABEL, X_IS_MASTER, Y_IS_MASTER]
        _vals = [x_id, y_id, x_name, y_name, x_label, y_label, x_is_master, y_is_master]
        for _key, _val in zip(_keys, _vals):
            if ignore_keys and _key in ignore_keys:
                continue
            _graph[_key] = _val

    df, master_df = extract_master_id_from_df(df, graph_param)  # type: DataFrame, DataFrame
    master_df[TIME_COL] = df[TIME_COL]
    master_df[x_label] = df[x_label]
    master_df[y_label] = df[y_label]
    # ↑--- Hover master info ---↑

    color_scale = []
    # chart type
    series_keys = [ARRAY_X, ARRAY_Y, COLORS, TIMES]
    dic_param[CHART_TYPE] = chart_type
    x_user_fmt = convert_to_d3_format(x_user_fmt)
    y_user_fmt = convert_to_d3_format(y_user_fmt)
    if chart_type == ChartType.HEATMAP.value:
        other_cols = [int(col) for col in [x_id, y_id, cat_div_id] if col]

        dic_param = gen_group_filter_list(df, graph_param, dic_param, other_cols)

        # ↓--- Hover master info ---↓
        def _add_master_info_heat_map(_graph: dict):
            _add_base_info_hover_master(_graph)
            master_info_in_range_df = master_df[master_df[TIME_COL].isin(_graph[TIMES])]

            def _add_master_base_on_axis(
                is_master,
                array_axis,
                missing_axis,
                axis_label,
                axis_master_col,
                array_axis_master,
            ):
                array_master = []
                if is_master:
                    for master_val in _graph[array_axis]:
                        master_ids: list
                        if master_val in missing_axis:
                            # In case not have data, find first element of master info in df in all time.
                            master_ids = (
                                master_df[master_df[axis_label] == master_val][axis_master_col].head(1).to_list()
                            )
                        else:
                            master_ids = (
                                master_info_in_range_df[master_info_in_range_df[axis_label] == master_val][
                                    axis_master_col
                                ]
                                .drop_duplicates()
                                .to_list()
                            )
                        array_master.append(master_ids)
                _graph[array_axis_master] = array_master

            _add_master_base_on_axis(x_is_master, ARRAY_X, missing_x, x_label, x_master_col, ARRAY_X_MASTER)
            _add_master_base_on_axis(y_is_master, ARRAY_Y, missing_y, y_label, y_master_col, ARRAY_Y_MASTER)

        # ↑--- Hover master info ---↑

        # gen matrix
        all_x, all_y = get_heatmap_distinct(output_graphs)
        # all_x_with_step, all_y_with_step = get_heatmap_range_with_steps(output_graphs)
        for graph in output_graphs:
            # gen_map_xy_heatmap_matrix(x_name, y_name, all_x_with_step, all_y_with_step, graph)
            # handle x, y, z data
            array_x = graph[ARRAY_X]
            array_y = graph[ARRAY_Y]
            unique_x = set(array_x.drop_duplicates().tolist())
            unique_y = set(array_y.drop_duplicates().tolist())

            missing_x = all_x - unique_x
            missing_y = all_y - unique_y
            array_z = pd.crosstab(array_y, array_x)
            # map_xy_array_z = pd.crosstab(array_y, array_x, values=graph[COLORS], aggfunc='first')
            for key in missing_x:
                array_z[key] = None
                # map_xy_array_z[key] = None

            sorted_cols = sorted(array_z.columns)

            array_z = array_z[sorted_cols]

            missing_data = [None] * len(missing_y)
            df_missing = pd.DataFrame({col: missing_data for col in array_z.columns}, index=missing_y)
            array_z = pd.concat([array_z, df_missing])
            array_z.sort_index(inplace=True)

            # limit 10K cells
            if array_z.size > HEATMAP_COL_ROW * HEATMAP_COL_ROW:
                array_z = array_z[:HEATMAP_COL_ROW][array_z.columns[:HEATMAP_COL_ROW]]

            graph[ARRAY_X] = pd.Series(array_z.columns)
            graph[ARRAY_Y] = pd.Series(array_z.index)
            graph[ORIG_ARRAY_Z] = matrix(array_z)

            # ratio
            z_count = len(array_x)
            array_z = array_z * 100 // z_count
            graph[ARRAY_Z] = matrix(array_z)

            # ↓--- Hover master info ---↓
            _add_master_info_heat_map(graph)
            # ↑--- Hover master info ---↑

            # reduce sending data to browser
            graph[COLORS] = pd.Series()
            graph[X_SERIAL] = pd.Series()
            graph[Y_SERIAL] = pd.Series()
            graph[TIMES] = pd.Series()
            graph[ELAPSED_TIME] = pd.Series()
    elif chart_type == ChartType.HEATMAP_BY_INT.value:
        # todo
        # draw heatmap for int columns
        # gen matrix
        all_x, all_y = get_heatmap_range_with_steps(output_graphs)
        for graph in output_graphs:
            # handle x, y, z data
            array_x = graph[ARRAY_X]
            array_y = graph[ARRAY_Y]
            unique_x = set(array_x.unique())
            unique_y = set(array_y.unique())

            missing_x = all_x - unique_x
            missing_y = all_y - unique_y
            map_xy_array_z = pd.crosstab(array_y, array_x, values=graph[COLORS], aggfunc='first')
            for key in missing_x:
                map_xy_array_z[key] = None

            sorted_cols = sorted(map_xy_array_z.columns)
            map_xy_array_z = map_xy_array_z[sorted_cols]

            missing_data = [None] * len(missing_y)
            df_missing = pd.DataFrame({col: missing_data for col in map_xy_array_z.columns}, index=missing_y)
            map_xy_array_z = pd.concat([map_xy_array_z, df_missing])
            map_xy_array_z.sort_index(inplace=True)

            # limit 10K cells
            if map_xy_array_z.size > HEATMAP_COL_ROW * HEATMAP_COL_ROW:
                map_xy_array_z = map_xy_array_z[:HEATMAP_COL_ROW][map_xy_array_z.columns[:HEATMAP_COL_ROW]]

            # graph[ARRAY_Z] = matrix(map_xy_array_z).tolist()
            # graph[ORIG_ARRAY_Z] = graph[ARRAY_Z]
            graph[X_NAME] = x_name
            graph[Y_NAME] = y_name
            graph[HEATMAP_MATRIX] = {
                'z': matrix(map_xy_array_z).tolist(),
                'x': all_x,
                'y': all_y,
            }

    elif chart_type == ChartType.SCATTER.value:
        other_cols = [int(col) for col in [color_id, cat_div_id] if col]

        dic_param = gen_group_filter_list(df, graph_param, dic_param, other_cols)

        # gen scatter matrix
        n_graph = len(output_graphs) or 1
        data_per_graph = math.floor(min(SCATTER_PLOT_TOTAL_POINT / n_graph, SCATTER_PLOT_MAX_POINT))
        for graph, (x_times, y_times) in zip(output_graphs, output_times):
            # limit and sort by color
            df_graph, _is_data_limited = gen_df_limit_data(graph, series_keys, data_per_graph)
            if _is_data_limited:
                is_data_limited = True

            if len(graph[X_SERIAL]):
                # serial_col = graph[X_SERIAL][0]['col_name']
                df_graph['x_serial_dat'] = graph[X_SERIAL][0]['data'][:data_per_graph]

            if len(graph[CYCLE_IDS]):
                # to sort serial and cycle_ids before assign again
                df_graph[CYCLE_IDS] = graph[CYCLE_IDS][:data_per_graph]

            # In case of index is datetime, it can be included duplicate records and will throw error when add new
            #  column in below code. Therefor change the index to increase number
            should_reset_index = df_graph.index.name == TIMES
            if should_reset_index:
                df_graph.reset_index(inplace=True)

            # sort by color (high frequency first)
            color_col = ELAPSED_TIME
            df_graph.sort_index(inplace=True)
            df_graph[color_col] = calc_elapsed_times(df_graph, TIMES)

            # Revert index to datetime
            if should_reset_index:
                df_graph = df_graph.set_index(TIMES)

            if color_order is ColorOrder.DATA:
                color_col = COLORS if COLORS in df_graph else ARRAY_X
                df_graph = sort_df(df_graph, [color_col])
            elif color_order is ColorOrder.TIME:
                color_col = TIME_NUMBERINGS
                df_graph[color_col] = pd.to_datetime(df_graph[TIMES]).rank().convert_dtypes()

            color_scale += df_graph[color_col].tolist()

            # group by and count frequency
            df_graph['__count__'] = df_graph.groupby(color_col)[color_col].transform('count')
            df_graph.sort_values('__count__', inplace=True, ascending=False)
            df_graph.drop('__count__', axis=1, inplace=True)

            df_cols = (df_col for df_col in df_graph.columns if df_col != 'x_serial_dat')
            for key in df_cols:
                graph[key] = df_graph[key]

            if graph[X_SERIAL]:
                # todo: assign for all serial, not only first serial key
                graph[X_SERIAL][0]['data'] = df_graph['x_serial_dat']

            # chart infos
            x_end_proc_times = x_times if len(x_times) else graph[TIMES][:data_per_graph]
            x_chart_infos, _ = get_chart_info_detail(
                root_graph_param.dic_proc_cfgs,
                x_end_proc_times,
                x_id,
                threshold_filter_detail_ids,
            )
            graph[X_THRESHOLD] = x_chart_infos[-1] if x_chart_infos else None
            y_end_proc_times = y_times if len(y_times) else graph[TIMES][:data_per_graph]
            y_chart_infos, _ = get_chart_info_detail(
                root_graph_param.dic_proc_cfgs,
                y_end_proc_times,
                y_id,
                threshold_filter_detail_ids,
            )
            graph[Y_THRESHOLD] = y_chart_infos[-1] if y_chart_infos else None

            # to show plotview
            graph['end_col_id'] = x_id
            graph['end_proc_id'] = x_proc_id
            color_fmt = (
                color_label_fmt
                if color_label_fmt
                else get_fmt_from_array(df[color_label].to_list())
                if color_label
                else ''
            )
            dic_param[COLOR] = convert_to_d3_format(color_fmt) if color_fmt else ''
    else:
        group_by_cols = []
        unique_data_cols = [cat_div_id, color_id]
        if x_category:
            str_col = ARRAY_X
            number_col = ARRAY_Y
            group_by_cols.append(str_col)
            unique_data_cols.append(x_id)
            dic_param['string_axis'] = 'x'
            if color_id and color_id != x_id:
                group_by_cols.append(COLORS)
        else:
            str_col = ARRAY_Y
            number_col = ARRAY_X
            group_by_cols.append(str_col)
            unique_data_cols.append(x_id)
            dic_param['string_axis'] = 'y'
            if color_id and color_id != y_id:
                group_by_cols.append(COLORS)

        number_of_graph = min(len(output_graphs), max_graph_config[MaxGraphNumber.SCP_MAX_GRAPH.name]) or 1
        limit_violin_per_graph = math.floor(TOTAL_VIOLIN_PLOT / number_of_graph)
        most_vals, is_reduce_violin_number = get_most_common_in_graphs(
            output_graphs,
            group_by_cols,
            limit_violin_per_graph,
        )
        number_of_violin = (len(most_vals) * number_of_graph) or 1
        max_n_per_violin = math.floor(10_000 / number_of_violin)

        # for show message reduced number of violin chart
        dic_param['is_reduce_violin_number'] = is_reduce_violin_number

        other_cols = [int(col) for col in [color_id, cat_div_id] if col]

        dic_param = gen_group_filter_list(df, graph_param, dic_param, other_cols)

        dic_param[IS_RESAMPLING] = False
        # gen violin data
        for graph, (x_times, y_times) in zip(output_graphs, output_times):
            # limit and sort by color
            df_graph, _is_data_limited = gen_df_limit_data(graph, series_keys)
            if _is_data_limited:
                is_data_limited = True

            df_graph = filter_violin_df(df_graph, group_by_cols, most_vals)
            df_graph = sort_df(df_graph, group_by_cols)

            # get hover information
            dic_summaries = {}
            str_col_vals = []
            num_col_vals = []
            str_col_master_ids = []
            num_col_master_ids = []
            for _key, df_sub in df_graph.groupby(group_by_cols, sort=False):
                key = _key
                if isinstance(key, (list, tuple)):
                    key = '|'.join([str(v) for v in key])

                vals = df_sub[number_col].tolist()
                try:
                    dic_summaries[key] = calc_summary_elements({ARRAY_X: df_sub[TIMES], ARRAY_Y: vals})
                except Exception:
                    dic_summaries[key] = {'count': {}, 'basic_statistics': {}, 'non_parametric': {}}

                resample_data = resample_preserve_min_med_max(df_sub[number_col], max_n_per_violin)

                if resample_data is not None:
                    vals = resample_data.tolist()
                    is_data_limited = True

                graph[IS_RESAMPLING] = False
                if df_sub[number_col].shape[0] > max_n_per_violin:
                    graph[IS_RESAMPLING] = True
                    dic_param[IS_RESAMPLING] = True

                str_col_vals.append(key)
                num_col_vals.append(vals)

                # ↓--- Hover master info ---↓
                master_info_df = master_df[master_df[TIME_COL].isin(df_sub[TIMES] if TIMES in df_sub else df_sub.index)]
                if x_is_master:
                    str_col_master_ids.append(master_info_df[x_master_col].drop_duplicates().tolist())
                if y_is_master:
                    num_col_master_ids.append(master_info_df[y_master_col].drop_duplicates().tolist())
                # ↑--- Hover master info ---↑

            graph[str_col] = str_col_vals
            graph[number_col] = num_col_vals
            graph[SUMMARIES] = dic_summaries

            # reduce sending data to browser
            graph[COLORS] = pd.Series()
            graph[X_SERIAL] = pd.Series()
            graph[Y_SERIAL] = pd.Series()
            graph[TIMES] = pd.Series()
            graph[ELAPSED_TIME] = pd.Series()

            if number_col == ARRAY_X:
                end_proc_times = x_times if len(x_times) else graph[TIMES]
                x_chart_infos, _ = get_chart_info_detail(
                    root_graph_param.dic_proc_cfgs,
                    end_proc_times,
                    x_id,
                    threshold_filter_detail_ids,
                )
                graph[X_THRESHOLD] = x_chart_infos[-1] if x_chart_infos else None
            else:
                end_proc_times = y_times if len(y_times) else graph[TIMES]
                y_chart_infos, _ = get_chart_info_detail(
                    root_graph_param.dic_proc_cfgs,
                    end_proc_times,
                    y_id,
                    threshold_filter_detail_ids,
                )
                graph[Y_THRESHOLD] = y_chart_infos[-1] if y_chart_infos else None

            # ↓--- Hover master info ---↓
            _add_base_info_hover_master(graph)
            graph[ARRAY_X_MASTER] = str_col_master_ids
            graph[ARRAY_Y_MASTER] = num_col_master_ids
            # ↑--- Hover master info ---↑
        # TODO : we should calc box plot and kde before send to front end to improve performance

    # matched_filter_ids, unmatched_filter_ids, not_exact_match_filter_ids
    dic_param[MATCHED_FILTER_IDS] = matched_filter_ids
    dic_param[UNMATCHED_FILTER_IDS] = unmatched_filter_ids
    dic_param[NOT_EXACT_MATCH_FILTER_IDS] = not_exact_match_filter_ids

    # flag to show that trace result was limited
    dic_param[ACTUAL_RECORD_NUMBER] = actual_record_number
    dic_param[UNIQUE_SERIAL] = unique_serial

    # check show col and row labels
    is_show_h_label = False
    is_show_v_label = False
    is_show_first_h_label = False
    v_labels = list({graph[V_LABEL] for graph in output_graphs})
    h_labels = list({graph[H_LABEL] for graph in output_graphs})
    if len(h_labels) > 1 or (h_labels and h_labels[0]):
        is_show_h_label = True

        if len(output_graphs) > len(h_labels):
            is_show_first_h_label = True

    if len(v_labels) > 1 or (v_labels and v_labels[0]):
        is_show_v_label = True

    # column names
    dic_param['x_name'] = dic_cols[x_id].shown_name if x_id else None
    dic_param['y_name'] = dic_cols[y_id].shown_name if y_id else None
    dic_param['color_name'] = dic_cols[color_id].shown_name if color_id else None
    dic_param['color_type'] = dic_cols[color_id].data_type if color_id else None
    dic_param['div_name'] = dic_cols[cat_div_id].shown_name if cat_div_id else None
    dic_param['div_data_type'] = dic_cols[cat_div_id].data_type if cat_div_id else None
    dic_param['level_names'] = [dic_cols[level_id].shown_name for level_id in level_ids] if level_ids else None
    dic_param['is_show_v_label'] = is_show_v_label
    dic_param['is_show_h_label'] = is_show_h_label
    dic_param['is_show_first_h_label'] = is_show_first_h_label
    dic_param['is_filtered'] = bool(dic_cat_filters)
    dic_param['x_user_fmt'] = x_user_fmt
    dic_param['y_user_fmt'] = y_user_fmt

    dic_param['x_fmt'] = get_fmt_from_array(df[x_label].to_list())
    dic_param['y_fmt'] = get_fmt_from_array(df[y_label].to_list())
    _add_base_info_hover_master(dic_param, ignore_keys=[X_NAME, Y_NAME])

    # if not is_datetime64_ns_dtype(df[x_label]):
    #     dic_param['x_fmt'] = x_user_fmt if x_user_fmt else get_fmt_from_array(df[x_label].to_list())
    # if not is_datetime64_ns_dtype(df[y_label]):
    #     dic_param['y_fmt'] = y_user_fmt if y_user_fmt else get_fmt_from_array(df[y_label].to_list())

    # add proc name for x and y column
    dic_param['x_proc'] = dic_proc_cfgs[dic_cols[x_id].process_id].shown_name if x_id else None
    dic_param['y_proc'] = dic_proc_cfgs[dic_cols[y_id].process_id].shown_name if y_id else None

    # min, max color
    # TODO: maybe we need to get chart infor for color to get ymax ymin of all chart infos
    if color_order is ColorOrder.DATA:
        dic_scale_color = calc_scale(
            root_graph_param.dic_proc_cfgs,
            df,
            color_id,
            color_label,
            dic_cols,
            force_outlier=True,
        )
    else:
        df_color_scale = pd.DataFrame({color_label: color_scale})
        dic_scale_color = calc_scale(
            root_graph_param.dic_proc_cfgs,
            df_color_scale,
            None,
            color_label,
            dic_cols,
            force_outlier=True,
        )

    dic_param[SCALE_COLOR] = dic_scale_color

    # y scale
    if not y_category:
        y_chart_configs = [graph[Y_THRESHOLD] for graph in output_graphs if graph.get(Y_THRESHOLD)]
        dic_scale_y = calc_scale(root_graph_param.dic_proc_cfgs, df, y_id, y_label, dic_cols, y_chart_configs)
        dic_param[SCALE_Y] = dic_scale_y

    # x scale
    if not x_category:
        x_chart_configs = [graph[X_THRESHOLD] for graph in output_graphs if graph.get(X_THRESHOLD)]
        dic_scale_x = calc_scale(root_graph_param.dic_proc_cfgs, df, x_id, x_label, dic_cols, x_chart_configs)
        dic_param[SCALE_X] = dic_scale_x

    # output graphs
    dic_param[ARRAY_PLOTDATA] = [convert_series_to_list(graph) for graph in output_graphs]
    dic_param[ARRAY_PLOTDATA] = list(output_graphs)
    if chart_type != ChartType.SCATTER.value and ROWID in df.columns:
        dic_param[CYCLE_IDS] = df[ROWID]
    else:
        dic_param[CYCLE_IDS] = []

    dic_param[IS_DATA_LIMITED] = is_data_limited
    dic_param['is_x_category'] = x_category
    dic_param['is_y_category'] = y_category
    dic_param['x_data_type'] = dic_cols[x_id].data_type
    dic_param['y_data_type'] = dic_cols[y_id].data_type

    serial_data, datetime_data, start_proc_name = get_serial_and_datetime_data(df, graph_param, dic_proc_cfgs)
    dic_param[SERIALS] = serial_data
    dic_param[DATETIME] = datetime_data
    dic_param[START_PROC] = start_proc_name

    dic_param = get_filter_on_demand_data(dic_param)

    if x_is_master or y_is_master:
        add_master_information_for_hovering_scp(master_df, dic_param, graph_param)

    return dic_param


@log_execution_time()
@abort_process_handler()
def calc_scale(dic_proc_cfgs, df, col_id, col_label, dic_cols, chart_configs=None, force_outlier=False):
    if not col_id and not col_label:
        return None

    if col_id:
        cfg_col = dic_cols.get(col_id)
        if not cfg_col:
            return None

        if df is None or not len(df):
            return None

        if DataType(cfg_col.data_type) not in (DataType.REAL, DataType.INTEGER, DataType.DATETIME):
            return None

        plot = {
            END_PROC_ID: cfg_col.process_id,
            END_COL_ID: col_id,
            ARRAY_X: df[TIME_COL],
            ARRAY_Y: df[col_label],
        }
    else:
        plot = {END_PROC_ID: None, END_COL_ID: None, ARRAY_X: pd.Series([None]), ARRAY_Y: df[col_label]}

    if chart_configs:
        plot[CHART_INFOS] = chart_configs

    min_max_list, all_min, all_max = calc_raw_common_scale_y([plot])
    calc_scale_info(dic_proc_cfgs, [plot], min_max_list, all_min, all_max, force_outlier=force_outlier)

    dic_scale = {
        scale_name: plot.get(scale_name)
        for scale_name in (SCALE_SETTING, SCALE_COMMON, SCALE_THRESHOLD, SCALE_AUTO, SCALE_FULL)
    }
    return dic_scale


@log_execution_time()
@abort_process_handler()
def convert_series_to_list(graph):
    for key, series in graph.items():
        if isinstance(series, (np.ndarray, RangeIndex, Index)):
            graph[key] = pd.Series(series.tolist())

    return graph


@log_execution_time()
@abort_process_handler()
def gen_df(root_graph_param: DicParam, dic_param, dic_filter, _use_expired_cache=False):
    # bind dic_param
    graph_param = bind_dic_param_to_class(
        root_graph_param.dic_proc_cfgs,
        root_graph_param.trace_graph,
        root_graph_param.dic_card_orders,
        dic_param,
    )

    # add start proc
    # graph_param.add_start_proc_to_array_formval()

    # add condition procs
    # graph_param.add_cond_procs_to_array_formval()
    # add level
    # graph_param.add_cat_exp_to_array_formval()
    # add color, cat_div
    graph_param.add_column_to_array_formval([graph_param.common.color_var, graph_param.common.div_by_cat])

    # get serials
    # dic_proc_cfgs = get_procs_in_dic_param(graph_param)
    # for proc in graph_param.array_formval:
    #     proc_cfg = dic_proc_cfgs[proc.proc_id]
    #     serial_ids = [serial.id for serial in proc_cfg.get_serials(column_name_only=False)]
    #     proc.add_cols(serial_ids)

    # add datetime col
    graph_param.add_datetime_col_to_start_proc()
    graph_param.add_agp_color_vars()

    # get data from database
    df, actual_record_number, is_res_limited = get_data_from_db(
        graph_param,
        dic_filter,
        use_expired_cache=_use_expired_cache,
    )
    return df, graph_param, actual_record_number, is_res_limited


@log_execution_time()
@abort_process_handler()
def get_chart_type(df: DataFrame, x_id, y_id, dic_cols):
    cfg_col_x = dic_cols[x_id]
    cfg_col_y = dic_cols[y_id]
    x_category = is_categorical_col(cfg_col_x)
    y_category = is_categorical_col(cfg_col_y)

    chart_type = ChartType.VIOLIN.value
    if cfg_col_x.data_type == DataType.INTEGER.name and cfg_col_y.data_type == DataType.INTEGER.name:
        # todo: confirm to apply for unique(distinct) < 128
        chart_type = ChartType.HEATMAP_BY_INT.value
    elif x_category and y_category:
        chart_type = ChartType.HEATMAP.value
    elif not x_category and not y_category:
        chart_type = ChartType.SCATTER.value

    return chart_type, x_category, y_category


@log_execution_time()
def split_data_by_number(df: DataFrame, count):
    df[DATA_COUNT_COL] = df.reset_index().index // count
    return df


@log_execution_time()
@abort_process_handler()
def sort_data_count_key(key):
    new_key = re.match(r'^\d+', str(key))
    if new_key is not None:
        return int(new_key[0])

    return key


@log_execution_time()
@abort_process_handler()
def group_by_df(
    df: DataFrame,
    cols,
    max_group=None,
    max_record_per_group=None,
    sort_key_func=None,
    reverse=True,
    get_from_last=None,
):
    dic_groups = {}
    if df is None or not len(df):
        return dic_groups

    if not cols:
        dic_groups[None] = df.head(max_record_per_group)
        return dic_groups

    df_groups = df.groupby(cols)
    max_group = max_group or len(df_groups.groups)

    def sort_func(x):
        if sort_key_func:
            return x[0]
        all_numeric = all(str(key).isnumeric() for key, df_group in df_groups)
        return int(x[0]) if all_numeric else str(x[0])

    groups = sorted(df_groups, key=sort_func, reverse=reverse)
    groups = groups[:max_group]
    if get_from_last:
        groups.reverse()

    for key, df_group in groups:
        dic_groups[key] = df_group.head(max_record_per_group)

    return dic_groups


@log_execution_time()
@abort_process_handler()
def split_df_by_time_range(dic_df_chunks, max_group=None, max_record_per_group=None):
    dic_groups = {}
    max_group = max_group or len(dic_df_chunks)

    count = 0
    for key, df_group in dic_df_chunks.items():
        # for key, df_group in df_groups.groups.items():
        if count >= max_group:
            break

        dic_groups[key] = df_group.head(max_record_per_group)
        count += 1

    return dic_groups


@log_execution_time()
def drop_missing_data(df: DataFrame, cols):
    if df is not None and len(df):
        df.dropna(subset=[col for col in cols if col], inplace=True)
        df = df.convert_dtypes()
    return df


@log_execution_time()
def get_v_keys_str(v_keys):
    if v_keys is None:
        v_keys_str = None
    elif isinstance(v_keys, (list, tuple)):
        v_keys_str = '|'.join([str(key) for key in v_keys])
    else:
        v_keys_str = v_keys
    return v_keys_str


@log_execution_time()
def calc_elapsed_times(df_data, time_col):
    elapsed_times = pd.to_datetime(df_data[time_col]).sort_values()
    elapsed_times = elapsed_times.iloc[0:] - elapsed_times.iat[0]
    elapsed_times = elapsed_times.dt.total_seconds().fillna(0)
    elapsed_times = elapsed_times.sort_index()
    elapsed_times = elapsed_times.convert_dtypes()
    return elapsed_times


@log_execution_time()
@abort_process_handler()
def gen_scatter_data_count(
    dic_proc_cfgs,
    matrix_col,
    df: DataFrame,
    x_proc_id,
    y_proc_id,
    x,
    y,
    x_user_fmt,
    y_user_fmt,
    data_count_div,
    color=None,
    color_fmt=None,
    levels=None,
    recent_flg=None,
    chart_type=None,
    cat_div_fmt=None,
):
    """
    spit by data count
    :param color_fmt:
    :param x_user_fmt:
    :param y_user_fmt:
    :param matrix_col:
    :param df:
    :param x_proc_id:
    :param y_proc_id:
    :param x:
    :param y:
    :param data_count_div:
    :param color:
    :param levels:
    :return:
    """
    if levels is None:
        levels = []

    # time column
    time_col = TIME_COL
    # time_col = [col for col in df.columns if col.startswith(TIME_COL)][0]

    # remove missing data
    df = drop_missing_data(df, [x, y, color] + levels)

    h_group_col = DATA_COUNT_COL

    v_group_cols = [col for col in levels if col and col != h_group_col]

    # graph number is depend on facet
    max_graph = matrix_col if v_group_cols else max_graph_config[MaxGraphNumber.SCP_MAX_GRAPH.name]

    # facet
    dic_groups = {}
    dic_temp_groups = group_by_df(df, v_group_cols)
    row_count = math.ceil(max_graph_config[MaxGraphNumber.SCP_MAX_GRAPH.name] / matrix_col)
    facet_keys = [key for key, _ in Counter(dic_temp_groups.keys()).most_common(row_count)]
    for key, _df_group in dic_temp_groups.items():
        if key not in facet_keys:
            continue
        df_group = _df_group
        df_group = split_data_by_number(df_group, data_count_div)
        df_group = reduce_data_by_number(df_group, max_graph, recent_flg)

        dic_groups[key] = group_by_df(
            df_group,
            h_group_col,
            max_graph,
            sort_key_func=sort_data_count_key,
            reverse=recent_flg,
            get_from_last=recent_flg,
        )

        facet_keys.append(key)

    # serials
    x_serial_cols = dic_proc_cfgs[x_proc_id].get_serials()
    y_serial_cols = None if y_proc_id == x_proc_id else dic_proc_cfgs[y_proc_id].get_serials()

    output_graphs = []
    output_times = []
    for v_keys, dic_group in dic_groups.items():
        if v_keys not in facet_keys:
            continue

        for idx, (_h_key, df_data) in enumerate(dic_group.items()):
            h_key = idx if recent_flg else _h_key
            h_keys_str = f'{data_count_div * h_key + 1} – {data_count_div * (h_key + 1)}'

            v_keys_str = get_v_keys_str(v_keys)
            # elapsed_times = calc_elapsed_times(df_data, time_col)

            # v_label : name ( not id )
            dic_data = gen_dic_graphs(
                df_data,
                x,
                y,
                h_keys_str,
                v_keys_str,
                x_user_fmt,
                y_user_fmt,
                color,
                time_col,
                sort_key=h_key,
                color_fmt=color_fmt,
                h_keys_fmt=cat_div_fmt,
            )

            # serial
            dic_data[X_SERIAL] = get_proc_serials(df_data, x_serial_cols)
            dic_data[Y_SERIAL] = get_proc_serials(df_data, y_serial_cols)
            if ROWID in df_data.columns and chart_type == ChartType.SCATTER.value:
                dic_data[CYCLE_IDS] = df_data[ROWID]
            else:
                dic_data[CYCLE_IDS] = pd.Series()

            output_times.append((get_proc_times(df_data, x_proc_id), get_proc_times(df_data, y_proc_id)))
            output_graphs.append(dic_data)

    return output_graphs, output_times


@log_execution_time()
@abort_process_handler()
def get_proc_times(df, proc_id):
    col_name = f'{TIME_COL}_{proc_id}'
    if col_name in df.columns:
        return df[col_name]

    return pd.Series()


@log_execution_time()
@abort_process_handler()
def gen_scatter_by_cyclic(
    dic_proc_cfgs,
    matrix_col,
    df: DataFrame,
    x_proc_id,
    y_proc_id,
    x,
    y,
    x_user_fmt,
    y_user_fmt,
    terms,
    color=None,
    color_fmt=None,
    levels=None,
    chart_type=None,
    cat_div_fmt=None,
):
    """
    split by terms
    :param color_fmt:
    :param x_user_fmt:
    :param y_user_fmt:
    :param matrix_col:
    :param df:
    :param x_proc_id:
    :param y_proc_id:
    :param x:
    :param y:
    :param terms:
    :param color:
    :param levels:
    :return:
    """
    if levels is None:
        levels = []

    # time column
    time_col = TIME_COL
    # time_col = [col for col in df.columns if col.startswith(TIME_COL)][0]

    # remove missing data
    df = drop_missing_data(df, [x, y, color] + levels)

    # ensure that time column should be time-stamp before filter data
    if 'datetime' not in str(df[time_col].dtype):
        df[time_col] = df[time_col].astype('datetime64')

    dic_df_chunks = {}
    df.set_index(time_col, inplace=True, drop=False)
    if not df.empty:
        for term_id, term in enumerate(terms):
            start_dt = pd.to_datetime(term['start_dt'])
            end_dt = pd.to_datetime(term['end_dt'])
            df_chunk = df[(df.index >= start_dt) & (df.index < end_dt)]
            dic_df_chunks[(start_dt, end_dt)] = df_chunk

    v_group_cols = [col for col in levels if col]

    # graph number is depend on facet
    max_graph = matrix_col if v_group_cols else max_graph_config[MaxGraphNumber.SCP_MAX_GRAPH.name]
    dic_groups = split_df_by_time_range(dic_df_chunks, max_graph)

    # facet
    dic_groups = {key: group_by_df(df_group, v_group_cols) for key, df_group in dic_groups.items()}
    row_count = math.ceil(max_graph_config[MaxGraphNumber.SCP_MAX_GRAPH.name] / matrix_col)
    facet_keys = [
        key for key, _ in Counter([val for vals in dic_groups.values() for val in vals]).most_common(row_count)
    ]

    # serials
    x_serial_cols = dic_proc_cfgs[x_proc_id].get_serials()
    y_serial_cols = None if y_proc_id == x_proc_id else dic_proc_cfgs[y_proc_id].get_serials()

    output_graphs = []
    output_times = []
    for h_key, dic_group in dic_groups.items():
        h_keys_str = f'{h_key[0]} – {h_key[1]}'

        for v_keys, df_data in dic_group.items():
            if v_keys not in facet_keys:
                continue

            v_keys_str = get_v_keys_str(v_keys)
            # elapsed_times = calc_elapsed_times(df_data, time_col)

            # v_label : name ( not id )
            dic_data = gen_dic_graphs(
                df_data,
                x,
                y,
                h_keys_str,
                v_keys_str,
                x_user_fmt,
                y_user_fmt,
                color,
                time_col,
                color_fmt=color_fmt,
                h_keys_fmt=cat_div_fmt,
            )

            # serial
            dic_data[X_SERIAL] = get_proc_serials(df_data, x_serial_cols)
            dic_data[Y_SERIAL] = get_proc_serials(df_data, y_serial_cols)
            if ROWID in df_data.columns and chart_type == ChartType.SCATTER.value:
                dic_data[CYCLE_IDS] = df_data[ROWID]
            else:
                dic_data[CYCLE_IDS] = pd.Series()

            output_times.append((get_proc_times(df_data, x_proc_id), get_proc_times(df_data, y_proc_id)))
            output_graphs.append(dic_data)

    return output_graphs, output_times


@log_execution_time()
@abort_process_handler()
def gen_scatter_cat_div(
    dic_proc_cfgs,
    matrix_col,
    df: DataFrame,
    x_proc_id,
    y_proc_id,
    x,
    y,
    x_user_fmt,
    y_user_fmt,
    cat_div=None,
    color=None,
    color_label_fmt=None,
    levels=None,
    chart_type=None,
    cat_div_fmt=None,
):
    """
    category divide
    :param cat_div_fmt:
    :param color_label_fmt:
    :param y_user_fmt:
    :param x_user_fmt:
    :param chart_type:
    :param matrix_col:
    :param df:
    :param x_proc_id:
    :param y_proc_id:
    :param x:
    :param y:
    :param cat_div:
    :param color:
    :param levels:
    :return:
    """
    if levels is None:
        levels = []

    # time column
    time_col = TIME_COL
    # time_col = [col for col in df.columns if col.startswith(TIME_COL)][0]

    # remove missing data
    df = drop_missing_data(df, [x, y, cat_div, color] + levels)

    h_group_col = cat_div
    if not h_group_col and len(levels) > 1:
        h_group_col = levels[-1]

    v_group_cols = [col for col in levels if col and col != h_group_col]

    # graph number is depend on facet
    max_graph = matrix_col if v_group_cols else max_graph_config[MaxGraphNumber.SCP_MAX_GRAPH.name]
    dic_groups = group_by_df(df, h_group_col, max_graph)

    # facet
    dic_groups = {key: group_by_df(df_group, v_group_cols) for key, df_group in dic_groups.items()}
    row_count = math.ceil(max_graph_config[MaxGraphNumber.SCP_MAX_GRAPH.name] / matrix_col)
    facet_keys = [
        key for key, _ in Counter([val for vals in dic_groups.values() for val in vals]).most_common(row_count)
    ]

    # serials
    x_serial_cols = dic_proc_cfgs[x_proc_id].get_serials()
    y_serial_cols = None if y_proc_id == x_proc_id else dic_proc_cfgs[y_proc_id].get_serials()

    output_graphs = []
    output_times = []
    for h_key, dic_group in dic_groups.items():
        h_keys_str = str(h_key) if h_key is not None else ''

        for v_keys, df_data in dic_group.items():
            if v_keys not in facet_keys:
                continue

            v_keys_str = get_v_keys_str(v_keys)
            # elapsed_times = calc_elapsed_times(df_data, time_col)

            # v_label : name ( not id )
            dic_data = gen_dic_graphs(
                df_data,
                x,
                y,
                h_keys_str,
                v_keys_str,
                x_user_fmt,
                y_user_fmt,
                color,
                time_col,
                color_fmt=color_label_fmt,
                h_keys_fmt=cat_div_fmt,
            )

            # serial
            dic_data[X_SERIAL] = get_proc_serials(df_data, x_serial_cols)
            dic_data[Y_SERIAL] = get_proc_serials(df_data, y_serial_cols)
            if ROWID in df_data.columns and chart_type == ChartType.SCATTER.value:
                dic_data[CYCLE_IDS] = df_data[ROWID]
            else:
                dic_data[CYCLE_IDS] = pd.Series()

            output_times.append((get_proc_times(df_data, x_proc_id), get_proc_times(df_data, y_proc_id)))
            output_graphs.append(dic_data)

    return output_graphs, output_times


@log_execution_time()
@abort_process_handler()
def gen_scatter_by_direct_term(
    dic_proc_cfgs,
    matrix_col,
    dic_df_chunks,
    x_proc_id,
    y_proc_id,
    x,
    y,
    x_user_fmt,
    y_user_fmt,
    color=None,
    color_fmt=None,
    levels=None,
    chart_type=None,
):
    """
    split by terms
    :param color_fmt:
    :param y_user_fmt:
    :param x_user_fmt:
    :param matrix_col:
    :param dic_df_chunks
    :param x_proc_id:
    :param y_proc_id:
    :param x:
    :param y:
    :param color:
    :param levels:
    :param chart_type:
    :return:
    """
    if levels is None:
        levels = []

    # time column
    time_col = TIME_COL

    # remove missing data
    for key, df in dic_df_chunks.items():
        dic_df_chunks[key] = drop_missing_data(df, [x, y, color] + levels)

    v_group_cols = [col for col in levels if col]

    # graph number is depend on facet
    max_graph = matrix_col if v_group_cols else max_graph_config[MaxGraphNumber.SCP_MAX_GRAPH.name]
    dic_groups = split_df_by_time_range(dic_df_chunks, max_graph)

    # facet
    dic_groups = {key: group_by_df(df_group, v_group_cols) for key, df_group in dic_groups.items()}
    row_count = math.ceil(max_graph_config[MaxGraphNumber.SCP_MAX_GRAPH.name] / matrix_col)
    facet_keys = [
        key for key, _ in Counter([val for vals in dic_groups.values() for val in vals]).most_common(row_count)
    ]
    # serials
    x_serial_cols = dic_proc_cfgs[x_proc_id].get_serials()
    y_serial_cols = None if y_proc_id == x_proc_id else dic_proc_cfgs[y_proc_id].get_serials()

    output_graphs = []
    output_times = []
    for h_key, dic_group in dic_groups.items():
        h_keys_str = f'{h_key[0]} {h_key[1]} – {h_key[2]} {h_key[3]}'

        for v_keys, df_data in dic_group.items():
            if v_keys not in facet_keys:
                continue

            v_keys_str = get_v_keys_str(v_keys)
            # elapsed_times = calc_elapsed_times(df_data, time_col)

            # v_label : name ( not id )
            dic_data = gen_dic_graphs(
                df_data,
                x,
                y,
                h_keys_str,
                v_keys_str,
                x_user_fmt,
                y_user_fmt,
                color,
                time_col,
                color_fmt=color_fmt,
            )
            # serial
            dic_data[X_SERIAL] = get_proc_serials(df_data, x_serial_cols)
            dic_data[Y_SERIAL] = get_proc_serials(df_data, y_serial_cols)
            if ROWID in df_data.columns and chart_type == ChartType.SCATTER.value:
                dic_data[CYCLE_IDS] = df_data[ROWID]
            else:
                dic_data[CYCLE_IDS] = pd.Series()

            output_times.append((get_proc_times(df_data, x_proc_id), get_proc_times(df_data, y_proc_id)))
            output_graphs.append(dic_data)

    return output_graphs, output_times


@log_execution_time()
@abort_process_handler()
def gen_dic_graphs(
    df_data,
    x,
    y,
    h_keys_str,
    v_keys_str,
    x_user_fmt,
    y_user_fmt,
    color,
    time_col,
    sort_key=None,
    color_fmt=None,
    h_keys_fmt=None,
):
    times = df_data[time_col]
    n = times.dropna().size
    time_min = np.nanmin(times) if n else None
    time_max = np.nanmax(times) if n else None
    x_format_padding = get_format_padding(x_user_fmt)
    if x_format_padding:
        add_zero_padding_df(df_data, x, x_format_padding)

    y_format_padding = get_format_padding(y_user_fmt)
    if y_format_padding:
        add_zero_padding_df(df_data, y, y_format_padding)

    dic_data = {
        H_LABEL: h_keys_str,
        V_LABEL: v_keys_str,
        'h_label_fmt': h_keys_fmt,
        ARRAY_X: df_data[x],
        ARRAY_Y: df_data[y],
        X_USER_FORMAT: convert_to_d3_format(x_user_fmt),
        Y_USER_FORMAT: convert_to_d3_format(y_user_fmt),
        COLORS: df_data[color] if color else [],
        COLOR: convert_to_d3_format(color_fmt),
        TIMES: times,
        TIME_MIN: time_min,
        TIME_MAX: time_max,
        N_TOTAL: n,
        SORT_KEY: h_keys_str if sort_key is None else str(sort_key),
    }
    return dic_data


@log_execution_time()
@abort_process_handler()
def gen_df_limit_data(graph, keys, limit=None):
    is_limit = False
    dic_data = {}
    for key in keys:
        count = len(graph[key])
        if not count:
            continue

        if limit is None or count <= limit:
            dic_data[key] = graph[key]
        else:
            is_limit = True
            dic_data[key] = graph[key][:limit]

    return pd.DataFrame(dic_data), is_limit


@log_execution_time()
@abort_process_handler()
def sort_df(df, columns):
    cols = [col for col in columns if col in df.columns]
    df.sort_values(by=cols, inplace=True)

    return df


@log_execution_time()
@abort_process_handler()
def get_most_common_in_graphs(graphs, columns, first_most_common):
    data = []
    for graph in graphs:
        vals = pd.DataFrame({col: graph[col] for col in columns}).drop_duplicates().to_records(index=False).tolist()
        data += vals

    original_vals = [key for key, _ in Counter(data).most_common(None)]
    most_vals = [key for key, _ in Counter(data).most_common(first_most_common)]
    if len(columns) == 1:
        most_vals = [vals[0] for vals in most_vals]

    is_reduce_violin_number = len(original_vals) > len(most_vals)

    return most_vals, is_reduce_violin_number


@log_execution_time()
@abort_process_handler()
def filter_violin_df(df, cols, most_vals):
    df_result = df[df.set_index(cols).index.isin(most_vals)]
    return df_result


@log_execution_time()
@abort_process_handler()
def get_heatmap_distinct(graphs):
    array_x = pd.Series()
    array_y = pd.Series()
    for graph in graphs:
        array_x = array_x.append(graph[ARRAY_X].drop_duplicates())
        array_y = array_y.append(graph[ARRAY_Y].drop_duplicates())

    return set(array_x), set(array_y)


@log_execution_time()
def get_heatmap_range_with_steps(graphs, step=1):
    # TODO(khanhdq): v4.7.230: refactor these code below, we don't need range_x or range_y
    # We just only need range_x_min, range_y_min, range_x_max, range_y_max
    array_x, array_y, range_x, range_y = pd.Series(), pd.Series(), pd.Series(), pd.Series()
    for graph in graphs:
        range_x = range_x.append(graph[ARRAY_X].drop_duplicates())
        range_y = range_y.append(graph[ARRAY_Y].drop_duplicates())

    for i, v in enumerate(range(range_x.min(), range_y.min())):
        if not i:
            array_x = array_x.append(pd.Series([v]))
        array_x = array_x.append([array_x[-1] + step])

    for i, v in enumerate(range(range_y.min(), range_y.min())):
        if not i:
            array_y = array_y.append(pd.Series([v]))
        array_y = array_y.append([array_y[-1] + step])
    return set(array_x), set(array_y)


@log_execution_time()
@abort_process_handler()
def get_proc_serials(df: DataFrame, serial_cols: List[CfgProcessColumn]):
    if not serial_cols:
        return []

    if df is None or len(df) == 0:
        return []

    # serials
    serials = []
    for col in serial_cols:
        sql_label = gen_sql_label(col.id, col.column_name)
        if sql_label in df.columns:
            dic_serial = {'col_name': col.shown_name, 'data': df[sql_label]}
            serials.append(dic_serial)

    return serials


@log_execution_time()
@abort_process_handler()
def reduce_data_by_number(df, max_graph, recent_flg=None):
    if not len(df):
        return df

    if recent_flg:
        first_num = df[DATA_COUNT_COL].iloc[-1] - max_graph
        if first_num >= 0:
            df = df[df[DATA_COUNT_COL] > first_num]
    else:
        first_num = df[DATA_COUNT_COL].iloc[0] + max_graph
        if first_num >= 0:
            df = df[df[DATA_COUNT_COL] < first_num]

    return df


@log_execution_time()
def gen_map_xy_heatmap_matrix(x_name, y_name, all_x, all_y, graph):
    array_x = graph[ARRAY_X]
    array_y = graph[ARRAY_Y]
    unique_x = set(array_x.unique())
    unique_y = set(array_y.unique())

    missing_x = all_x - unique_x
    missing_y = all_y - unique_y
    map_xy_array_z = pd.crosstab(array_y, array_x, values=graph[COLORS], aggfunc='first')
    for key in missing_x:
        map_xy_array_z[key] = None

    sorted_cols = sorted(map_xy_array_z.columns)
    map_xy_array_z = map_xy_array_z[sorted_cols]

    missing_data = [None] * len(missing_y)
    df_missing = pd.DataFrame({col: missing_data for col in map_xy_array_z.columns}, index=missing_y)
    map_xy_array_z = pd.concat([map_xy_array_z, df_missing])
    map_xy_array_z.sort_index(inplace=True)

    # limit 10K cells
    if map_xy_array_z.size > HEATMAP_COL_ROW * HEATMAP_COL_ROW:
        map_xy_array_z = map_xy_array_z[:HEATMAP_COL_ROW][map_xy_array_z.columns[:HEATMAP_COL_ROW]]

    graph[X_NAME] = x_name
    graph[Y_NAME] = y_name
    graph[HEATMAP_MATRIX] = {
        'z': matrix(map_xy_array_z),
        'x': all_x,
        'y': all_y,
    }
