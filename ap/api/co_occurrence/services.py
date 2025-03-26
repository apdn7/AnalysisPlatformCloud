import itertools
from io import BytesIO

import numpy as np
import pandas as pd
from flask_babel import gettext as _
from pandas import DataFrame

from ap.api.setting_module.services.data_import import NA_VALUES
from ap.common.common_utils import detect_encoding, detect_file_encoding
from ap.common.constants import CUM_RATIO_VALUE, AggregateBy, DataType
from ap.common.logger import log_execution_time, logger
from ap.common.services.normalization import normalize_str
from ap.common.services.request_time_out_handler import abort_process_handler
from ap.common.services.sse import MessageAnnouncer
from ap.common.trace_data_log import EventAction, EventType, Target, TraceErrKey, trace_log
from ap.common.yaml_utils import YamlConfig

dic_colors = {
    'bar_highlight': '#729e44',  # green, same as other charts
    'bar_normal': '#8d8b8b',
    'line_80': '#a9a9a9',
    'chart_title': '#3385b7',  # blue, same as other charts
}


def validate_csv_data(df: DataFrame):
    if df is None or df.size == 0:
        return Exception('There is no data')

    cols = df.columns.tolist()
    date_time_col = cols[0]
    data_cols = cols[1:]

    # fill na
    df.dropna(how='all', inplace=True)
    df.fillna(0, inplace=True)

    # convert time
    try:
        df[date_time_col] = pd.to_datetime(df[date_time_col])
    except Exception as e:
        logger.exception(e)
        return Exception('There are some none datetime values in Datetime column (First column)')

    # check numeric
    try:
        for col in data_cols:
            df[col] = pd.to_numeric(df[col])
    except Exception as e:
        logger.exception(e)
        return Exception('There are some none integer values in data columns')

    # check is int data type
    for col in data_cols:
        try:
            df[col] = pd.to_numeric(df[col])
        except Exception as e:
            logger.exception(e)
            return Exception('There are some none integer values in data columns')

        # convert float to int
        df[col] = df[col].convert_dtypes()

        # check < zero
        if (df[col] < 0).any():
            return Exception('There are some values < 0 in data columns')

    # check not integer ( float )
    if df.select_dtypes(include=['integer']).columns.size < len(data_cols):
        return Exception('There are some float values in data columns')

    return True


@MessageAnnouncer.notify_progress(60)
@abort_process_handler()
def calc_csv_graph_data(df: DataFrame, aggregate_by: AggregateBy, pareto=None):
    if pareto is None:
        pareto = {}
    cols = df.columns.tolist()
    date_time_col = cols[0]
    data_cols = cols[1:]

    df.set_index(date_time_col, inplace=True)

    freq = 'H' if aggregate_by is AggregateBy.HOUR else 'D'

    df_sum = df.groupby(pd.Grouper(freq=freq)).sum()
    nodes_cum_rate_80 = YamlConfig.get_node(pareto, ['bar', 'highlight_bars'], set()) or set()
    nodes = []
    for key, val in df_sum.sum().to_dict().items():
        color = dic_colors['bar_highlight'] if key in nodes_cum_rate_80 else ''
        nodes.append({'id': key, 'label': key, 'size': int(val), 'color': color})

    edges = []
    for source, target in itertools.combinations(data_cols, 2):
        edge = [source, target]
        size = int(df_sum[edge].min(axis=1).sum())
        edge_id = '-'.join([str(node_id) for node_id in edge])
        dic_edge = {'id': edge_id, 'label': size, 'source': source, 'target': target}
        edges.append(dic_edge)

    return nodes, edges


def add_node_coordinate(nodes, layout='CIRCLE'):
    if layout == 'CIRCLE':
        nodes = gen_circular_coordinate(nodes)
    elif layout == 'FORCE_ATLAS_2':
        nodes = gen_random_coordinate(nodes)
    return nodes


def gen_circular_coordinate(nodes):
    if not nodes:
        return []
    radius = 200  # TODO
    num_nodes = len(nodes)  # number of sensors (what user selected)
    # note that 0 and 2*pi indicate same position
    theta = np.linspace(0.5 * np.pi, 2.5 * np.pi, num_nodes + 1)
    theta = theta[:num_nodes]

    pos_x = np.cos(theta) * radius
    pos_y = np.sin(theta) * radius
    for idx, node in enumerate(nodes):
        node['x'] = -pos_x[idx]
        node['y'] = -pos_y[idx]
    return nodes


def gen_random_coordinate(nodes):
    if not nodes:
        return []
    import random

    for idx, node in enumerate(nodes):
        node['x'] = random.randint(1, 5)
        node['y'] = random.randint(1, 5)
    return nodes


def filter_edge_by_threshold(edges, threshold=100):
    if not edges:
        return []
    edges = sorted(edges, key=lambda x: x.get('label') or -1, reverse=True)
    limit = round(int(threshold) * len(edges) / 100)
    edges = edges[0:limit]
    return edges


@MessageAnnouncer.notify_progress(30)
@abort_process_handler()
@trace_log(
    (TraceErrKey.TYPE, TraceErrKey.ACTION, TraceErrKey.TARGET),
    (EventType.COG, EventAction.PLOT, Target.GRAPH),
    send_ga=True,
)
def calc_pareto(df: pd.DataFrame):
    drop_col = df.columns.values[0]

    # ----- summarize -----
    # sort with descending order and take cumsum for pareto chart
    total_occurrences = df.drop(drop_col, axis='columns').sum(axis='index').sort_values(ascending=False)
    cum_occurrences_ratio = total_occurrences.cumsum() / total_occurrences.sum()
    alarm_names = total_occurrences.index.values
    print('alarm names: ', alarm_names[:5], '...')
    print('total number of alarms: ', total_occurrences.values[:5], '...')
    print('cumulative ratio of number of alarms [%]: ', cum_occurrences_ratio.values[:5], '...')

    # change color of bar (highlight cumulative ratio <= 80%)
    # note that this list is not in the original column order.
    highlight_bars = set()
    bar_colors = []
    for alarm_name, cum_ratio_value in list(zip(cum_occurrences_ratio.index, cum_occurrences_ratio)):
        if cum_ratio_value <= CUM_RATIO_VALUE:
            color = dic_colors['bar_highlight']
            highlight_bars.add(alarm_name)
        else:
            color = dic_colors['bar_normal']
        bar_colors.append(color)

    return {
        'title': _('Pareto Chart'),
        'bar': {
            'y': alarm_names,
            'x': total_occurrences,
            'name': _('Total Occurrences'),
            'orientation': 'h',
            'marker_color': bar_colors,
            'text': total_occurrences.values,
            'highlight_bars': highlight_bars,
        },
        'line_cum_ratio': {
            'x': cum_occurrences_ratio * total_occurrences.max(),
            'name': _('Cumulative Ratio [%]'),
            'text': cum_occurrences_ratio.values * 100,
            'mode': 'lines+markers',
        },
        'line_80_percent': {
            'x': np.repeat(0.8, len(alarm_names)) * total_occurrences.max(),
            'name': '80 [%]',
            'marker_color': dic_colors['line_80'],
            'mode': 'lines',
        },
    }


@log_execution_time()
def csv_to_df(
    transformed_file,
    data_src,
    head_skips,
    data_first_row,
    skip_row,
    csv_delimiter,
    default_csv_param=None,
    from_file=False,
    dic_use_cols=None,
    col_names=None,
):
    # read csv file
    read_csv_param = {}
    if default_csv_param:
        read_csv_param.update(default_csv_param)

    read_csv_param.update({'skiprows': head_skips + list(range(data_first_row, skip_row + data_first_row))})

    # get encoding
    if from_file:
        encoding = detect_file_encoding(transformed_file)
        transformed_file = BytesIO(transformed_file)
    else:
        encoding = detect_encoding(transformed_file)

    # load csv data to dataframe
    df = pd.read_csv(
        transformed_file,
        sep=csv_delimiter,
        skipinitialspace=True,
        na_values=NA_VALUES,
        error_bad_lines=False,
        encoding=encoding,
        skip_blank_lines=True,
        **read_csv_param,
    )
    df.dropna(how='all', inplace=True)

    if col_names:
        df.columns = col_names

    # convert data type
    if dic_use_cols:
        for col, d_type in dic_use_cols.items():
            if d_type and DataType[d_type] in [
                DataType.REAL_SEP,
                DataType.INTEGER_SEP,
                DataType.EU_REAL_SEP,
                DataType.EU_INTEGER_SEP,
            ]:
                convert_eu_decimal(df, col, d_type)

    col_names = {col: normalize_str(col) for col in df.columns}
    df = df.rename(columns=col_names)

    # skip tail
    if data_src.skip_tail and len(df):
        df.drop(df.tail(data_src.skip_tail).index, inplace=True)

    return df


@log_execution_time()
def convert_eu_decimal(df: DataFrame, df_col, data_type):
    if data_type == DataType.REAL_SEP.name:
        df[df_col] = df[df_col].astype(str).str.replace(r'\,+', '', regex=True).astype('float64')

    if data_type == DataType.INTEGER_SEP.name:
        df[df_col] = df[df_col].astype(str).str.replace(r'\,+', '', regex=True).astype('int32')

    if data_type == DataType.EU_REAL_SEP.name:
        df[df_col] = df[df_col].astype(str).str.replace(r'\.+', '', regex=True)
        df[df_col] = df[df_col].astype(str).str.replace(r'\,+', '.', regex=True).astype('float64')

    if data_type == DataType.EU_INTEGER_SEP.name:
        df[df_col] = df[df_col].astype(str).str.replace(r'\.+', '', regex=True)
        df[df_col] = df[df_col].astype(str).str.replace(r'\,+', '.', regex=True).astype('int32')
