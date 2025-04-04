import json
from copy import deepcopy
from typing import TYPE_CHECKING

import pandas as pd

from ap.api.common.services.utils import TraceGraph
from ap.common.common_utils import get_format_padding
from ap.common.constants import (
    EMPTY_STRING,
    ID,
    CacheType,
    CfgConstantType,
    DataGroupType,
    DataType,
    RawDataTypeDB,
    dict_convert_raw_data_type,
)
from ap.common.logger import log_execution_time
from ap.common.memoize import memoize
from ap.setting_module.models import CfgConstant, CfgProcess, CfgProcessColumn, CfgTrace, make_session
from ap.setting_module.schemas import ShowGraphSchema, TraceSchema

if TYPE_CHECKING:
    from ap.trace_data.schemas import DicParam


class DictToClass:
    # TODO: clear updated_at , created_at to reduce memory
    def __init__(self, **entries):
        self.__dict__.update(entries)
        for key, value in self.__dict__.items():
            if isinstance(value, (list, tuple)):
                self.__dict__[key] = [self.__class__(**val) if isinstance(val, dict) else val for val in value]
            elif isinstance(value, dict):
                self.__dict__[key] = self.__class__(**value)

    def get_keys(self):
        return [key for key, _ in self.__dict__.items()]

    def get_values(self):
        return [value for _, value in self.__dict__.items()]

    def get_items(self):
        return self.__dict__.items()

    def as_dict(self):
        return self.__dict__


class ShowGraphConfigData(DictToClass):
    def __init__(self, **entries):
        super().__init__(**entries)

    def get_by_raw_data_type(self, raw_data_type: RawDataTypeDB):
        cols = [col for col in self.columns if col.raw_data_type == raw_data_type.value]
        return cols

    def get_cols_by_data_type(self, data_type: DataType, column_name_only=True):
        """
        get date column
        :param data_type:
        :param column_name_only:
        :return:
        """
        if column_name_only:
            cols = [col.column_name for col in self.columns if col.data_type == data_type.value]
        else:
            cols = [col for col in self.columns if col.data_type == data_type.value]

        return cols

    def get_dic_cols_by_ids(self, col_ids):
        # col_ids = [int(col_id) for col_id in col_ids if col_id]
        dic_cols = {col.id: col for col in self.columns if col.id in col_ids}
        return dic_cols

    def get_serials(self, column_name_only=False):
        if column_name_only:
            cols = [col.column_name for col in self.columns if col.is_serial_no]
        else:
            cols = [col for col in self.columns if col.is_serial_no]

        return cols

    def get_lines(self, column_name_only=False):
        if column_name_only:
            cols = [col.column_name for col in self.columns if col.column_type == DataGroupType.LINE.value]
        else:
            cols = [col for col in self.columns if col.column_type == DataGroupType.LINE.value]

        return cols

    def get_equips(self, column_name_only=False):
        if column_name_only:
            cols = [col.column_name for col in self.columns if col.column_type == DataGroupType.EQUIP.value]
        else:
            cols = [col for col in self.columns if col.column_type == DataGroupType.EQUIP.value]

        return cols

    def get_parts(self, column_name_only=False):
        if column_name_only:
            cols = [col.column_name for col in self.columns if col.column_type == DataGroupType.PART.value]
        else:
            cols = [col for col in self.columns if col.column_type == DataGroupType.PART.value]

        return cols

    def get_date_col(self, column_name_only=True):
        """
        get date column
        :param column_name_only:
        :return:
        """
        cols = [col for col in self.columns if col.is_get_date]
        if cols:
            if column_name_only:
                return cols[0].column_name

            return cols[0]

        return None

    def get_order_cols(self, column_name_only=True, column_id_only=False):
        cols = [
            col
            for col in self.columns
            if col.is_serial_no
            or col.data_type
            in [
                DataType.DATETIME.value,
                DataType.TEXT.value,
                DataType.INTEGER.value,
            ]
        ]
        if column_name_only:
            cols = [col.column_name for col in cols]

        if column_id_only:
            cols = [col.id for col in cols]

        return cols

    def get_dic_cfg_col_by_ids(self, dic_proc_cfgs: dict[int, CfgProcess], col_ids):
        dic_procs = {}
        dic_cols = {}
        for proc_id, proc in dic_proc_cfgs.items():
            _dic_cols = proc.get_dic_cols_by_ids(col_ids)
            if _dic_cols:
                dic_cols.update(_dic_cols)
                dic_procs[proc_id] = proc

        return dic_procs, dic_cols

    def get_sensor_default_chart_info(self, col_id, start_tm, end_tm):
        targets = []
        for cfg_visual in self.visualizations:
            if (
                cfg_visual.control_column_id == col_id
                and cfg_visual.filter_detail_id is None
                and (
                    cfg_visual.act_from is None
                    or pd.to_datetime(cfg_visual.act_from) < pd.to_datetime(end_tm)
                    or cfg_visual.act_from == ''
                )
                and (
                    cfg_visual.act_to is None
                    or pd.to_datetime(cfg_visual.act_to) > pd.to_datetime(start_tm)
                    or cfg_visual.act_to == ''
                )
            ):
                targets.append(cfg_visual)

        targets.sort(key=lambda obj: obj.act_from, reverse=True)
        return targets

    def get_by_control_n_filter_detail_ids(self, col_id, filter_detail_ids, start_tm, end_tm):
        targets = []
        for cfg_visual in self.visualizations:
            if (
                cfg_visual.control_column_id == col_id
                and cfg_visual.filter_detail_id in filter_detail_ids
                and (
                    cfg_visual.act_from is None
                    or pd.to_datetime(cfg_visual.act_from) < end_tm
                    or cfg_visual.act_from == ''
                )
                and (
                    cfg_visual.act_to is None or pd.to_datetime(cfg_visual.act_to) > start_tm or cfg_visual.act_to == ''
                )
            ):
                targets.append(cfg_visual)

        targets.sort(key=lambda obj: obj.act_from, reverse=True)
        return targets

    def modify_col_name(self, col_id, name):
        col = self.get_col(col_id)
        if col:
            col.column_name = name
            col.shown_name = name

    def copy_new_col_cfg(self, col_id, dummy_idx=1000000):
        col = deepcopy(self.get_col(col_id))
        if col:
            col.id = col_id * dummy_idx
            self.columns.append(col)
            return col

    def get_dic_filter_details(self, filter_detail_ids=None):
        dic_filter_details = {}
        for cfg_filter in self.filters:
            for filter_detail in cfg_filter.filter_details:
                if filter_detail is None or filter_detail_ids is None or filter_detail.id in filter_detail_ids:
                    filter_detail.column = cfg_filter.column
                    dic_filter_details[filter_detail.id] = filter_detail

        return dic_filter_details

    def get_cols(self, col_ids):
        len_col = len(col_ids)
        cols = []
        for col in self.columns:
            if col.id in col_ids:
                cols.append(col)
                if len(cols) >= len_col:
                    return cols

        return cols

    def get_col(self, col_id):
        for col in self.columns:
            if col.id == col_id:
                return col

        return None

    def get_filter_cfg_by_col_ids(self, col_ids):
        return [filter_cfg for filter_cfg in self.filters if filter_cfg.column_id in col_ids]


def preprocess_column(column: CfgProcessColumn) -> CfgProcessColumn:
    # modify data type based on function columns
    if column.function_details and column.function_details[-1].return_type:
        function_return_type = column.function_details[-1].return_type
        column.data_type = dict_convert_raw_data_type.get(function_return_type, function_return_type)
        column.format = EMPTY_STRING

    # change data type based on format
    # Padding: Only allow columns with data type INTEGER to be formatted with padding,
    # otherwise, modify `format` attribute to empty
    elif get_format_padding(column.format):
        if column.data_type == DataType.INTEGER.value:
            column.data_type = DataType.TEXT.value
        else:
            column.format = EMPTY_STRING

    # need to change again, make sure date, time, boolean be converted to text
    if column.data_type in [DataType.DATE.value, DataType.TIME.value, DataType.BOOLEAN.value]:
        column.data_type = DataType.TEXT.value
        column.format = EMPTY_STRING

    return column


def preprocess_process(process: CfgProcess) -> CfgProcess:
    for column in process.columns:
        preprocess_column(column)
    return process


@log_execution_time()
@memoize(cache_type=CacheType.CONFIG_DATA)
def get_config_data():
    """
    get all process from graph_param then gen its json
    :return:
    """
    # TODO: because we use this function for show graph function, so many processes between start and end will be used.
    # TODO: so currently , get all processes may better and easy , but there is an issue of caching and performance.
    # TODO: remove below code if we found better solution

    show_graph_schema = ShowGraphSchema()
    trace_schema = TraceSchema()

    # create new session to query from database, because we change data in `process_process`
    # we don't want those changes to be commited to database
    with make_session() as meta_session:
        processes = meta_session.query(CfgProcess).all()
        for process in processes:
            preprocess_process(process)
        cfg_traces = meta_session.query(CfgTrace).all()

        procs = show_graph_schema.dump(processes, many=True)
        traces = trace_schema.dump(cfg_traces, many=True)

        # rollback to avoid changes data to be commited to database
        meta_session.rollback()

    dic_procs = {}
    proc_ids = []
    for dic_proc in procs:
        proc_id = dic_proc[ID]
        proc_ids.append(str(proc_id))
        show_graph_proc_data: CfgProcess = ShowGraphConfigData(**dic_proc)
        dic_procs[proc_id] = show_graph_proc_data

    traces = [DictToClass(**trace) for trace in traces]
    trace_graph = TraceGraph(traces)
    dic_card_orders = {}
    for cfg_const in CfgConstant.get_value_by_type_names(const_type=CfgConstantType.TS_CARD_ORDER.name, names=proc_ids):
        dic_card_orders[int(cfg_const.name)] = json.loads(cfg_const.value)

    return dic_procs, trace_graph, dic_card_orders


def get_proc_ids_in_graph_param(graph_param: 'DicParam'):
    """
    get process
    :param graph_param:
    :return:
    """
    procs = [graph_param.common.start_proc]
    for proc in graph_param.common.cond_procs:
        procs.append(proc.proc_id)

    for proc in graph_param.common.cate_procs:
        procs.append(proc.proc_id)

    for proc in graph_param.array_formval:
        procs.append(proc.proc_id)

    for proc_id in graph_param.common.serial_processes:
        procs.append(proc_id)

    cols = []
    cols += _get_cols(graph_param.common.cat_exp)
    cols += _get_cols(graph_param.common.color_var)
    cols += _get_cols(graph_param.common.objective_var)
    cols += _get_cols(graph_param.common.div_by_cat)

    if cols:
        cols = list(set(cols))
        _proc_ids = [proc.process_id for proc in CfgProcessColumn.get_by_ids(cols)]
        procs += _proc_ids

    return list(set(procs))


# def get_procs_in_dic_param(graph_param: 'DicParam'):
#     """
#     get process
#     :param graph_param:
#     :return:
#     """
#     # TODO 1
#     proc_ids = get_proc_ids_in_graph_param(graph_param)
#     dic_procs = gen_dict_procs(proc_ids)
#     return dic_procs


def _get_cols(cols):
    if not cols:
        return []

    if not isinstance(cols, (tuple, list)):
        cols = [cols]

    return cols


def gen_dict_procs(proc_ids):
    return {proc.id: proc for proc in CfgProcess.get_procs(proc_ids)}
