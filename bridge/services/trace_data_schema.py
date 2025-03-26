from collections import defaultdict
from typing import Dict, List

from bridge.models.bridge_station import BridgeStationModel
from bridge.models.cfg_filter_detail import CfgFilterDetail
from bridge.models.cfg_process_column import CfgProcessColumn
from bridge.models.m_data import MData

SELECT_ALL = 'All'
NO_FILTER = 'NO_FILTER'


class EndProc:
    proc_id: int
    col_ids: List[int]
    col_names: List[str]
    col_show_names: List[str]

    def __init__(self, proc_id, cols):
        self.proc_id = int(proc_id)
        self.col_ids = []
        self.col_names = []
        self.col_show_names = []

        if cols:
            self.add_cols(cols)

    def add_cols(self, col_ids, append_first=False):
        if not isinstance(col_ids, (list, tuple)):
            col_ids = [col_ids]

        ids = [int(col) for col in col_ids]

        for col_id in ids:
            if col_id in self.col_ids:
                idx = self.col_ids.index(col_id)
                id = self.col_ids.pop(idx)
                column_name = self.col_names.pop(idx)
                name = self.col_show_names.pop(idx)
            else:
                # with BridgeStationModel.get_db_proxy() as db_instance:
                #     row = CfgProcessColumn.get_by_id(db_instance, col_id)
                with BridgeStationModel.get_db_proxy() as db_instance:
                    sys_name, show_name = get_column_name(db_instance, col_id)
                # column = CfgProcessColumn(row)
                # column_name = column.column_name
                # name = column.name
                id = col_id
                column_name = sys_name
                name = show_name

            if append_first:
                self.col_ids.insert(0, id)
                self.col_names.insert(0, column_name)
                self.col_show_names.insert(0, name)
            else:
                self.col_ids.append(id)
                self.col_names.append(column_name)
                self.col_show_names.append(name)

    def get_col_ids(self):
        return self.col_ids


class CategoryProc:
    proc_id: int
    col_ids: List[int]
    col_names: List[str]
    col_show_names: List[str]

    def __init__(self, proc, cols):
        self.proc_id = int(proc)
        if isinstance(cols, (list, tuple)):
            self.col_ids = [int(col) for col in cols]
        else:
            self.col_ids = [int(cols)]

        self.col_names = []
        self.col_show_names = []

        with BridgeStationModel.get_db_proxy() as db_instance:
            for id in self.col_ids:
                sys_name, show_name = get_column_name(db_instance, id)
                self.col_names.append(sys_name)
                self.col_show_names.append(show_name)

                # row = CfgProcessColumn.get_by_id(db_instance, id)
                # column = CfgProcessColumn(row)
                # self.col_names.append(column.column_name)
                # self.col_show_names.append(column.name)


class ConditionProcDetail:
    cfg_filter_details: List[CfgFilterDetail]
    is_no_filter: bool
    is_select_all: bool
    column_id: int
    column_name: str

    def __init__(self, filter_detail_ids):
        self.is_select_all = False
        self.is_no_filter = False
        self.cfg_filter_details = []
        self.column_id = None
        self.column_name = None

        ids = filter_detail_ids
        if not isinstance(filter_detail_ids, (list, tuple)):
            ids = [filter_detail_ids]

        column = None
        for id in ids:
            if str(id).lower() == NO_FILTER.lower():
                self.is_no_filter = True
                continue

            if str(id).lower() == SELECT_ALL.lower():
                self.is_select_all = True
                continue

            with BridgeStationModel.get_db_proxy() as db_instance:
                row = CfgFilterDetail.get_by_id(db_instance, id)
                if not row:
                    continue
                filter_detail = CfgFilterDetail(row)
                self.cfg_filter_details.append(filter_detail)
                column = CfgProcessColumn.get_column_by_filter_id(db_instance, filter_detail.filter_id)
                column = CfgProcessColumn(column)
                self.column_id = column.id

                column_name, _ = get_column_name(db_instance, column.id)
                self.column_name = column_name

            # TODO: unsafe: if filter_detail_ids is wrong, filter_detail is None,
            #  and filter_detail.cfg_filter.column occur error
            # if column is None:
            #     column = filter_detail.cfg_filter.column
            #     if column:
            #         self.column_id = column.id
            #         self.column_name = column.column_name


class ConditionProc:
    proc_id: int
    dic_col_name_filters: Dict[str, List[ConditionProcDetail]]
    dic_col_id_filters: Dict[int, List[ConditionProcDetail]]

    def __init__(self, proc, condition_details: List[ConditionProcDetail]):
        self.proc_id = int(proc)
        self.dic_col_name_filters = defaultdict(list)
        self.dic_col_id_filters = defaultdict(list)
        for f_detail in condition_details:
            if not f_detail.column_id:
                continue

            self.dic_col_name_filters[f_detail.column_name].append(f_detail)
            self.dic_col_id_filters[f_detail.column_id].append(f_detail)


class CommonParam:
    start_proc: int
    start_date: str
    start_time: str
    end_date: str
    end_time: str
    cond_procs: List[ConditionProc]
    cate_procs: List[CategoryProc]

    def __init__(
        self,
        start_proc=None,
        start_date=None,
        start_time=None,
        end_date=None,
        end_time=None,
        cond_procs=None,
        cate_procs=None,
    ):
        self.start_proc = int(start_proc)
        self.start_date = start_date
        self.start_time = start_time
        self.end_date = end_date
        self.end_time = end_time
        self.cond_procs = cond_procs
        self.cate_procs = cate_procs


class DicParam:
    chart_count: int
    common: CommonParam
    array_formval: List[EndProc]

    def __init__(self, chart_count, common, array_formval, cyclic_terms=[]):
        self.array_formval = array_formval
        self.common = common
        self.chart_count = chart_count
        self.cyclic_terms = cyclic_terms

    def search_end_proc(self, proc_id):
        for idx, proc in enumerate(self.array_formval):
            if proc.proc_id == proc_id:
                return idx, proc

        return None, None

    def add_proc_to_array_formval(self, proc_id, col_ids, append_first=False):
        idx, proc = self.search_end_proc(proc_id)
        proc = self.array_formval.pop(idx) if proc else EndProc(proc_id, [])

        proc.add_cols(col_ids)

        if append_first:
            self.array_formval.insert(0, proc)
        else:
            self.array_formval.append(proc)

        return proc

    def add_start_proc_to_array_formval(self):
        proc = self.add_proc_to_array_formval(self.common.start_proc, [], True)
        return proc

    def add_cate_procs_to_array_formval(self):
        for proc in self.common.cate_procs:
            self.add_proc_to_array_formval(proc.proc_id, proc.col_ids)

    def add_cond_procs_to_array_formval(self):
        for proc in self.common.cond_procs:
            self.add_proc_to_array_formval(proc.proc_id, list(proc.dic_col_id_filters.keys()))

    def get_all_proc_ids(self):
        proc_ids = set()
        proc_ids.add(self.common.start_proc)
        proc_ids.update([proc.proc_id for proc in self.common.cond_procs])
        proc_ids.update([proc.proc_id for proc in self.common.cate_procs])
        proc_ids.update([proc.proc_id for proc in self.array_formval])

        return list(proc_ids)

    def get_cate_var_col_id(self):
        if len(self.common.cate_procs):
            return self.common.cate_procs[0].col_ids[0]
        else:
            return None

    def get_cate_var_filter_details(self, var_col_id):
        candidate_sets = [set()]
        if len(self.common.cond_procs):
            # cate var column may be the same as filter column -> 2 cond procs created -> get all of them
            for idx, cond in enumerate(self.common.cond_procs):
                # cate var belong to start proc only
                if cond.proc_id != self.common.start_proc:
                    continue
                dic_col_id_filters = cond.dic_col_id_filters or {}
                for col_id, col_detail in dic_col_id_filters.items():
                    # get filter details of var column only
                    if col_id != var_col_id:
                        continue
                    set_filter_details = set(col_detail[0].cfg_filter_details or [])  # check idx
                    candidate_sets.append(set_filter_details)

        candidate_sets = [s for s in candidate_sets if len(s)]
        if candidate_sets:
            return list(set.intersection(*candidate_sets))
        return []

    def get_end_cols(self, proc_id):
        _, end_proc = self.search_end_proc(proc_id)
        if isinstance(end_proc, EndProc):
            return end_proc.get_col_ids() or []
        return []

    def get_start_proc(self):
        return self.common.start_proc


def get_column_name(db_instance, column_id):
    m_data = MData.get_by_id(db_instance, column_id, is_cascade=True)
    if not m_data:
        return None, None
    sys_name = m_data.m_data_group.get_sys_name()
    show_name = m_data.m_data_group.get_name()  # todo chose by GUI lang
    return sys_name, show_name
