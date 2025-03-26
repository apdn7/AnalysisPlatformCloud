from collections import defaultdict
from typing import List

from ap.common.common_utils import gen_sql_label
from ap.common.logger import log_execution_time
from ap.setting_module.models import CfgTrace, ProcLinkCount
from ap.trace_data.models import ProcDataCount
from bridge.models.bridge_station import BridgeStationModel
from bridge.services.proc_link import gen_proc_link_of_edge


@log_execution_time('[SIMULATE GLOBAL ID]')
def sim_gen_global_id(edges: List[CfgTrace]):
    traces = CfgTrace.get_all()
    dic_proc_data_count = {data.process_id: data.count for data in ProcDataCount.get_procs_count()}
    dic_proc_link_count = {
        (data.process_id, data.target_process_id): data.matched_count for data in ProcLinkCount.calc_proc_link()
    }

    # matched count on edge
    dic_edge_cnt = defaultdict(list)

    # proc : rows in database
    dic_proc_cnt = defaultdict(list)

    # existing tracing config
    existing_trace = {(trace.self_process_id, trace.target_process_id): trace for trace in traces}

    for requested_trace in edges:
        start_proc_id = requested_trace.self_process_id
        end_proc_id = requested_trace.target_process_id
        edge_id = f'{start_proc_id}-{end_proc_id}'
        edge_cnt = 0

        # if exactly same tracing => get from t_proc_link_count
        # otherwise => do full flow get t_proc_link to count joined cycle_id, without insert to t_proc_link
        if (start_proc_id, end_proc_id) in existing_trace:
            already_save_trace = existing_trace[(start_proc_id, end_proc_id)]
            if already_save_trace.is_same_tracing(requested_trace):
                # only do if requested tracing exactly same with existing tracing config data
                edge_cnt = dic_proc_link_count.get((start_proc_id, end_proc_id), 0)
                dic_edge_cnt[edge_id] = edge_cnt

        if not edge_cnt:
            with BridgeStationModel.get_db_proxy() as db_instance:
                # TODO: add constants for this limit
                edge_cnt = gen_proc_link_of_edge(db_instance, requested_trace, limit=500_000)

        dic_edge_cnt[edge_id] = edge_cnt

        for proc_id in (start_proc_id, end_proc_id):
            # sum_value = dic_proc_cnt.get(proc_id,  0) + edge_cnt
            cycle_cnt = dic_proc_data_count.get(proc_id, 0)
            # dic_proc_cnt[proc_id] = tuple([sum_value, cycle_cnt])
            dic_proc_cnt[proc_id] = cycle_cnt

    return dic_proc_cnt, dic_edge_cnt


# @log_execution_time()
# def sim_order_before_mapping_data(edges: List[CfgTrace]):
#     """ trace all node in dic_node , and gen sql
#     """
#     ordered_edges = []
#
#     max_loop = sum(range(1, len(edges) + 1))
#     edges = deque(edges)
#     cnt = 0
#     while edges:
#         if cnt > max_loop:
#             raise Exception('Edges made a ring circle, You must re-setting tracing edge to break the ring circle!!!')
#
#         # get first element
#         edge = edges.popleft()
#
#         # check if current start proc is in others edge's end procs
#         # if YES , we must wait for these end proc run first( move the current edge to the end)
#         # traceback. So target => start , self => end
#         if any((edge.target_process_id == other_edge.self_process_id for other_edge in edges)):
#             # move to the end of queue
#             edges.append(edge)
#         else:
#             ordered_edges.append(edge)
#
#         cnt += 1
#
#     return ordered_edges


def gen_dic_proc_data(data, trace_key_infos):
    dic_filter = {}
    for row in data:
        keys = []
        for key in trace_key_infos:
            val = str(getattr(row, gen_sql_label(key.column_name)))
            if key.from_char:
                val = val[key.from_char - 1 : key.to_char]

            keys.append(val)

        dic_filter[tuple(keys)] = row

    return dic_filter
