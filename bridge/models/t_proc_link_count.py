from ap.common.constants import DataType
from ap.common.logger import logger
from bridge.models.bridge_station import TransactionModel
from bridge.models.model_utils import TableColumn


class ProcLinkCount(TransactionModel):
    # a = ProcLinkCount(**{'id':1})
    # a = ProcLinkCount(id=1)                   # <==  two ways of usages
    def __init__(
        self,
        job_id=None,
        process_id=None,
        target_process_id=None,
        matched_count=None,
        created_at=None,
        updated_at=None,
        **kwargs,
    ):
        # self.id = id
        self.process_id = process_id
        self.target_process_id = target_process_id
        self.matched_count = matched_count
        self.job_id = job_id
        self.created_at = created_at
        self.updated_at = updated_at
        if isinstance(id, dict):
            logger.warning('''You're trying to assign dictionary to a single field. May be you're missing **dict ? ''')
        if kwargs:
            logger.warning(f'leakage data {kwargs}')

    class Columns(TableColumn):
        # id = (1, DataType.INTEGER)
        job_id = (2, DataType.INTEGER)
        process_id = (3, DataType.INTEGER)
        target_process_id = (4, DataType.INTEGER)
        matched_count = (5, DataType.INTEGER)
        created_at = (6, DataType.DATETIME)
        updated_at = (7, DataType.DATETIME)

    _table_name = 't_proc_link_count'
    primary_keys = []

    @classmethod
    def calc_proc_link(cls, db_instance, proc_ids):
        param_symbol = cls.get_parameter_marker()
        # if ServerConfig.get_server_type() in (ServerType.BridgeStationGrpc, ServerType.BridgeStationWeb):
        sql = f'''SELECT {ProcLinkCount.Columns.process_id.name}, {ProcLinkCount.Columns.target_process_id.name},
        SUM({ProcLinkCount.Columns.matched_count.name})  as sum_matched_count
        FROM {ProcLinkCount.get_table_name()} WHERE {ProcLinkCount.Columns.process_id.name} IN {param_symbol}
        GROUP BY {ProcLinkCount.Columns.process_id.name}, {ProcLinkCount.Columns.target_process_id.name}
        ORDER BY {ProcLinkCount.Columns.target_process_id.name} NULLS LAST, {ProcLinkCount.Columns.process_id.name}'''

        cols, rows = db_instance.run_sql(sql, row_is_dict=False, params=(tuple(proc_ids),))
        # else:
        #     param_symbols = ','.join([param_symbol] * len(proc_ids))
        #     sql = f'''SELECT self_process_id, target_process_id, SUM(matched_count) as sum_matched_count
        #     FROM t_proc_link_count
        #     WHERE self_process_id IN ({param_symbols})
        #     GROUP BY self_process_id, target_process_id
        #     ORDER BY target_process_id NULLS LAST, self_process_id'''
        #
        #     cols, rows = db_instance.run_sql(sql, row_is_dict=False, params=proc_ids)

        if not rows:
            return {}
        return {(record[0], record[1]): record[2] for record in rows}

    @classmethod
    def save_proc_link_history(cls, db_instance, job_id, dic_cycle_ids, dic_edge_cnt):
        """
        Save match count on edge of two nodes and the node itself
        :param db_instance:
        :param job_id:
        :param dic_cycle_ids:
        :param dic_edge_cnt:
        :return:
        """
        # Save matched count on one edge
        for (start_proc_id, end_proc_id), matched_cnt in dic_edge_cnt.items():
            dict_proc_link_hist = {
                cls.Columns.job_id.name: job_id,
                cls.Columns.process_id.name: start_proc_id,
                cls.Columns.target_process_id.name: end_proc_id,
                cls.Columns.matched_count.name: matched_cnt,
            }
            cls.insert_record(db_instance, dict_proc_link_hist)

        # Save matched count on one node
        for proc_id, cycle_ids in dic_cycle_ids.items():
            dict_proc_link_hist = {
                cls.Columns.job_id.name: job_id,
                cls.Columns.process_id.name: proc_id,
                cls.Columns.matched_count.name: len(cycle_ids),
            }
            cls.insert_record(db_instance, dict_proc_link_hist)

    @classmethod
    def get_delete_sql(cls, self_process_id, target_process_id):
        """
        Gets delete sql for delete

        :return:
        """
        pm = cls.get_parameter_marker()
        self_process_id_col = cls.Columns.process_id.name
        target_process_id_col = cls.Columns.target_process_id.name
        sql = (
            f'DELETE FROM {cls.get_table_name()} WHERE {self_process_id_col} = {pm} AND {target_process_id_col} = {pm}'
        )
        return sql, [self_process_id, target_process_id]
