from typing import Optional

from ap.common.logger import log_execution_time
from ap.common.pydn.dblib.postgresql import PostgreSQL
from bridge.models.cfg_trace import CfgTrace
from bridge.models.transaction_model import TransactionData
from bridge.services.sql.sql_generator import gen_sql_proc_link_count
from bridge.services.sql.utils import gen_sql_and_params


@log_execution_time('gen_proc_link')
def gen_proc_link_of_edge(db_instance: PostgreSQL, trace: CfgTrace, limit: Optional[int] = None):
    # create table if not exist
    for proc_id in (trace.self_process_id, trace.target_process_id):
        trans_data = TransactionData(proc_id)
        if not trans_data.is_table_exist(db_instance):
            trans_data.create_table(db_instance)
            db_instance.connection.commit()
    sql_stmt = gen_sql_proc_link_count(trace, limit)
    sql, params = gen_sql_and_params(sql_stmt)
    _, rows = db_instance.run_sql(sql, row_is_dict=False, params=params)
    count = rows[0][0]
    return count


def convert_datetime_to_integer(dt):
    # dt: maybe a single datetime.datetime or np.series of np.datetime
    # yyyymm
    if isinstance(dt, str):
        return int(dt[0:4]) * 100 + int(dt[5:7])
    return dt.year * 100 + dt.month
