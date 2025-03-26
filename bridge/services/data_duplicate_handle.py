from collections import defaultdict
from datetime import datetime

import numpy
from apscheduler.triggers.interval import IntervalTrigger
from pytz import utc

from ap import scheduler
from ap.common.common_utils import merge_list_in_list_to_one_list
from ap.common.constants import JobType
from ap.common.logger import log_execution_time, logger
from ap.common.pydn.dblib.db_common import SqlComparisonOperator, add_double_quote, gen_update_sql
from ap.common.pydn.dblib.postgresql import PostgreSQL
from bridge.models.bridge_station import BridgeStationModel
from bridge.models.t_factory_import import FactoryImport
from bridge.services.proc_link import convert_datetime_to_integer

DONE_IMPORT_FACOTORY_RECORD = 10
HANDLE_RECORDS = 2_000


def handle_duplicate_data_job():
    job_name = JobType.DUPLICATE_DATA_HANDLE.name
    job_id = job_name
    # trigger = IntervalTrigger(seconds=60 * 60, timezone=utc)
    trigger = IntervalTrigger(seconds=60, timezone=utc)

    scheduler.add_job(
        id=job_id,
        name=job_name,
        func=data_duplicate_handle_main,
        replace_existing=True,
        trigger=trigger,
        next_run_time=datetime.now().astimezone(utc),
    )

    return True


@log_execution_time('[MAIN REMOVE DUPLICATED TRANSACTION]')
def data_duplicate_handle_main():
    with BridgeStationModel.get_db_proxy() as db_instance:
        dic_procs = get_check_target(db_instance)

        for proc_id, dic_time_range_job_ids in dic_procs.items():
            dic_type_cols, first_data_type, first_col_group_id = get_table_n_col_group(db_instance, proc_id)
            if not dic_type_cols:
                continue

            cycle_groups = []
            all_factory_import_ids = []  # list t_factory_import records have duplicate data
            for (start_tm, end_tm), factory_import_ids in dic_time_range_job_ids.items():
                all_factory_import_ids.extend(factory_import_ids)
                is_last_loop = is_last(dic_time_range_job_ids, (start_tm, end_tm))

                # get duplicate group of first column group id
                sub_cycle_groups = get_duplicate_data_for_first_column(
                    db_instance,
                    proc_id,
                    start_tm,
                    end_tm,
                    first_data_type,
                    first_col_group_id,
                )
                cycle_groups.extend(sub_cycle_groups)
                if get_count_total_cycle_ids(cycle_groups) < HANDLE_RECORDS and not is_last_loop:
                    continue
                cycle_groups = get_duplicate_data_for_other_col_groups(
                    db_instance,
                    proc_id,
                    dic_type_cols,
                    cycle_groups,
                )

                if get_count_total_cycle_ids(cycle_groups) < HANDLE_RECORDS and not is_last_loop:
                    continue

                # delete cycle_groups
                remove_duplicate_cycle_ids(db_instance, proc_id, cycle_groups, dic_type_cols)
                remove_in_proc_link(db_instance, proc_id, cycle_groups)

                # update status for duplicate check
                update_factory_import_records(db_instance, all_factory_import_ids)
                all_factory_import_ids.clear()
                cycle_groups.clear()
                # for job_id in job_ids:
                #     FactoryImport.update_duplicate_check_status(db_instance, job_id)

                # data commit per range time
                db_instance.connection.commit()

    return True


@log_execution_time('[MAIN REMOVE DUPLICATED TRANSACTION]')
def handle_duplicate_data(db_instance, proc_id):
    dic_procs = get_check_target(db_instance, proc_id)

    for proc_id, dic_time_range_job_ids in dic_procs.items():
        dic_type_cols, first_data_type, first_col_group_id = get_table_n_col_group(db_instance, proc_id)
        if not dic_type_cols:
            continue

        cycle_groups = []
        all_factory_import_ids = []  # list t_factory_import records have duplicate data
        for (start_tm, end_tm), factory_import_ids in dic_time_range_job_ids.items():
            all_factory_import_ids.extend(factory_import_ids)
            is_last_loop = is_last(dic_time_range_job_ids, (start_tm, end_tm))

            # get duplicate group of first column group id
            sub_cycle_groups = get_duplicate_data_for_first_column(
                db_instance,
                proc_id,
                start_tm,
                end_tm,
                first_data_type,
                first_col_group_id,
            )
            cycle_groups.extend(sub_cycle_groups)
            count_total_cycle_ids = get_count_total_cycle_ids(cycle_groups)
            if not count_total_cycle_ids or count_total_cycle_ids < HANDLE_RECORDS and not is_last_loop:
                continue

            # get duplicate group of all other column group od
            cycle_groups = get_duplicate_data_for_other_col_groups(db_instance, proc_id, dic_type_cols, cycle_groups)
            count_total_cycle_ids = get_count_total_cycle_ids(cycle_groups)
            if not count_total_cycle_ids or count_total_cycle_ids < HANDLE_RECORDS and not is_last_loop:
                continue

            # delete cycle_groups
            dup_cycle_ids = []
            for cycle_ids, time in cycle_groups:
                logger.info(cycle_ids)
                dup_cycle_ids.append(cycle_ids[0])
            yield list(set(dup_cycle_ids))  # yield to write file

            remove_duplicate_cycle_ids(db_instance, proc_id, cycle_groups, dic_type_cols)
            remove_in_proc_link(db_instance, proc_id, cycle_groups)

            # update status for duplicate check
            update_factory_import_records(db_instance, all_factory_import_ids)
            all_factory_import_ids.clear()

            cycle_groups.clear()
            # data commit per range time
            db_instance.connection.commit()

        if all_factory_import_ids:  # If no duplicated record found, update t_factory_import
            update_factory_import_records(db_instance, all_factory_import_ids)
            all_factory_import_ids.clear()
            db_instance.connection.commit()


def update_factory_import_records(db_instance, all_factory_import_ids):
    dic_conditions = {FactoryImport.Columns.id.name: [(SqlComparisonOperator.IN, tuple(all_factory_import_ids))]}
    sql, params = gen_update_sql(FactoryImport, {FactoryImport.Columns.is_duplicate_checked.name: True}, dic_conditions)
    db_instance.execute_sql(sql, params=params)


def get_check_target(db_instance, proc_id=None):
    dic_procs = {}
    recs = FactoryImport.get_duplicate_check_targets(db_instance)
    for rec in recs:
        if proc_id and rec.process_id != proc_id:
            continue
        if rec.process_id in dic_procs:  # get dict by process or new dict
            dic_time_range_job_ids = dic_procs[rec.process_id]
        else:
            dic_time_range_job_ids = defaultdict(list)
            dic_procs[rec.process_id] = dic_time_range_job_ids

        is_found_key = check_overlay_time_range(dic_time_range_job_ids, rec)  # check overlay time range
        if is_found_key:
            dic_time_range_job_ids[is_found_key].append(rec.id)
        else:
            dic_time_range_job_ids[(rec.cycle_start_tm, rec.cycle_end_tm)].append(rec.id)  # first time add to dic

    return dic_procs


def check_overlay_time_range(dic_time_range_job_ids, rec):
    for key in dic_time_range_job_ids:
        start_tm, end_tm = key
        if rec.cycle_start_tm >= start_tm and rec.cycle_end_tm <= end_tm:
            return key
    return None


def get_table_n_col_group(db_instance, proc_id):
    """
    group by data_type (key:data_type, value:list of col_group_id)
    :param db_instance:
    :param proc_id:
    :return:
    """
    col_groups = []  # ColumnGroup.get_col_groups(db_instance, proc_id)
    dic_table_cols = defaultdict(list)
    first_data_type = None
    first_col_group_id = None
    for col in col_groups:
        data_type = col['data_type']
        col_group_id = col['col_group_id']
        dic_table_cols[data_type].append(col_group_id)
        if not first_col_group_id:
            first_data_type = data_type
            first_col_group_id = col_group_id

    return dic_table_cols, first_data_type, first_col_group_id


def get_count_total_cycle_ids(cycle_groups):
    if not cycle_groups:
        return 0
    return numpy.sum([len(cycle_ids) for cycle_ids, time in cycle_groups])


def is_last(dict, key):
    return list(dict.keys())[-1] == key


def get_duplicate_data_for_first_column(db_instance: PostgreSQL, proc_id, start_tm, end_tm, data_type, col_group_id):
    table_name = None  # get_sensor_model(data_type).get_table_name(proc_id)
    col_group_sql = gen_duplicate_check_sql_by_col_group(table_name)
    _, sub_cycle_groups = db_instance.run_sql(
        col_group_sql,
        row_is_dict=False,
        params=[proc_id, col_group_id, start_tm, end_tm],
    )
    return sub_cycle_groups


def get_duplicate_data_for_other_col_groups(db_instance: PostgreSQL, proc_id, dic_type_cols, cycle_groups):
    for data_type, col_group_ids in dic_type_cols.items():
        table_name = None  # get_sensor_model(data_type).get_table_name(proc_id)
        cycle_id_sql = gen_duplicate_check_sql_by_cycle_id(table_name)

        # for other col groups
        union_sql = ' UNION '.join([cycle_id_sql] * len(cycle_groups))
        union_params = [(proc_id, tuple(cycle_ids), cycle_time) for cycle_ids, cycle_time in cycle_groups]
        union_params = merge_list_in_list_to_one_list(union_params)
        _, sub_cycle_groups = db_instance.run_sql(union_sql, row_is_dict=False, params=union_params)
        cycle_groups = sub_cycle_groups  # this effect on next loop of data_type

    return cycle_groups


@log_execution_time()
def remove_duplicate_cycle_ids(db_instance, proc_id, cycle_groups, dic_type_cols):
    where_sql = ' WHERE "process_id" = %s AND  '
    sub_or_sql = ' ("time" = %s AND "cycle_id" IN %s) '
    sub_or_sqls = [sub_or_sql] * len(cycle_groups)
    params = [proc_id]

    for cycle_ids, time in cycle_groups:
        # delete by bundle
        params.append(time)
        params.append(tuple(cycle_ids[0:-1]))

    for data_type, col_group_ids in dic_type_cols.items():
        if not col_group_ids:
            continue
        model_cls = None  # get_sensor_model(data_type)
        table_name = model_cls.get_table_name(proc_id)
        delete_sql = f'DELETE FROM {add_double_quote(table_name)} '
        sql = ''.join([delete_sql, where_sql, f'( {" OR ".join(sub_or_sqls)} )'])
        db_instance.execute_sql(sql, params=params)

    return True


def remove_in_proc_link(db_instance, proc_id, cycle_ids_groups):
    delete_sql = 'DELETE FROM "t_proc_link" '
    params = [proc_id]

    # delete by bundle
    for cycle_ids, time in cycle_ids_groups:
        params.append(convert_datetime_to_integer(time))
        params.append(tuple(cycle_ids))

    # self
    self_where_sql = ' WHERE "self_process_id" = %s AND  '
    self_sub_or_sql = ' ("link_time" = %s AND "self_cycle_id" IN %s) '
    self_sub_or_sqls = [self_sub_or_sql] * len(cycle_ids_groups)
    self_sql = ''.join([delete_sql, self_where_sql, f'( {" OR ".join(self_sub_or_sqls)} )'])
    db_instance.execute_sql(self_sql, params=params)

    # target
    target_where_sql = ' WHERE "target_process_id" = %s AND  '
    target_sub_or_sql = ' ("link_time" = %s AND "target_cycle_id" IN %s) '
    target_sub_or_sqls = [target_sub_or_sql] * len(cycle_ids_groups)
    target_sql = ''.join([delete_sql, target_where_sql, f'( {" OR ".join(target_sub_or_sqls)} )'])
    db_instance.execute_sql(target_sql, params=params)

    return True


@log_execution_time()
def gen_duplicate_check_sql_by_col_group(table_name):
    sql_para = BridgeStationModel.get_parameter_marker()

    # if ServerConfig.get_server_type() in (ServerType.BridgeStationGrpc, ServerType.BridgeStationWeb):
    select_sql = 'array_agg(cycle_id)'
    # else:
    #     select_sql = f'group_concat(cycle_id)'

    sql = f'''
    SELECT {select_sql} as cycle_ids, time
    FROM {table_name}
    WHERE process_id = {sql_para}
      AND col_group_id = {sql_para}
      AND time between {sql_para} AND {sql_para}
    GROUP BY time, val_01, val_02, val_03, val_04, val_05, val_06, val_07, val_08, val_09, val_10
    HAVING count(cycle_id) > 1
    '''

    return sql


@log_execution_time()
def gen_duplicate_check_sql_by_cycle_id(table_name):
    sql_para = BridgeStationModel.get_parameter_marker()
    sql = f'''
    SELECT array_agg(cycle_id) as cycle_ids, time
    FROM {table_name}
    WHERE process_id = {sql_para}
      AND cycle_id IN {sql_para}
      AND time = {sql_para}
    GROUP BY time, col_group_id, val_01, val_02, val_03, val_04, val_05, val_06, val_07, val_08, val_09, val_10
    HAVING count(cycle_id) > 1
    '''

    return sql
