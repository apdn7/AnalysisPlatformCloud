import codecs
import pickle
from asyncio import sleep
from collections import defaultdict
from datetime import datetime, timedelta

from apscheduler.triggers.date import DateTrigger
from pytz import utc

from ap import scheduler
from ap.common.common_utils import chunks, get_current_timestamp
from ap.common.constants import COLS, REQUEST_MAX_TRIED, ROWS, JobType
from ap.common.logger import log_execution_time
from ap.common.scheduler import scheduler_app_context
from ap.setting_module.models import CfgProcess, CfgTrace, FactoryImport, make_session
from ap.setting_module.services.background_process import JobInfo, send_processing_info
from bridge.models.bridge_station import BridgeStationModel
from bridge.services.sync_transaction_data import (
    gen_response,
    get_archived_cycle,
    get_import_metadata,
)
from grpc_server.connection import check_connection_to_server
from grpc_server.services.grpc_service_proxy import grpc_api_stream
from grpc_server.services.import_transaction_data import bulk_insert_sync_data


def sync_transaction_jobs():
    for cfg_proc in CfgProcess.get_all_ids():
        start_sync_transaction_job(JobType.SYNC_TRANSACTION, cfg_proc.id)


def start_sync_transaction_job(job_type: JobType, process_id):
    job_id = f'{job_type}_{process_id}'
    job_name = f'{job_type}'
    scheduler.add_job(
        job_id,
        sync_transaction_job,
        name=job_name,
        replace_existing=True,
        trigger=DateTrigger(datetime.now().astimezone(utc), timezone=utc),
        kwargs=dict(
            _job_id=job_id,
            _job_name=job_name,
            process_id=process_id,
            number_of_try_if_failed=REQUEST_MAX_TRIED,
        ),
    )


@scheduler_app_context
def sync_transaction_job(_job_id, _job_name, process_id, number_of_try_if_failed=1):
    gen_func = request_transaction_data(process_id, number_of_try_if_failed)
    is_end = send_processing_info(gen_func, JobType.SYNC_TRANSACTION, process_id=process_id)
    if not is_end:
        # delay 5 seconds before start new job
        start_sync_transaction_job(JobType.SYNC_TRANSACTION, process_id)
    # else:
    #     # publish to clients that proc link job was done !
    #     background_announcer.announce(True, AnnounceEvent.PROC_LINK.name)

    return True


@log_execution_time()
def request_transaction_data(process_id, number_of_try_if_failed=1):
    yield 0
    job_info = JobInfo()
    yield job_info

    # sync_order_table_name = get_sync_order_table_name()
    # request grpc
    with BridgeStationModel.get_db_proxy() as db_instance:
        last_import = FactoryImport.get_last_import(process_id, JobType.TRANSACTION_IMPORT.name, only_synced=True)
        request_from = last_import.imported_cycle_id if last_import else None
        response_stream = SyncBridgeToEdge(process_id, request_from)
        _imported_records = None
        is_end = None
        job_ids = []
        for (
            t_proc_id,
            column_names,
            data_type,
            imported_records,
            last_cycle_id,
            data,
        ) in response_stream:
            if job_info.percent < 97:
                job_info.percent += 3
            data = pickle.loads(data)
            if not data:
                continue
            if not imported_records:
                _imported_records = imported_records
            if data_type == FactoryImport.get_original_table_name():
                with make_session() as meta_session:
                    row = data[0]
                    if isinstance(row, dict):
                        cols = list(row)
                        rows = [list(dic_row.values()) for dic_row in data]
                    if isinstance(row, BridgeStationModel):
                        cols = row.Columns.get_column_names()
                        rows = [row.convert_to_list_of_values() for row in data]
                    FactoryImport.insert_records(cols, rows, meta_session)
            else:
                bulk_insert_sync_data(db_instance, data_type, column_names, data)

            if data_type == FactoryImport.get_original_table_name():
                job_ids.extend([row.job_id for row in data])
                if last_cycle_id > data[-1].imported_cycle_id:
                    is_end = False  # continue request if last_cycle_id is not FactoryImport.imported_cycle_id

            yield job_info

        # sync expired cycles
        chunk_cycles = []
        if job_ids:
            chunk_cycles = get_archived_cycle(job_ids)

        cols = []
        rows = []
        for dic_archived_cycles in chunk_cycles:
            cols = dic_archived_cycles.get(COLS, [])
            rows.extend(dic_archived_cycles.get(ROWS, []))

        # delete archived cycle ids
        if cols and rows:
            delete_archived_cycles(db_instance, cols, rows)

    yield job_info
    yield 100
    return is_end


def delete_archived_cycles(db_instance, columns, rows):
    """
    delete archived transaction cycle_ids
    :param db_instance:
    :param columns:
    :param rows:
    :return:
    """
    if not columns or not rows:
        return

    dic_cycles = defaultdict(list)
    dic_updated_at = defaultdict(list)
    for proc_id, cycle_ids, updated_at in rows:
        cycle_ids = pickle.loads(codecs.decode(cycle_ids.encode(), 'base64'))
        dic_cycles[proc_id].extend(cycle_ids)
        dic_updated_at[proc_id].append(updated_at)

    for proc_id, cycle_ids in dic_cycles.items():
        data_types = []
        for cls in data_types:
            for _cycle_ids in chunks(cycle_ids, 32_000):
                # TODO: add column to archived table to delete faster
                sql, params = cls.get_delete_cycles_sql(proc_id, _cycle_ids)  # TODO:performance add start_tm and end_tm
                db_instance.execute_sql(sql, params=params)

    return True


def do_stream_grpc_request(grpc_method, request, number_of_try_if_failed=3):
    for cnt in range(number_of_try_if_failed):
        try:
            request_time = get_current_timestamp()
            responses_stream = grpc_method(request)
            for response in responses_stream:
                response_time = get_current_timestamp()
                if response.is_end_stream:
                    yield None, request_time, response_time
                    return True
                yield response, request_time, response_time
            return True
        except Exception:
            # stop job if connection is off
            if not check_connection_to_server():
                break

            # sleep 10 seconds
            sleep(10)
    return False


def sync_proc_link_jobs():
    # TODO: rewrite sync proc link
    for cfg_trace in CfgTrace.get_all():
        start_sync_proc_link_job(JobType.SYNC_PROC_LINK, cfg_trace.self_process_id, cfg_trace.target_process_id)


def start_sync_proc_link_job(job_type: JobType, self_process_id, target_process_id, is_reset=None, delay=0):
    job_id = f'{job_type}_{self_process_id}_{target_process_id}'
    job_name = f'{job_type}'
    scheduler.add_job(
        job_id,
        sync_proc_link_job,
        name=job_name,
        replace_existing=True,
        trigger=DateTrigger(datetime.now().astimezone(utc) + timedelta(0, delay), timezone=utc),
        max_instances=1,
        kwargs=dict(
            _job_id=job_id,
            _job_name=job_name,
            self_process_id=self_process_id,
            target_process_id=target_process_id,
            is_reset=is_reset,
            number_of_try_if_failed=REQUEST_MAX_TRIED,
        ),
    )


@scheduler_app_context
def sync_proc_link_job(
    _job_id, _job_name, self_process_id, target_process_id, is_reset=None, number_of_try_if_failed=1
):
    gen = request_for_proclink(self_process_id, target_process_id, is_reset, number_of_try_if_failed)
    send_processing_info(gen, JobType.SYNC_PROC_LINK, process_id=[self_process_id, target_process_id])

    # publish to clients that proc link job was done !
    # background_announcer.announce(True, AnnounceEvent.PROC_LINK.name)

    return True


def request_for_proclink(self_process_id, target_process_id, is_reset=None, number_of_try_if_failed=1):
    yield 0
    yield 100
    # # delete before request
    # if is_reset and self_process_id and target_process_id:
    #     # TODO: remove by raw sql
    #     with make_session() as session:
    #         ProcLink.delete_by_process_id(self_process_id, target_process_id, session)
    #         ProcLinkCount.delete_by_process_id(self_process_id, target_process_id, session)
    #
    # yield 50
    #
    # updated_at = CfgConstant.get_value_by_type_name(CfgConstantType.SYNC_PROC_LINK.name, ProcLink.get_table_name())
    # proc_link_count_job_id = ProcLinkCount.get_job_ids_by_proc(self_process_id, target_process_id)
    #
    # # last import date
    # request = RequestProcLinkBridgeToEdge(self_process_id=self_process_id, target_process_id=target_process_id,
    #                                       job_id=proc_link_count_job_id, updated_at=updated_at)
    #
    # # request grpc
    # # TODO: use generic function
    # with get_grpc_channel() as channel:
    #     response_stream = do_stream_grpc_request(SyncTransactionStub(channel).SyncProcLinkBridgeToEdge, request,
    #                                              number_of_try_if_failed)
    #     with make_session() as session:
    #         for response, request_time, response_time in response_stream:
    #             if not response:
    #                 continue  # end of stream
    #
    #             cols = list(response.column_names)
    #             rows = pickle.loads(response.data)
    #
    #             # TODO: use raw sql faster
    #             if response.is_proc_link_count:
    #                 # insert proc link count
    #                 ProcLinkCount.insert_records(cols, rows, session)
    #             else:
    #                 # insert proc link
    #                 ProcLink.insert_records(cols, rows, session)
    #                 # TODO: check below code error
    #                 # updated_at_idx = cols.index(proc_link_cls.updated_at.key)
    #                 updated_at_idx = cols.index('updated_at')
    #                 max_updated_at = max(rows, key=lambda _cols: _cols[updated_at_idx])[updated_at_idx]
    #
    #                 # save the latest time to constant
    #                 CfgConstant.create_or_update_by_type(session, const_type=CfgConstantType.SYNC_PROC_LINK.name,
    #                                                      const_name=ProcLink.get_table_name(),
    #                                                      const_value=max_updated_at)

    return True


@grpc_api_stream()
def SyncBridgeToEdge(process_id, request_from):
    # NOTE: Returning order is DaiJi. Please make sure you adapt consumer method when you change order
    # Order: ColumnGroup, DataInt, DataFloat, DataDatetime, DataText, JobManagement, CsvImport, FactoryImport
    # This part [Process, ColumnGroup, DataInt, DataFloat, DataDatetime, DataText] is one group to commit
    # This part [JobManagement, CsvImport, FactoryImport] is one group to commit

    cfg_proc_id = process_id
    with BridgeStationModel.get_db_proxy() as db_instance:
        meta_data = get_import_metadata(db_instance, cfg_proc_id, request_from)
        if not meta_data:
            return None

        # TODO : add t_proc_link tables
        (
            factory_imports,
            cnt_imported_records,
            last_cycle_id,
            from_cycle,
            to_cycle,
        ) = meta_data  # unpack tuple
        # get transaction data
        trans_tuples = None  # get_transaction_data_by_range(db_instance, cfg_proc_id, from_cycle, to_cycle)
        if trans_tuples:
            # send column group first
            col_groups = []  # ColumnGroup.get_column_groups_by_process_id(db_instance, cfg_proc_id)
            response = gen_response(
                cfg_proc_id, None, 't_column_group', col_groups, cnt_imported_records, last_cycle_id
            )
            yield from response

            # send transaction data
            for model_cls, cols, data_list in trans_tuples:
                response = gen_response(
                    cfg_proc_id,
                    cols,
                    model_cls.get_original_table_name(),
                    data_list,
                    cnt_imported_records,
                    last_cycle_id,
                )
                yield from response

    if factory_imports:
        for factory_import in factory_imports:
            factory_import.synced = True
        response = gen_response(
            cfg_proc_id,
            None,
            FactoryImport.get_original_table_name(),
            factory_imports,
            cnt_imported_records,
            last_cycle_id,
        )

        yield from response
