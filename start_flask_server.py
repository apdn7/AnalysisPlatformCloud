import atexit
import contextlib
import os
from datetime import datetime

from apscheduler.schedulers.base import STATE_STOPPED
from flask import Flask
from flask_migrate import upgrade
from pytz import utc

from ap import dic_config, max_graph_config, scheduler
from ap.api.setting_module.services.polling_frequency import change_polling_all_interval_jobs
from ap.api.trace_data.services.proc_link import add_restructure_indexes_job
from ap.common.clean_expired_request import add_job_delete_expired_request
from ap.common.clean_old_data import add_job_delete_old_zipped_log_files, add_job_zip_all_previous_log_files
from ap.common.common_utils import bundle_assets, get_log_path, init_sample_config
from ap.common.constants import SHUTDOWN, JobType, ServerType
from ap.common.ga import is_app_source_dn, is_running_in_window
from ap.common.logger import logger
from ap.common.services.import_export_config_and_master_data import pull_n_import_sample_data, set_break_job_flag
from ap.common.services.notify_listen import process_listen_job
from ap.common.trace_data_log import (
    EventAction,
    EventCategory,
    EventType,
    Location,
    send_gtag,
)
from ap.setting_module.models import JobManagement
from bridge.services.master_data_import import dum_data_from_files
from bridge.services.view_gen import VIEW_TABLE_DICT, check_view_tables_exist, gen_view_tables


def stop_scheduler():
    if scheduler.state != STATE_STOPPED:
        scheduler.shutdown(wait=False)
        logger.info('End Scheduler!!!')
    else:
        logger.info('Scheduler already shutdown!!!')


def serve_flask_server(app: Flask, port: int, server_type: ServerType, env: str = None):
    from ap import init_db
    from grpc_server.redis_connection import bridge_changed_listen_job
    from grpc_server.services.sync_config_utils import initialize_edge

    print('SCHEDULER START!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!')
    # start scheduler (Notice: start scheduler at the end , because it may run job before above setting info was set)
    if not app.config.get('SCHEDULER_TIMEZONE'):
        # set timezone for scheduler before init job
        # all job will run in UTC instead of local time
        app.config['SCHEDULER_TIMEZONE'] = utc

    scheduler.init_app(app)

    # Shut down the scheduler when exiting the app
    atexit.register(stop_scheduler)
    scheduler.start(paused=True)

    with app.app_context():
        process_listen_job()
        bundle_assets(app)

        try:
            # migrate database
            upgrade()
        except Exception:
            logger.warning('Cannot upgrade DB.  ====>  Do initialize DB first')
        finally:
            init_db(app)

        with contextlib.suppress(Exception):
            is_exist = check_view_tables_exist(list(VIEW_TABLE_DICT.keys())[0])
            if not is_exist:
                from ap.setting_module.models import CfgConstant

                init_sample_config(ignore_tables=[CfgConstant.__tablename__])
                pull_n_import_sample_data()
                gen_view_tables()

        initialize_edge()
        dum_data_from_files()
        if server_type is ServerType.EdgeServer:
            bridge_changed_listen_job()

        # init cfg_constants for usage_disk
        from ap.api.setting_module.services.polling_frequency import add_idle_monitoring_job
        from ap.common.clean_old_data import run_clean_data_job
        from ap.common.common_utils import get_cache_path
        from ap.common.pydn.dblib.db_mainternance import run_db_maintenance_job
        from ap.setting_module.models import CfgConstant

        CfgConstant.initialize_disk_usage_limit()
        CfgConstant.initialize_max_graph_constants()

        for key, _ in max_graph_config.items():
            max_graph_config[key] = CfgConstant.get_value_by_type_first(key, int)

        set_break_job_flag(False)
        # CfgConstant.initialize_feature_month_ago()
        JobManagement.update_processing_to_failed()

        # run jobs
        # scheduler.resume()
        logger.info('Start job scheduler!!!')
        run_clean_data_job(
            JobType.CLEAN_CACHE.name,
            folder=get_cache_path(),
            num_day_ago=-1,
            job_repeat_sec=24 * 60 * 60,
        )
        # DO NOT clean folder data because it contains many important stuffs
        # run_clean_data_job(
        #     JobType.CLEAN_DATA.name,
        #     folder=get_data_path(),
        #     num_day_ago=30,
        #     job_repeat_sec=24 * 60 * 60,
        # )
        run_clean_data_job(
            JobType.CLEAN_LOG.name,
            folder=get_log_path(),
            num_day_ago=7,
            job_repeat_sec=24 * 60 * 60,
        )
        run_db_maintenance_job()

        add_job_zip_all_previous_log_files()
        add_job_delete_old_zipped_log_files()
        add_idle_monitoring_job()
        add_restructure_indexes_job()
        change_polling_all_interval_jobs(run_now=True)

        # delete req_id created > 24h ago
        add_job_delete_expired_request()

        # import_transaction_all_processes()

    # BRIDGE STATION - Refactor DN & OSS version
    if is_app_source_dn():
        true_values = [True, 'true', '1', 1]
        # check and update R-Portable folder
        should_update_r_lib = os.environ.get('UPDATE_R', 'false')
        if should_update_r_lib and should_update_r_lib.lower() in true_values:
            from ap.script.check_r_portable import check_and_copy_r_portable

            check_and_copy_r_portable()

    # disable quick edit of terminal to avoid pause
    is_debug = app.config.get('DEBUG')
    # BRIDGE STATION - csv cannot run inside windows for now
    if not is_debug and is_running_in_window():
        from ap.script.disable_terminal_quickedit import disable_quickedit

        disable_quickedit()

    # TODO: take time , so comment
    # add job when app started
    # add_backup_dbs_job()

    # TODO: slow , so comment
    # kick R process
    # from ap.script.call_r_process import call_r_process

    # call_r_process()

    # clear cache
    # clear_cache()

    # convert user setting
    # from ap.script.convert_user_setting import convert_user_setting_url

    # convert_user_setting()
    # convert_user_setting_url()

    # Synchronize master data from Bridge to Edge
    # # todo: remove histview_FactoryImport in request_for_imported_master, replace by bridge_station.t_factor_import
    # def imported_master_with_app_context(number_of_try_if_failed):
    #     with app.app_context():
    #         request_for_imported_master(number_of_try_if_failed)
    # add_sync_master_data_job(imported_master_with_app_context)

    # TODO: ignore to avoid error
    # print(f'Edge Server\'s disk usage: {get_disk_usage_percent()[1]}%')

    # socketio.run(app, host="0.0.0.0", port=port, debug=is_debug, use_reloader=is_debug, log_output=False)
    # app.run(host="0.0.0.0", port=port, threaded=True, debug=is_debug, use_reloader=False)

    scheduler.resume()
    try:
        app.config.update({'app_startup_time': datetime.utcnow()})
        if env == 'dev':
            print('Start Development Flask server!!!')
            app.run(host='0.0.0.0', port=port, threaded=True, debug=is_debug, use_reloader=False)
        else:
            from waitress import serve

            print('Start Production Waitress server!!!')
            with app.app_context():
                # If the result of sending first Gtag is a failure,
                # the environment is deemed as unable to connect to GA
                # GA will no longer be sent after this
                if not send_gtag(
                    ec=EventCategory.APP_START.value,
                    ea=EventType.APP.value + '_lt',
                    el=Location.PYTHON.value + EventAction.START.value,
                ):
                    app.config.update({'IS_SEND_GOOGLE_ANALYTICS': False})
            serve(app, host='0.0.0.0', port=port, threads=20)
    finally:
        stop_scheduler()
        dic_config[SHUTDOWN] = True
        print('End server!!!')
