from __future__ import annotations

import os
import time
from datetime import datetime, timedelta
from functools import wraps
from typing import TYPE_CHECKING

import wtforms_json
from apscheduler.jobstores.base import JobLookupError
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.date import DateTrigger
from flask import Flask, Response, g
from flask_apscheduler import APScheduler
from flask_babel import Babel
from flask_compress import Compress
from flask_marshmallow import Marshmallow
from flask_migrate import Migrate
from flask_sqlalchemy import SQLAlchemy
from pytz import utc
from sqlalchemy.pool import NullPool

from ap.common.common_utils import (
    DATE_FORMAT_STR,
    NoDataFoundException,
    get_process_queue,
)
from ap.common.constants import (
    ANNOUNCE_UPDATE_TIME,
    APP_BROWSER_DEBUG_ENV,
    APP_DB_FILE,
    APP_FILE_MODE_ENV,
    APP_LANGUAGE_ENV,
    APP_SUBTITLE_ENV,
    COND_PROC,
    DB_SECRET_KEY,
    END_PROC,
    EXTERNAL_API,
    GET02_VALS_SELECT,
    HTML_CODE_304,
    IGNORE_MULTIPROCESSING_LOCK_KEY,
    LAST_REQUEST_TIME,
    LIMIT_CHECKING_NEWER_VERSION_TIME,
    LOG_LEVEL,
    PARTITION_NUMBER,
    PORT,
    PROCESS_QUEUE,
    REQUEST_THREAD_ID,
    SERVER_ADDR,
    SHUTDOWN,
    SQLITE_CONFIG_DIR,
    TESTING,
    UNIVERSAL_DB_FILE,
    YAML_CONFIG_AP,
    YAML_CONFIG_BASIC,
    YAML_CONFIG_DB,
    YAML_CONFIG_PROC,
    YAML_CONFIG_VERSION,
    YAML_START_UP,
    ApLogLevel,
    ListenNotifyType,
    MaxGraphNumber,
)
from ap.common.ga import ga_info, is_app_source_dn
from ap.common.logger import log_execution_time, logger
from ap.common.services.http_content import json_dumps
from ap.common.services.request_time_out_handler import RequestTimeOutAPI, set_request_g_dict
from ap.common.trace_data_log import TraceErrKey, get_log_attr
from ap.common.yaml_utils import (
    YAML_CONFIG_AP_FILE_NAME,
    YAML_CONFIG_BASIC_FILE_NAME,
    YAML_CONFIG_DB_FILE_NAME,
    YAML_CONFIG_PROC_FILE_NAME,
    YAML_START_UP_FILE_NAME,
    BasicConfigYaml,
)
from ap.equations.error import FunctionErrors

if TYPE_CHECKING:
    from config import Config

dic_config = {
    PROCESS_QUEUE: None,
    DB_SECRET_KEY: None,
    SQLITE_CONFIG_DIR: None,
    APP_DB_FILE: None,
    UNIVERSAL_DB_FILE: None,
    TESTING: None,
    SHUTDOWN: None,
    PORT: None,
    PARTITION_NUMBER: None,
}

max_graph_config = {
    MaxGraphNumber.AGP_MAX_GRAPH.name: None,
    MaxGraphNumber.FPP_MAX_GRAPH.name: None,
    MaxGraphNumber.RLP_MAX_GRAPH.name: None,
    MaxGraphNumber.CHM_MAX_GRAPH.name: None,
    MaxGraphNumber.SCP_MAX_GRAPH.name: None,
    MaxGraphNumber.MSP_MAX_GRAPH.name: None,
    MaxGraphNumber.STP_MAX_GRAPH.name: None,
}


class CustomizeScheduler(APScheduler):
    RESCHEDULE_SECONDS = 30

    @staticmethod
    def _notify_info(notify_type: ListenNotifyType, id, func, **kwargs):
        # kwargs = codecs.encode(pickle.dumps(kwargs), 'base64').decode()
        func_module = func.__module__
        func_name = func.__qualname__
        dic_config[PROCESS_QUEUE][notify_type.name][id] = (id, func_module, func_name, kwargs)

    def add_job(self, id, func, is_main: bool = False, **kwargs):
        if not is_main:
            self._notify_info(ListenNotifyType.ADD_JOB, id, func, **kwargs)
            return

        super().add_job(id, func, **kwargs)

    def reschedule_job(self, id, func, func_params, is_main: bool = False):
        if not is_main:
            self._notify_info(ListenNotifyType.RESCHEDULE_JOB, id, func, **func_params)
            return

        run_time = datetime.now().astimezone(utc) + timedelta(seconds=self.RESCHEDULE_SECONDS)
        try:
            job = scheduler.get_job(id)
            job.next_run_time = run_time
            if job.trigger:
                if type(job.trigger).__name__ == 'DateTrigger':
                    job.trigger.run_date = run_time
                elif type(job.trigger).__name__ == 'IntervalTrigger':
                    job.trigger.start_date = run_time
            job.reschedule(trigger=job.trigger)
        except (JobLookupError, AttributeError):
            trigger = DateTrigger(run_date=run_time, timezone=utc)
            super().add_job(id, func, trigger=trigger, replace_existing=True, kwargs=func_params)

    def modify_and_reschedule_job(self, id, trigger, **update_kwargs) -> None:
        job = scheduler.get_job(id)
        if job is None:
            logger.error(f'Job {id} does not exist')
            return

        new_kwargs = job.kwargs
        new_kwargs.update(update_kwargs)
        job.modify(kwargs=new_kwargs)
        job.reschedule(trigger=trigger)


db = SQLAlchemy(
    engine_options={'poolclass': NullPool},
)
migrate = Migrate()
scheduler = CustomizeScheduler(BackgroundScheduler(timezone=utc))
ma = Marshmallow()
wtforms_json.init()

background_jobs = {}

LOG_IGNORE_CONTENTS = ('.html', '.js', '.css', '.ico', '.png')
# yaml config files
dic_yaml_config_file = {'basic': None, 'db': None, 'proc': None, 'ap': None, 'version': 0}
dic_yaml_config_file[YAML_START_UP] = os.path.join(os.getcwd(), YAML_START_UP_FILE_NAME)

# last request time
dic_request_info = {LAST_REQUEST_TIME: datetime.utcnow()}

# ############## init application metadata db ###############
# basic yaml
dic_yaml_config_instance = {}


def close_sessions():
    # close universal db session
    try:
        db.session.rollback()
        db.session.close()
    except Exception:
        pass


# ##########################################################


def create_app(object_name: str | Config = None) -> Flask:
    """Create and configure an instance of the Flask application."""
    from flask import request

    from ap.equations.error import FunctionFieldError

    from .aggregate_plot import create_module as agp_create_module
    from .analyze import create_module as analyze_create_module
    from .api import create_module as api_create_module
    from .calendar_heatmap import create_module as calendar_heatmap_create_module
    from .categorical_plot import create_module as categorical_create_module
    from .cli import create_command
    from .co_occurrence import create_module as co_occurrence_create_module
    from .common.logger import bind_user_info
    from .heatmap import create_module as heatmap_create_module
    from .multiple_scatter_plot import create_module as multiple_scatter_create_module
    from .parallel_plot import create_module as parallel_create_module
    from .ridgeline_plot import create_module as ridgeline_create_module
    from .sankey_plot import create_module as sankey_create_module
    from .scatter_plot import create_module as scatter_plot_create_module
    from .setting_module import create_module as setting_create_module
    from .table_viewer import create_module as table_viewer_create_module
    from .tile_interface import create_module as tile_interface_create_module
    from .trace_data import create_module as trace_data_create_module

    # from .script.migrate_cfg_data_source_csv import migrate_cfg_data_source_csv

    app = Flask(__name__)
    app.config.from_object(object_name)

    # Bridge will need sqlite3 db for scheduler.
    # app.config.update(SCHEDULER_JOBSTORES={'default': SQLAlchemyJobStore(url=app.config['SCHEDULER_DATABASE_URI'])})

    # table partition number
    dic_config[PARTITION_NUMBER] = app.config[PARTITION_NUMBER]

    # testing param
    dic_config[TESTING] = app.config.get(TESTING, None)

    db.init_app(app)
    migrate.init_app(app, db)
    ma.init_app(app)

    # yaml files path
    yaml_config_dir = app.config.get('YAML_CONFIG_DIR')
    dic_yaml_config_file[YAML_CONFIG_BASIC] = os.path.join(yaml_config_dir, YAML_CONFIG_BASIC_FILE_NAME)
    dic_yaml_config_file[YAML_CONFIG_DB] = os.path.join(yaml_config_dir, YAML_CONFIG_DB_FILE_NAME)
    dic_yaml_config_file[YAML_CONFIG_PROC] = os.path.join(yaml_config_dir, YAML_CONFIG_PROC_FILE_NAME)
    dic_yaml_config_file[YAML_CONFIG_AP] = os.path.join(yaml_config_dir, YAML_CONFIG_AP_FILE_NAME)
    dic_yaml_config_file[YAML_START_UP] = os.path.join(os.getcwd(), YAML_START_UP_FILE_NAME)

    # db secret key
    dic_config[DB_SECRET_KEY] = app.config[DB_SECRET_KEY]

    # sqlalchemy echo flag
    app.config['SQLALCHEMY_ECHO'] = app.config.get('DEBUG')

    dic_config['INIT_LOG_DIR'] = app.config.get('INIT_LOG_DIR')

    babel = Babel(app)
    Compress(app)

    api_create_module(app)
    scatter_plot_create_module(app)
    calendar_heatmap_create_module(app)
    heatmap_create_module(app)
    setting_create_module(app)
    trace_data_create_module(app)
    analyze_create_module(app)
    table_viewer_create_module(app)
    categorical_create_module(app)
    ridgeline_create_module(app)
    parallel_create_module(app)
    sankey_create_module(app)
    co_occurrence_create_module(app)
    multiple_scatter_create_module(app)
    tile_interface_create_module(app)
    agp_create_module(app)
    create_command(app)

    # BRIDGE STATION - Refactor DN & OSS version
    if is_app_source_dn():
        from .mapping_config import create_module as mapping_config_create_module
        from .name_aggregation_setting import create_module as name_aggregation_setting_module

        mapping_config_create_module(app)
        name_aggregation_setting_module(app)

    app.add_url_rule('/', endpoint='tile_interface.tile_interface')

    basic_config_yaml = BasicConfigYaml(dic_yaml_config_file[YAML_CONFIG_BASIC])
    start_up_yaml = BasicConfigYaml(dic_yaml_config_file[YAML_START_UP])
    hide_setting_page = basic_config_yaml.get_node(['info', 'hide-setting-page'], False)
    default_log_level = basic_config_yaml.get_node(['info', LOG_LEVEL], ApLogLevel.INFO.name)
    is_default_log_level = default_log_level == ApLogLevel.INFO.name
    dic_yaml_config_instance[YAML_CONFIG_BASIC] = basic_config_yaml
    dic_yaml_config_instance[YAML_START_UP] = start_up_yaml

    dic_yaml_config_file[YAML_CONFIG_VERSION] = ga_info.config_version

    sub_title = get_subtitle()

    lang = get_language()
    lang = lang or app.config['BABEL_DEFAULT_LOCALE']

    @babel.localeselector
    def get_locale():
        return request.cookies.get('locale') or lang

    @app.before_request
    def before_request_callback():
        g.request_start_time = time.time()
        # get the last time user request
        global dic_request_info

        # get api request thread id
        thread_id = request.form.get(REQUEST_THREAD_ID, None)
        set_request_g_dict(thread_id)

        resource_type = request.base_url or ''
        is_ignore_content = any(resource_type.endswith(extension) for extension in LOG_IGNORE_CONTENTS)
        if not is_ignore_content and request.blueprint != EXTERNAL_API:
            bind_user_info(request)

            # if not dic_config.get(TESTING):
            #     is_valid_browser, is_valid_version = check_client_browser(request)
            #     if not is_valid_version:
            #         # safari not valid version
            #         g.is_valid_version = True
            #
            #     if not is_valid_browser:
            #         # browser not valid
            #         content = {
            #             'title': _('InvalidBrowserTitle'),
            #             'message': _('InvalidBrowserContent'),
            #         }
            #         return render_template('none.html', **content)

    @app.after_request
    def after_request_callback(response: Response):
        if 'event-stream' in str(request.accept_mimetypes):
            return response

        # In case of text/html request, add information of disk capacity to show up on UI.
        if 'text/html' in str(request.accept_mimetypes) or 'text/html' in str(response.headers):
            from ap.common.disk_usage import (
                add_disk_capacity_into_response,
                get_disk_capacity_to_load_ui,
                get_ip_address,
            )

            dict_capacity = get_disk_capacity_to_load_ui()
            add_disk_capacity_into_response(response, dict_capacity)
            if not request.cookies.get('locale'):
                response.set_cookie('locale', lang)

            server_ip = get_ip_address()
            server_ip = [server_ip] + SERVER_ADDR
            client_ip = request.remote_addr
            is_admin = int(client_ip in server_ip)
            response.set_cookie('is_admin', str(is_admin))
            response.set_cookie('sub_title', sub_title)
            response.set_cookie('user_group', ga_info.app_group.value)

        # close app db session
        close_sessions()

        response.cache_control.public = True

        # better performance
        if not request.content_type:
            response.cache_control.max_age = 60 * 5
            response.cache_control.must_revalidate = True

        # check everytime (acceptable performance)
        # response.cache_control.no_cache = True
        response.direct_passthrough = False
        response.add_etag()
        response.make_conditional(request)
        if response.status_code == HTML_CODE_304:
            return response

        resource_type = request.base_url or ''
        is_ignore_content = any(resource_type.endswith(extension) for extension in LOG_IGNORE_CONTENTS)
        if not is_ignore_content:
            bind_user_info(request, response)
            response.set_cookie('log_level', str(is_default_log_level))
            response.set_cookie('hide_setting_page', str(hide_setting_page))
            response.set_cookie('app_version', ga_info.app_version)
            response.set_cookie('app_location', ga_info.app_source.value)
            response.set_cookie('app_type', ga_info.app_type.value)
            response.set_cookie('app_os', ga_info.app_os.value)

        if app.config.get('app_startup_time'):
            response.set_cookie(
                'app_startup_time',
                str(app.config.get('app_startup_time').strftime(DATE_FORMAT_STR)),
            )
        response.set_cookie('announce_update_time', str(ANNOUNCE_UPDATE_TIME))
        response.set_cookie('limit_checking_newer_version_time', str(LIMIT_CHECKING_NEWER_VERSION_TIME))

        return response

    @app.errorhandler(404)
    def page_not_found(e):
        from ap.common.flask_customize import render_template

        # note that we set the 404 status explicitly
        return render_template('404.html', do_not_send_ga=True), 404

    @app.errorhandler(500)
    def internal_server_error(e):
        # close app db session
        close_sessions()
        logger.exception(e)

        response = json_dumps({'code': e.code, 'message': str(e), 'dataset_id': get_log_attr(TraceErrKey.DATASET)})
        status = 500
        return Response(response=response, status=status)
        # return render_template('500.html'), 500

    @app.errorhandler(NoDataFoundException)
    def no_data_found(e):
        # close app db session
        close_sessions()
        logger.exception(e)

        response = json_dumps({'message': str('No data Found!')})
        status = 500
        return Response(response=response, status=status)

    # @app.errorhandler(Exception)
    # def unhandled_exception(e):
    #     # close app db session
    #     close_sessions()
    #     logger.exception(e)
    #
    #     response = json.dumps({
    #         "code": e.status_code,
    #         "message": e.message,
    #         "dataset_id": get_log_attr(TraceErrKey.DATASET)
    #     })
    #     return Response(response=response)

    @app.errorhandler(RequestTimeOutAPI)
    def request_timeout_api_error(e):
        """Return JSON instead of HTML for HTTP errors."""
        # close app db session
        close_sessions()

        # logger.error(e)

        # start with the correct headers and status code from the error
        # replace the body with JSON

        response = json_dumps(
            {
                'code': e.status_code,
                'message': e.message,
            },
        )
        return Response(response=response, status=408)

    @app.errorhandler(FunctionFieldError)
    def function_field_api_error(e: FunctionFieldError):
        status = 400
        response = json_dumps(e.parse())
        return Response(response=response, status=status)

    @app.errorhandler(FunctionErrors)
    def functions_api_error(e: FunctionErrors):
        status = 400
        response = json_dumps(e.parse())
        return Response(response=response, status=status)

    @app.teardown_appcontext
    def shutdown_session(exception=None):
        # close app db session
        close_sessions()

    return app


@log_execution_time()
def init_db(app):
    """
    init db with some parameter
    :return:
    """
    db.create_all(app=app)


# TODO: add field to this list if found something new remain id
list_id_alias = {
    'id',
    'column_id',
    'data_source_id',
    'dataset_id',
    'filter_id',
    'job_id',
    'parent_detail_id',
    'parent_id',
    'self_column_id',
    'self_process_id',
    'target_column_id',
    'target_process_id',
    'trace_id',
    'process_id',
    END_PROC,
    COND_PROC,
    GET02_VALS_SELECT,
}


@log_execution_time()
def get_basic_yaml_obj(file_name=None) -> BasicConfigYaml:
    if file_name:
        dic_yaml_config_instance[YAML_CONFIG_BASIC] = BasicConfigYaml(file_name)

    if not dic_yaml_config_instance.get(YAML_CONFIG_BASIC):
        dic_yaml_config_instance[YAML_CONFIG_BASIC] = BasicConfigYaml()
    return dic_yaml_config_instance[YAML_CONFIG_BASIC]


@log_execution_time()
def get_start_up_yaml_obj() -> BasicConfigYaml:
    if not dic_yaml_config_instance.get(YAML_START_UP):
        dic_yaml_config_instance[YAML_START_UP] = BasicConfigYaml(dic_yaml_config_file.get(YAML_START_UP))
    return dic_yaml_config_instance.get(YAML_START_UP)


def get_browser_debug(file_name=None) -> bool:
    browser_mode = os.environ.get(APP_BROWSER_DEBUG_ENV)
    if browser_mode is not None and browser_mode != '':
        return browser_mode.lower() == str(True).lower()

    start_up_yaml = get_start_up_yaml_obj()
    basic_config_yaml = get_basic_yaml_obj(file_name)
    if start_up_yaml.get_browser_debug() is not None:
        browser_mode = start_up_yaml.get_browser_debug()
    elif basic_config_yaml.get_browser_debug() is not None:
        browser_mode = basic_config_yaml.get_browser_debug()
    else:
        browser_mode = False
    return browser_mode


def get_file_mode(file_name=None) -> bool:
    file_mode = os.environ.get(APP_FILE_MODE_ENV)
    if file_mode is not None and file_mode != '':
        return file_mode.lower() == str(True).lower()

    start_up_yaml = get_start_up_yaml_obj()
    basic_config_yaml = get_basic_yaml_obj(file_name)
    if start_up_yaml.get_file_mode() is not None:
        file_mode = start_up_yaml.get_file_mode()
    elif basic_config_yaml.get_file_mode() is not None:
        file_mode = basic_config_yaml.get_file_mode()
    else:
        # BRIDGE STATION - Refactor DN & OSS version
        file_mode = is_app_source_dn()
    return file_mode


def get_language(file_name=None):
    lang = os.environ.get(APP_LANGUAGE_ENV)
    if lang is not None and lang != '' or lang == 'null':
        return lang

    start_up_yaml = get_start_up_yaml_obj()
    lang = start_up_yaml.get_node(['setting_startup', 'language'], None)

    if not start_up_yaml.dic_config or lang is None or lang == 'null':
        basic_config_yaml = get_basic_yaml_obj(file_name)
        lang = basic_config_yaml.get_node(['info', 'language'], None)

    if lang is None or lang == 'null':
        # BRIDGE STATION - Refactor DN & OSS version
        lang = 'ja' if is_app_source_dn() else 'en'

    return lang


def get_subtitle():
    subtitle = os.environ.get(APP_SUBTITLE_ENV)
    if subtitle is not None and subtitle != '':
        return subtitle

    start_up_yaml = get_start_up_yaml_obj()
    subtitle = start_up_yaml.get_node(['setting_startup', 'subtitle'], None)

    if not start_up_yaml.dic_config or subtitle is None or subtitle == 'null':
        subtitle = ''

    return subtitle


def multiprocessing_lock(lock_key: str):
    """Decorator to lock function in run time
    Arguments:
        lock_key {str} -- The key
        fn {function} -- [description]
    Returns:
        fn {function} -- [description]
    """

    if not lock_key:
        raise Exception('No lock key provided')

    def decorator(fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            if kwargs.get(IGNORE_MULTIPROCESSING_LOCK_KEY, False):
                del kwargs[IGNORE_MULTIPROCESSING_LOCK_KEY]
                return fn(*args, **kwargs)

            from multiprocessing import Manager

            process_queue: Manager.dict = dic_config.get(PROCESS_QUEUE, None)
            if process_queue is None:
                process_queue = get_process_queue()
                dic_config[PROCESS_QUEUE] = process_queue

            lock: Manager.Lock = process_queue.get(lock_key)
            if lock is None:
                lock = Manager.Lock()
                process_queue[lock_key] = lock

            with lock:
                try:
                    logger.debug('[MappingDataLock] Lock acquired')
                    result = fn(*args, **kwargs)
                except Exception as e:
                    raise e
                finally:
                    logger.debug('[MappingDataLock] Lock released')

            return result

        return wrapper

    return decorator
