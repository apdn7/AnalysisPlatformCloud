import contextlib
import os

from ap import app_source, create_app, get_basic_yaml_obj, get_browser_debug
from ap.common.constants import (
    ANALYSIS_INTERFACE_ENV,
    APP_HOST_ENV,
    PORT,
    PROCESS_QUEUE,
    AppSource,
    ServerType,
)
from ap.common.logger import set_log_config
from ap.common.pydn.dblib.db_proxy import get_db_proxy
from bridge.common.disk_usage import MainDiskUsage, PostgreDiskUsage
from bridge.common.server_config import ServerConfig

env = os.environ.get(ANALYSIS_INTERFACE_ENV, 'prod')
app = create_app('config.%sConfig' % env.capitalize())

# --- DO NOT CHANGE THIS ORDERS ---
basic_yaml_config = get_basic_yaml_obj()
server_type = basic_yaml_config.get_current_mode()
ServerConfig.set_server_config({ServerConfig.SERVER_TYPE: server_type})
_dic_config = {ServerConfig.DB_PROXY: get_db_proxy, ServerConfig.LOCK: None}

ServerConfig.set_server_config(basic_yaml_config.dic_config)
ServerConfig.current_app = app

if server_type is ServerType.BridgeStationGrpc:
    _dic_config.update({ServerConfig.DISK_USAGE_CHECK: (PostgreDiskUsage, MainDiskUsage)})
elif server_type is ServerType.BridgeStationWeb:
    _dic_config.update({ServerConfig.DISK_USAGE_CHECK: (PostgreDiskUsage,)})
elif server_type in (ServerType.StandAlone, ServerType.EdgeServer):
    _dic_config.update({ServerConfig.DISK_USAGE_CHECK: (MainDiskUsage,)})

ServerConfig.set_server_config(_dic_config)
ServerConfig.set_server_config({ServerConfig.BROWSER_MODE: get_browser_debug()})

is_main = __name__ == '__main__'
set_log_config(is_main)
if is_main:
    from ap import dic_config, get_start_up_yaml_obj
    from ap.common.check_available_port import check_available_port
    from ap.common.common_utils import init_process_queue
    from ap.common.memoize import clear_cache

    # from ap.script.disable_terminal_close_button import disable_terminal_close_btn
    from bridge.redis_utils.pubsub import redis_connection
    from bridge.redis_utils.redis_storage_utils import clear_redis_storage
    from start_flask_server import serve_flask_server
    from start_grpc_server import serve_grpc_server

    port = os.environ.get(APP_HOST_ENV)

    # main params
    dic_start_up = get_start_up_yaml_obj().dic_config
    if not port and dic_start_up:
        port = os.environ.get(APP_HOST_ENV) or get_start_up_yaml_obj().dic_config['setting_startup'].get('port', None)

    if not port:
        port = basic_yaml_config.dic_config['info'].get('port-no') or app.config.get(PORT)

    check_available_port(port)

    dic_config[PORT] = int(port)

    # processes queue
    dic_config[PROCESS_QUEUE] = init_process_queue()

    # update interrupt jobs by shutdown immediately
    with app.app_context():
        from ap.setting_module.models import JobManagement

        with contextlib.suppress(Exception):
            JobManagement.update_interrupt_jobs()

    # Universal DB init
    # init_db(app)

    true_values = [True, 'true', '1', 1]

    if server_type is ServerType.BridgeStationWeb:
        # app_heartbeat()
        # TODO: open it later
        # print(f'Postgres\'s disk usage: {PostgresDiskUsage.get_disk_usage().used_percent}')
        if redis_connection() is None:
            print('CAN NOT START SERVER . BECAUSE OF REDIS PROBLEM')

        clear_redis_storage()

    # disable quick edit of terminal to avoid pause
    # is_debug = app.config.get('DEBUG')
    # if not is_debug:
    #     try:
    #        from ap.script.disable_terminal_quickedit import disable_quickedit

    #        disable_quickedit()
    #        from ap.script.hide_exe_root_folder import hide_bundle_folder, heartbeat_bundle_folder
    #        heartbeat_bundle_folder()
    #        hide_bundle_folder()
    #    except Exception:
    #        pass

    # BRIDGE STATION - Refactor DN & OSS version
    if app_source == AppSource.DN.value:
        # kick R process
        from ap.script.call_r_process import call_r_process

        call_r_process()

    # clear cache
    clear_cache()

    if not app.config.get('TESTING'):
        # hide close button of cmd
        # disable_terminal_close_btn()
        ...

    # TODO: khi khoi dong start edge server ,thi phai gan 1 bien online=0 de ko call grpc nua. de co the dung offline.
    if server_type is ServerType.BridgeStationGrpc:
        with app.app_context():
            serve_grpc_server(app, port)
    else:
        ServerConfig.is_main_thread = True
        ServerConfig.set_server_config(app.config)
        serve_flask_server(app, port, server_type, env)
