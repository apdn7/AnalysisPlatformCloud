import importlib
import threading
from time import sleep

from ap import PROCESS_QUEUE, SHUTDOWN, ListenNotifyType, dic_config, scheduler
from ap.api.setting_module.services.shutdown_app import handle_shutdown_app
from ap.common.common_utils import get_process_queue
from ap.common.constants import NOTIFY_DELAY_TIME, JobType
from ap.common.logger import logger
from ap.common.memoize import memoize, set_all_cache_expired
from ap.common.services.sse import background_announcer
from bridge.services.data_import import handle_category_error


def process_listen_job():
    """
    start job to listen changed from bridge
    :return:
    """
    job_id = f'{JobType.PROCESS_COMMUNICATE.name}'
    listen_thread = threading.Thread(target=listener, kwargs={'_job_id': job_id, '_job_name': job_id})

    # Starting the thread
    listen_thread.start()

    return True


# cache to avoid calling importlib multiple times
@memoize()
def get_method_from_module(module: str, qualname: str):
    """The real function signature from our code, need to dynamically import them before use
    See more: `python doc <https://docs.python.org/3/library/importlib.html#approximating-importlib-import-module>`

    WARNING: this function is copied from `EventBaseFunction.function` in rainbow code.
    """

    # we can import module without any trouble
    module = importlib.import_module(module)

    # need to get correct item from qualname
    # currently, we only allow 2 type of function
    # simple function: `fn`
    # classmethod function: `classname.fn`

    # check if we are handling class method here
    # if we have classmethod, this will be:
    # - classname, '.', function
    # otherwise
    # - '', '', function

    cls_name, _, fn_name = qualname.rpartition('.')

    # no class method, just get function
    if cls_name == '':
        method = getattr(module, fn_name)
    else:
        if '.' in cls_name:
            raise RuntimeError(f'Only support class method for now, please check {module}:{qualname} again')

        cls = getattr(module, cls_name)
        method = getattr(cls, fn_name)

    if method is None:
        raise RuntimeError(f'No method found for {module}:{qualname}')

    return method


def listener(_job_id=None, _job_name=None):
    process_queue = get_process_queue()
    dic_config[PROCESS_QUEUE] = process_queue

    dic_progress = process_queue[ListenNotifyType.JOB_PROGRESS.name]
    dic_add_job = process_queue[ListenNotifyType.ADD_JOB.name]
    dic_modify_job = process_queue[ListenNotifyType.RESCHEDULE_JOB.name]
    dic_clear_cache = process_queue[ListenNotifyType.CLEAR_CACHE.name]
    dic_shutdown = process_queue[ListenNotifyType.SHUTDOWN.name]
    dic_category_error = process_queue[ListenNotifyType.CATEGORY_ERROR.name]

    # import webbrowser
    # webbrowser.open_new(f'http://localhost:{dic_config[PORT]}')
    while True:
        if dic_config[SHUTDOWN]:
            logger.info('Stop communication loop')
            break

        sleep(NOTIFY_DELAY_TIME)
        add_job_ids = list(dic_add_job.keys()) or []
        for job_id in add_job_ids:
            id, module_name, fn_name, kwargs = dic_add_job.pop(job_id)
            func = get_method_from_module(module_name, fn_name)
            scheduler.add_job(id, func, is_main=True, **kwargs)

        modify_job_ids = list(dic_modify_job.keys()) or []
        for job_id in modify_job_ids:
            id, module_name, fn_name, func_params = dic_modify_job.pop(job_id)
            module = importlib.import_module(module_name)
            func = getattr(module, fn_name)
            scheduler.reschedule_job(id, func, func_params, is_main=True)

        for _ in range(len(dic_category_error)):
            _, category_error = dic_category_error.popitem()
            handle_category_error(category_error)

        for _ in range(len(dic_progress)):
            _, (dic_job, job_event) = dic_progress.popitem()
            background_announcer.announce(dic_job, job_event, is_main=True)

        for _ in range(len(dic_clear_cache)):
            cache_type, _ = dic_clear_cache.popitem()
            set_all_cache_expired(cache_type, is_main=True)

        for _ in range(len(dic_shutdown)):
            dic_shutdown.popitem()
            handle_shutdown_app(is_main=True)

    return True
