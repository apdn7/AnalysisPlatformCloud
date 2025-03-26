import psycopg2
from apscheduler.triggers.cron import CronTrigger

from ap.common.constants import DEFAULT_POSTGRES_SCHEMA, JobType
from ap.common.scheduler import add_job_to_scheduler, scheduler_app_context
from ap.setting_module.models import JobManagement
from ap.setting_module.services.background_process import send_processing_info
from bridge.models.bridge_station import BridgeStationModel


def run_db_maintenance_job():
    job_id = JobType.DATABASE_MAINTENANCE.name
    trigger = CronTrigger(hour=3, minute=0)

    dic_import_param = {'_job_id': job_id, '_job_name': job_id}
    add_job_to_scheduler(
        job_id,
        job_id,
        trigger,
        db_maintenance_job,
        run_now=False,
        dic_import_param=dic_import_param,
    )
    # scheduler.add_job(job_id, db_maintenance_job, replace_existing=True, trigger=trigger,
    #                   kwargs=dict(_job_id=job_id, _job_name=job_id))


@scheduler_app_context
def db_maintenance_job(_job_id=None, _job_name=None):
    """scheduler job to delete process from db

    Keyword Arguments:
        _job_id {[type]} -- [description] (default: {None})
        _job_name {[type]} -- [description] (default: {None})
    """
    gen = db_maintenance()
    send_processing_info(gen, _job_name, is_check_disk=False)


def db_maintenance():
    sql = f'''
    SELECT conrelid::regclass AS table_name, conname AS primary_key
    FROM   pg_constraint
    WHERE  contype = 'p' AND connamespace = '{DEFAULT_POSTGRES_SCHEMA}'::regnamespace AND conparentid = 0
    '''

    with BridgeStationModel.get_db_proxy() as db_instance:
        db_instance.connection.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        _, rows = db_instance.run_sql(sql, row_is_dict=False)

        table_count = len(rows)
        ignore_tables = [JobManagement.get_table_name()]
        for idx, (table_name, index_name) in enumerate(rows, 1):
            if table_name in ignore_tables:
                continue

            db_instance.execute_sql(f'REINDEX TABLE CONCURRENTLY {table_name}')
            db_instance.execute_sql(f'CLUSTER {table_name} USING {index_name}')

            jump_percent = idx * 100 // table_count
            yield jump_percent

    yield 100
