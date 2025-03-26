import shutil
from typing import List, Tuple

import psycopg2

from ap.common.common_utils import get_server_name_and_ip
from ap.common.constants import DiskUsageStatus
from ap.common.logger import logger
from ap.common.memoize import memoize
from bridge.common.server_config import ServerConfig
from bridge.models.bridge_station import BridgeStationModel
from bridge.models.cfg_constant import CfgConstantModel
from bridge.services.decorators import run_once_async

grep_param = ServerConfig.get_postgres_mounted_dir()


class DiskUsageInterface:
    @classmethod
    def get_disk_usage(cls, path=None):
        raise NotImplementedError


class PostgreDiskUsage(DiskUsageInterface):
    """
    Gets disk usage of hard disk that Postgres was installed on.

    DO NOT mis-understand with Postgres' db disk usage.

    """

    sql_init_function = rf'''
                    CREATE OR REPLACE FUNCTION sys_df()
                    RETURNS SETOF text[] as
                    $$
                    BEGIN
                        CREATE TEMP TABLE IF NOT EXISTS tmp_sys_df (content text) ON COMMIT DROP;
                        COPY tmp_sys_df FROM PROGRAM 'df | grep {grep_param}';
                        RETURN QUERY SELECT regexp_split_to_array(content, '\s+') FROM tmp_sys_df;
                    END;$$
                    LANGUAGE plpgsql;'''

    def __init__(self, row: [Tuple, List]):
        self.source = row[0]
        self.total = int(row[1])
        self.used = int(row[2])
        self.available = int(row[3])
        self.used_percent = row[4]
        self.mounted = row[5]  # unused

    def __str__(self):
        return (
            f'source: {self.source}; '
            f'block: {self.total}; '
            f'used: {self.used}; '
            f'available: {self.available}; '
            f'used_percent: {self.used_percent}; '
            f'mounted: {self.mounted};'
        )

    @staticmethod
    def init_function():
        with BridgeStationModel.get_db_proxy() as db_instance:
            db_instance.execute_sql(PostgreDiskUsage.sql_init_function)

    @classmethod
    def get_disk_usage(cls, path=None):
        # NOTE: in case of WSL2, this is virtual disk. DO NOT compare with your Windows' real hard disk.
        # path: unused.
        try:
            sql = 'select * from sys_df();'
            with BridgeStationModel.get_db_proxy() as db_instance:
                _, rows = db_instance.run_sql(sql, row_is_dict=False)
        except psycopg2.InternalError as e:  # In case of function sys_df() is syntax error, or grep string not found.
            rows = None
            logger.info(e)
            logger.warning(f'Check "sys_df()" syntax or "grep {grep_param}"')

        logger.info('====== POSTGRES DISK INFO ======')
        usage = PostgreDiskUsage(rows[0][0]) if rows else None
        logger.info(usage)
        logger.info('================================')

        return usage


class MainDiskUsage(DiskUsageInterface):
    """
    Checks disk usage of disk/partition that main application was installed on.

    """

    @classmethod
    def get_disk_usage(cls, path=None):
        return shutil.disk_usage(path=path)


def get_disk_usage_percent(path=None):
    """
    Gets disk usage information.

    :param path: disk location<br>In case not pass this argument, it will be set './' as default location
    :return: a tuple of (disk status, used percent)
    """
    if not path:
        path = './'  # as default dir

    rules: Tuple[DiskUsageInterface] = ServerConfig.get_disk_usage_rule()
    with BridgeStationModel.get_db_proxy() as db_instance:
        dict_status_measures = {
            CfgConstantModel.get_warning_disk_usage(db_instance): DiskUsageStatus.Warning,
            CfgConstantModel.get_error_disk_usage(db_instance): DiskUsageStatus.Full,
        }
        dict_limit_capacity = {y: x for x, y in dict_status_measures.items()}  # switch key value

    status = DiskUsageStatus.Normal
    used_percent = 0
    for checker in rules:
        usage = checker.get_disk_usage(path)
        used_percent = round(usage.used / usage.total * 100)
        for measure in sorted(dict_status_measures.keys()):
            if used_percent >= measure:
                status = dict_status_measures.pop(measure)
        if not dict_status_measures:
            break

    return status, used_percent, dict_limit_capacity


@memoize(duration=15 * 60)
def get_disk_capacity():
    """
    Get information of disk capacity on Bridge Station & Postgres DB

    :return: <b>DiskCapacityException</b> object that include disk status, used percent and message if have.
    """
    disk_status, used_percent, dict_limit_capacity = get_disk_usage_percent()
    print(f'Disk usage: {used_percent}% - {disk_status.name}')

    message = ''
    if disk_status == DiskUsageStatus.Full:
        message = (
            'Data import has stopped because the hard disk capacity of `__SERVER_INFO__` has reached '
            f'{dict_limit_capacity.get(DiskUsageStatus.Full)}%. '
            'Data import will restart when unnecessary data is deleted and the free space increases.'
        )
    elif disk_status == DiskUsageStatus.Warning:
        message = (
            'Please delete unnecessary data because the capacity of the hard disk of `__SERVER_INFO__` has '
            f'reached {dict_limit_capacity.get(DiskUsageStatus.Warning)}%.'
        )

    server_info = get_server_name_and_ip(ServerConfig.get_server_type())
    message = message.replace('__SERVER_INFO__', server_info)
    return DiskCapacityException(
        disk_status,
        used_percent,
        server_info,
        ServerConfig.get_server_type().name,
        dict_limit_capacity.get(DiskUsageStatus.Warning),
        dict_limit_capacity.get(DiskUsageStatus.Full),
        message,
    )


@run_once_async
def get_disk_capacity_once(_job_id=None):
    """
    Get information of disk capacity on Bridge Station & Postgres DB and always return DiskCapacityException object

    Attention: DO NOT USE ANYWHERE ELSE, this is only used in send_processing_info method to serve checking
     disk capacity for each job.

    :param _job_id: serve to check & run only once for each <b>_job_id</b>
    :return: <b>DiskCapacityException</b> object
    """
    return get_disk_capacity()


class DiskCapacityException(Exception):
    """Exception raised for disk usage exceed the allowed limit.

    Attributes:
        disk_status -- status of disk usage
        used_percent -- amount of used storage
        server_info -- String of server information
        server_type -- Type of server
        warning_limit_percent -- limit level
        error_limit_percent -- limit level
        message -- explanation of the error
    """

    def __init__(
        self,
        disk_status,
        used_percent,
        server_info,
        server_type,
        warning_limit_percent,
        error_limit_percent,
        message,
    ):
        self.disk_status: DiskUsageStatus = disk_status
        self.used_percent = used_percent
        self.server_info = server_info
        self.server_type = server_type
        self.warning_limit_percent = warning_limit_percent
        self.error_limit_percent = error_limit_percent
        self.message = message
        super().__init__(self.message)

    def to_dict(self):
        return {
            'disk_status': self.disk_status.name,
            'used_percent': self.used_percent,
            'server_info': self.server_info,
            'server_type': self.server_type,
            'warning_limit_percent': self.warning_limit_percent,
            'error_limit_percent': self.error_limit_percent,
            'message': self.message,
        }
