import pandas as pd

from ap.common.constants import (
    DataType,
)
from ap.common.logger import log_execution_time
from bridge.models.bridge_station import BridgeStationModel
from bridge.models.cfg_process import CfgProcess


def to_type(df, column_name, data_type):
    if data_type == DataType.TEXT.value:
        return df[column_name].astype('string')
    elif data_type in [DataType.INTEGER.value, DataType.REAL.value]:
        return pd.to_numeric(df[column_name], errors='coerce')
    elif data_type == DataType.DATETIME.value:
        return pd.to_datetime(df[column_name], errors='coerce', format='%Y%m%d%H%M%S')


def rename_cycle_column(target_proc_id):
    return f'cycle_{target_proc_id}'


@log_execution_time()
def get_basic_config_process(proc_id):
    """

    :param proc_id:
    :return:
    """
    with BridgeStationModel.get_db_proxy() as db_instance:
        cfg_process = CfgProcess.get_by_process_id(db_instance, proc_id, is_cascade_column=True)
        if not cfg_process:
            raise Exception('no data config found')

        # sort column : serials first, auto_increment second, then others
        cfg_process.columns.sort(key=lambda c: (c.data_type, c.column_name))
        cfg_process.columns.sort(
            key=lambda c: int(c.is_serial_no or 0) * 2 + int(c.is_auto_increment or 0),
            reverse=True,
        )

        return cfg_process
