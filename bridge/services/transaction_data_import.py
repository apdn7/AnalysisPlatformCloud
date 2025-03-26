import threading

import pandas as pd
from pandas import Series

from ap.setting_module.models import CfgProcess
from bridge.models.transaction_model import TransactionData

pd.options.mode.chained_assignment = None  # default='warn'

COL_PER_GROUP = 10

# TODO : important hold last cycle_id per process
lock = threading.Lock()  # multi thread on Edge Server


def gen_transaction_partition_table(db_instance, df, transaction_data_obj: TransactionData, time_col):
    try:
        # Try to remove timezone before insert into transaction table to avoid value of datetime column change
        if df[time_col].dt.tz:
            df[time_col] = df[time_col].dt.tz_convert('UTC').dt.tz_localize(None)
    except Exception:
        pass

    months = get_unique_months(df[time_col])
    for year_month in months:
        transaction_data_obj.create_partition_by_time(db_instance, year_month)

    return True


def get_all_sensor_models(ignore_t_master_data=False):
    models = []
    if not ignore_t_master_data:
        # models.append()
        pass

    return models


def get_transaction_import_columns(process_id):
    # get columns from parent cfg process that should be import as transaction.
    # todo select columns import as normal transaction data
    common_columns = []

    # temp.二律背反. ở đây dùng tạm get link column từ cfg data table để biết có vấn đề này thôi. (luôn get ra rỗng)
    # đúng ra lấy link column của process
    cfg_process: CfgProcess = CfgProcess.get_by_id(process_id)
    linking_columns = cfg_process.get_serials(column_name_only=False)

    serial_col = cfg_process.get_serials(column_name_only=False)

    # remove column is linking column and serial column same time.
    linking_columns_ids = [col.id for col in linking_columns]
    serial_col = [col for col in serial_col if col.id not in linking_columns_ids]

    return list(filter(lambda col: col is not None, [*common_columns, *serial_col, *linking_columns]))


def get_unique_months(series: Series):
    s = pd.to_datetime(series, errors='coerce').dropna()
    # Cover for timezone (but not necessary now)
    # s_next_day = s + pd.Timedelta(days=1)
    # s_back_day = s + pd.Timedelta(days=-1)
    # s = pd.concat([s, s_next_day, s_back_day])
    return list((s.dt.year * 100 + s.dt.month).unique())
