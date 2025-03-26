from typing import Tuple

from ap.common.common_utils import get_current_timestamp


def import_master(db_instance, table_name, column_names, rows: Tuple[Tuple]):
    """
    import n rows into 1 master table
    :param db_instance:
    :param table_name:
    :param column_names:
    :param rows:
    :return:
    """
    start_tm = get_current_timestamp()
    try:
        res = db_instance.bulk_insert(table_name, column_names, rows, is_replace=True)
    except Exception as e:
        res = e

    end_tm = get_current_timestamp()

    return res, start_tm, end_tm
