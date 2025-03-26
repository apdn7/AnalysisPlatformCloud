import decimal
import re
from datetime import date, datetime, time

from ap.common.common_utils import is_int_32
from ap.common.constants import DataType


def gen_data_types(data):
    """
    check datatype of a list of columns
    :param data:
    :return:
    """
    data_types = {check_data_type(val) for val in data}

    if DataType.TEXT in data_types:
        return DataType.TEXT.value

    if DataType.DATETIME in data_types:
        return DataType.DATETIME.value

    if DataType.REAL in data_types:
        return DataType.REAL.value

    if DataType.INTEGER in data_types:
        return DataType.INTEGER.value

    return DataType.TEXT.value


# check data type of 1 data
def check_data_type(data):
    if data is None or data == '':
        return DataType.NULL

    if isinstance(data, datetime):
        return DataType.DATETIME

    if isinstance(data, (date, time)):
        return DataType.TEXT

    if isinstance(data, int):
        # In case number exceed MIN MAX INT32, it will be set to text type
        return DataType.INTEGER if is_int_32(data) else DataType.TEXT

    if isinstance(data, (decimal.Decimal, float)):
        return DataType.REAL

    try:
        if str(int(data)) == str(data):
            # In case number exceed MIN MAX INT32, it will be set to text type
            return DataType.INTEGER if is_int_32(int(data)) else DataType.TEXT
        else:
            return DataType.TEXT
    except ValueError:
        pass

    try:
        if float(data) or float(data) == 0:
            return DataType.REAL
    except ValueError:
        pass

    try:
        re_dt = (
            r'^\d{4}[-\/]\d{1,2}[-\/]\d{1,2}[\sT]\d{1,2}:\d{1,2}(:\d{1,2})?(\.\d{3,7})?((\s?([+-]?\d{1,2}:\d{2})?)|Z)?$'
        )
        matches = re.match(re_dt, data)
        if matches:
            return DataType.DATETIME
    except (ValueError, TypeError):
        pass

    return DataType.TEXT
