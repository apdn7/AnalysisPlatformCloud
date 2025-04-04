import json
from datetime import date, datetime, time
from decimal import Decimal
from fractions import Fraction
from functools import singledispatch

import pandas as pd
import simplejson
from numpy import datetime64, float32, float64, int8, int16, int32, int64, ndarray
from orjson import OPT_NON_STR_KEYS, OPT_PASSTHROUGH_DATETIME, OPT_SERIALIZE_NUMPY, OPT_SORT_KEYS, orjson
from pandas import DataFrame, Series

from ap.common.constants import MAX_SAFE_INTEGER, DataType


@singledispatch
def json_serial(obj):
    try:
        # call toJSON method in customize class.
        # create toJSON method in your class
        if hasattr(obj, '__table__'):
            cols = [col.name for col in list(obj.__table__.columns)]
            return {col: obj.__dict__.get(col) for col in cols}
        elif hasattr(obj, '__dict__'):
            return obj.__dict__

        return str(obj)

    except AttributeError:
        return str(obj)


@json_serial.register(date)
@json_serial.register(datetime)
@json_serial.register(time)
def _(obj):
    return obj.isoformat()


@json_serial.register(datetime64)
def _(obj):
    return str(obj)


@json_serial.register(Decimal)
@json_serial.register(Fraction)
@json_serial.register(float32)
@json_serial.register(float64)
def _(obj):
    return float(obj)


@json_serial.register(int8)
@json_serial.register(int16)
@json_serial.register(int32)
def _(obj):
    return int(obj)


@json_serial.register(int)
def _(obj):
    if obj > MAX_SAFE_INTEGER:
        return str(obj)

    return int(obj)


@json_serial.register(int64)
def _(obj):
    return str(obj)


@json_serial.register(DataType)
def _(obj):
    # print(type(obj), obj)
    return obj.value


@json_serial.register(Series)
def _(obj):
    if hasattr(obj, 'to_list'):
        return obj.to_list()
    return json.JSONEncoder.default(obj)


@json_serial.register(set)
def _(obj):
    return list(obj)


@json_serial.register(type(pd.NA))
@json_serial.register(type(pd.NaT))
def _(obj):
    return None


# @json_serial.register(dict)
# def _(obj):
#     new_dict = {}
#     for key, value in obj.items():
#         if isinstance(key, int64):
#             new_dict[str(key)] = value
#         elif isinstance(key, (int8, int16, int32)):
#             new_dict[int(key)] = value
#         else:
#             new_dict[key] = value
#
#     return new_dict


@json_serial.register(ndarray)
def _(obj):
    if hasattr(obj, 'tolist'):
        return obj.tolist()
    return json.JSONEncoder.default(obj)


@json_serial.register(DataFrame)
def _(obj):
    if hasattr(obj, 'to_dict'):
        return obj.to_dict('list')
    return json.JSONEncoder.default(obj)


@json_serial.register(type(pd.NA))
@json_serial.register(type(pd.NaT))
def _(obj):
    return None


def build_dic_data(*args, **kwargs):
    if len(args) == 1 and args[0] is None:
        dic_data = {}
    else:
        try:
            dic_data = dict(*args)
        except Exception as e:
            if len(args) == 1:
                dic_data = args[0]
                return dic_data
            else:
                raise e

    dic_data.update(kwargs)
    return dic_data


def orjson_dumps(*args, **kwargs):
    dic_data = build_dic_data(*args, **kwargs)
    json_str = orjson.dumps(
        dic_data,
        option=OPT_NON_STR_KEYS | OPT_SERIALIZE_NUMPY | OPT_PASSTHROUGH_DATETIME | OPT_SORT_KEYS,
        default=json_serial,
    )

    return json_str


def json_dumps(*args, **kwargs):
    dic_data = build_dic_data(*args, **kwargs)
    return simplejson.dumps(dic_data, ensure_ascii=False, default=json_serial, ignore_nan=True)
