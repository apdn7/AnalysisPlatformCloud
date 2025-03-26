import pandas as pd
import simplejson
from flask import jsonify
from flask import render_template as _render_template
from orjson import OPT_NON_STR_KEYS, OPT_PASSTHROUGH_DATETIME, OPT_SERIALIZE_NUMPY, orjson

from ap import app_source, app_type, os_system
from ap.common.constants import BRIDGE_STATION_WEB_URL, MODE, AppSource, OsSystem, ServerType
from ap.common.memoize import memoize
from ap.common.services import http_content
from bridge.common.server_config import ServerConfig


def render_template(template_name_or_list, is_json_dumps_loads=True, **dic_context):
    dic_context.update(add_bridge_info_into_response())
    if is_json_dumps_loads:
        # convert bigInt to str
        dic_context = json_dumps_loads(dic_context)

    # BRIDGE STATION - Refactor DN & OSS version
    dic_context.update(
        {
            'app_type': app_type,
            'app_source': app_source,
            'os_system': os_system,
            'AppSource': {
                'DN': AppSource.DN.value,
                'OSS': AppSource.OSS.value,
            },
            'OsSystem': {
                'WINDOWS': OsSystem.WINDOWS.value,
                'LINUX': OsSystem.LINUX.value,
                'MACOS': OsSystem.MACOS.value,
            },
        },
    )

    return _render_template(template_name_or_list, **dic_context)


@memoize()
def add_bridge_info_into_response():
    """
    add bridge station web host, port
    :return:
    """
    from ap import get_basic_yaml_obj

    dic_output = {}
    server_type = ServerConfig.get_server_type()
    dic_output[MODE] = server_type.value
    dic_output[ServerConfig.BROWSER_MODE] = ServerConfig.get_browser_debug()
    dic_output[BRIDGE_STATION_WEB_URL] = None
    if server_type is ServerType.EdgeServer:
        basic_yml = get_basic_yaml_obj()
        bridge_host = basic_yml.get_bridge_station_web_host()
        bridge_port = basic_yml.get_bridge_station_web_port()
        dic_output[BRIDGE_STATION_WEB_URL] = f'http://{bridge_host}:{bridge_port}'
        # dic_output[BRIDGE_STATION_WEB_HOST] = bridge_host
        # dic_output[BRIDGE_STATION_WEB_PORT] = bridge_port

    return dic_output


# def json_dumps(dict_out):
#     # return jsonify(simplejson.loads(simple_json_dumps(*args)))
#     return simplejson.dumps(dict_out, ensure_ascii=False, default=http_content.json_serial)


def json_dumps(*args, **kwargs):
    if args:
        return jsonify(simplejson.loads(simple_json_dumps(*args)))

    if kwargs:
        return jsonify(simplejson.loads(simple_json_dumps(kwargs)))


def json_dumps_loads(dic_context):
    """
    convert bigInt to str
    :param dic_context:
    :return:
    """
    return simplejson.loads(simple_json_dumps(dic_context))


def orjson_dumps(dic_data):
    json_str = orjson.dumps(
        dic_data,
        option=OPT_NON_STR_KEYS | OPT_SERIALIZE_NUMPY | OPT_PASSTHROUGH_DATETIME,
        default=http_content.json_serial,
    )

    return json_str


def simple_json_dumps(dic_data):
    json_str = simplejson.dumps(dic_data, ensure_ascii=False, default=http_content.json_serial, ignore_nan=True)
    return json_str


def df_json_dumps(df):
    """
    Converts a pandas DataFrame into a JSON-serializable format.
    Converts int64 values to int and datetime values to ISO-formatted strings.

    Args:
        df (pandas.DataFrame): The DataFrame to convert.

    Returns:
        df
    """
    # cols = df.select_dtypes(include=['Int64', 'int64', 'float64', 'Float64', 'datetime64']).columns
    # if len(cols):
    #     df[cols] = df[cols].convert_dtypes().replace({pd.NA: None}).astype(object)
    if not df.empty:
        df = df.convert_dtypes().replace({pd.NA: None}).astype(object)
    return df
