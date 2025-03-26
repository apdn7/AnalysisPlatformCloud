import csv
import glob
import os
from enum import Enum

import pandas as pd

from ap.common.common_utils import add_addition_cols
from ap.common.constants import MasterDBType, ServerType
from ap.common.pydn.dblib.postgresql import PostgreSQL
from bridge.common.bridge_station_config_utils import PostgresSequence
from bridge.common.server_config import ServerConfig
from bridge.models.bridge_station import BridgeStationModel
from bridge.models.cfg_data_source import CfgDataSource
from bridge.models.cfg_data_source_db import CfgDataSourceDB
from bridge.models.etl_mapping import EtlMappingUtil

SHIFT_JIS_ENCODING = 'cp932'
UTF8_ENCODING = 'utf8'


def dump_sample_data(db_instance: PostgreSQL = None, source_data_folder=None):
    if not source_data_folder:
        print('No data was found.')
        return

    if db_instance is None:
        with BridgeStationModel.get_db_proxy() as db_instance:
            dump_from_csv(db_instance, source_data_folder)
    else:
        dump_from_csv(db_instance, source_data_folder)


def create_dummy_data(data_folder):
    try:
        with BridgeStationModel.get_db_proxy() as db_instance:
            cfg_data_source_db = CfgDataSourceDB.get_by_master_type(db_instance, MasterDBType.EFA.name)
            if cfg_data_source_db:
                CfgDataSourceDB.delete_by_pk(db_instance, cfg_data_source_db, mode=0)
                CfgDataSource.delete_by_pk(db_instance, cfg_data_source_db.cfg_data_source, mode=0)
            EtlMappingUtil.truncate_all_mapping_data(db_instance)
            dump_sample_data(db_instance, data_folder)
    except Exception as e:
        print(e)


def dump_from_csv(db_instance: PostgreSQL, source_data_folder, is_truncate=False, extensions=['csv']):
    edge_server_ignore_tables = ['cfg_repository']
    bridge_station_ignore_tables = []
    files = []
    for extension in extensions:
        files += list(glob.glob(f'{source_data_folder}/*.{extension}'))
    files.sort()
    for file_relative_path in files:
        file_name_with_ext = os.path.basename(file_relative_path)
        # file name has struct like 1.etl_bridge_table.csv
        split_name = file_name_with_ext.split('.')
        if len(split_name) < 3:
            print(f'file {split_name} has wrong format name')
            continue

        table_name = split_name[1]
        print(f'-----------------------------{table_name}-----------------------------------')
        print(f'Dumping data from {file_relative_path} into table {table_name} ...')

        # delete
        if ServerConfig.get_server_type() == ServerType.EdgeServer:
            ignore_tables = edge_server_ignore_tables
        else:
            ignore_tables = bridge_station_ignore_tables

        if table_name in ignore_tables:
            continue

        if is_truncate:
            db_instance.execute_sql(f'DELETE FROM {table_name}')
        else:
            cols, rows = db_instance.run_sql(f'SELECT * FROM {table_name} LIMIT 1')
            if rows:
                continue

        # insert
        insert_sample_data(file_relative_path, db_instance, table_name)
        PostgresSequence.set_max_sequence_id(db_instance, table_name)
        db_instance.connection.commit()

    print('Done!!!')


def read_data(f_name):
    with open(f_name, 'r', encoding=UTF8_ENCODING) as f:
        delimiter = '\t' if f_name[-3:] == 'tsv' else ','
        rows = csv.reader(f, delimiter=delimiter, quotechar=None, quoting=csv.QUOTE_NONE)
        for row in rows:
            yield row


def convert_bool_int(val: str):
    return 1 if val.lower() in [str(True).lower(), str('T').lower(), str(1).lower()] else 0


def convert_localhost(val: str):
    if val == 'host.docker.internal':
        return 'localhost'


class ConvertFunc(Enum):
    CONVERT_BOOL_INT = (1, convert_bool_int)
    CONVERT_LOCAL_HOST = (2, convert_localhost)


def insert_sample_data(file_path, db_instance, table_name):
    data = read_data(file_path)
    headers = next(data)
    next(data)
    convert_funcs = next(data)
    rows = list(data)

    # tunghh: no need any more, Edge and Bridge have same db now.
    # flg_env = 1
    # if isinstance(db_instance, SQLite3):
    #     flg_env = 2
    #
    # use_headers = [_col for _col, _env in zip(headers, target_envs) if
    #                not _env or not str(_env).strip() or _env == flg_env]
    use_headers = headers
    dic_convert_funcs = dict(zip(headers, convert_funcs))
    df = pd.DataFrame(rows, columns=headers)
    for col in use_headers:
        df[col] = df[col].apply(lambda x: None if x == '' else x)
        func_str = dic_convert_funcs.get(col)
        if not func_str:
            continue

        if isinstance(db_instance, PostgreSQL):
            continue

        convert_func = ConvertFunc[func_str].value[1]
        df[col] = df[col].apply(convert_func)

    add_addition_cols(db_instance, df, table_name)

    if len(df):
        rows = df.values.tolist()
        db_instance.bulk_insert(table_name, df.columns, rows)
