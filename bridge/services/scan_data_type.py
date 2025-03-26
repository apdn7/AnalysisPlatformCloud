import os
from typing import Iterable, List, Optional, Tuple, Union

import numpy as np
import pandas as pd
from pandas import DataFrame, Series
from pandas.core.dtypes.common import is_datetime64_ns_dtype, is_int64_dtype

from ap import multiprocessing_lock
from ap.api.setting_module.services.filter_settings import insert_default_filter_config_raw_sql
from ap.common.common_utils import (
    format_df,
    get_current_timestamp,
    get_nayose_path,
    is_boolean,
    is_int_16,
    is_int_32,
    read_feather_file,
    write_feather_file,
)
from ap.common.constants import (
    CATEGORY_COUNT,
    CATEGORY_RATIO,
    CATEGORY_TEXT_SHORTEST,
    DATE_TYPE_REGEX,
    INDEX_COL,
    MAPPING_DATA_LOCK,
    NUMBER_RECORD_FOR_SCAN_DATA,
    PREVIEW_DATA_RECORDS,
    TIME_TYPE_REGEX,
    BooleanStringDefinition,
    DataGroupType,
    DataType,
    FileExtension,
    MasterDBType,
    RawDataTypeDB,
    Suffixes,
)
from ap.common.logger import log_execution_time
from ap.common.pydn.dblib.postgresql import PostgreSQL
from ap.setting_module.models import (
    CfgDataTable,
    CfgDataTableColumn,
)
from ap.setting_module.models import MData as DbMData
from bridge.models.bridge_station import BridgeStationModel
from bridge.models.cfg_data_table import CfgDataTable as BSCfgDataTable
from bridge.models.cfg_data_table_column import CfgDataTableColumn as BSCfgDataTableColumn
from bridge.models.m_data import MData
from bridge.models.m_data_group import MDataGroup, get_primary_group
from bridge.models.m_process import MProcess
from bridge.models.mapping_category_data import MappingCategoryData
from bridge.models.mapping_factory_machine import MappingFactoryMachine
from bridge.models.mapping_process_data import MappingProcessData
from bridge.services.data_import import (
    generate_config_process,
    join_mapping_process_data,
    save_preview_data_file,
)
from bridge.services.etl_services.etl_controller import ETLController
from bridge.services.etl_services.etl_v2_multi_history_service import V2MultiHistoryService
from bridge.services.nayose_handler import (
    NAYOSE_FILE_NAMES,
)


def merge_value_counts(series, others):
    if series is None:
        if others.empty:
            return None

        return others.reset_index()

    if len(series) > 50_000:
        return series

    return series.append(others.reset_index()).groupby('index').sum()


@log_execution_time()
@BridgeStationModel.use_db_instance_generator()
def scan_data_type(
    data_table_id: int,
    data_steam: Iterable[Tuple[Optional[DataFrame], Optional[list], Union[int, float]]] = None,
    db_instance: PostgreSQL = None,
    is_user_approved_master: bool = False,
):
    yield 0

    cfg_data_table: BSCfgDataTable = BSCfgDataTable.get_by_id(db_instance, data_table_id, is_cascade=True)

    master_type = cfg_data_table.get_master_type()
    is_export_to_pickle_files = cfg_data_table.is_export_file()
    is_horizon_data = BSCfgDataTableColumn.get_column_names_by_data_group_types(
        db_instance,
        data_table_id,
        [DataGroupType.HORIZONTAL_DATA],
    )

    dic_data_series = {}
    full_data_ids = set()
    dict_process_series = {}
    yield 10

    cols, rows = MappingProcessData.get_data_ids_by_data_table_id(db_instance, data_table_id)
    df_data_ids = pd.DataFrame(rows, columns=cols, dtype='object')
    df_data_ids = format_df(df_data_ids)
    all_data_ids = list(df_data_ids[cols[0]].unique())
    mfc_rows = MappingFactoryMachine.get_process_id_with_data_table_id(db_instance, [data_table_id])
    all_process_ids = [row.get(MData.Columns.process_id.name) for row in mfc_rows]
    existed_process_ids = MProcess.get_existed_process_ids(db_instance, all_process_ids)
    len_all_data_ids = len(all_data_ids)
    if cfg_data_table.is_has_serial_col():
        len_all_data_ids = len_all_data_ids + len(set(existed_process_ids))  # + serial column of process

    scan_data_type_records = scan_data_type_per_column(
        db_instance,
        cfg_data_table,
        data_steam,
        is_user_approved_master=is_user_approved_master,
    )
    df_m_data = MData.get_all_as_df(
        db_instance,
        select_cols=[
            MData.Columns.id.name,
            MData.Columns.process_id.name,
            MData.Columns.data_group_id.name,
        ],
    )
    df_m_data_group = MDataGroup.get_all_as_df(
        db_instance,
        select_cols=[MDataGroup.Columns.id.name, MDataGroup.Columns.data_name_sys.name],
    )

    def _stop_generator_(_data_generator, _scan_data_type_records):
        if not (isinstance(_data_generator, (list, tuple))):
            next(_data_generator, None)  # to update scan_status of reading files
            _data_generator.close()
        _scan_data_type_records.close()

    df_merge = df_m_data.merge(df_m_data_group, on=MData.Columns.data_group_id.name)
    dict_m_data = df_merge.set_index('data_id').to_dict()[MDataGroup.Columns.data_name_sys.name]
    for data_id, series, process_id, outer_master_df, data_generator in scan_data_type_records:
        # for horizon data, get only one round
        if data_id is None and len(is_horizon_data):
            # all data_ids enough records
            _stop_generator_(data_generator, scan_data_type_records)
            break

        master_df = outer_master_df.tail(PREVIEW_DATA_RECORDS)
        if data_id in full_data_ids:
            continue

        data_name_sys = dict_m_data.get(data_id)
        if data_id in dic_data_series:
            dic_data_series[data_id] = dic_data_series[data_id].append(series)
        else:
            dic_data_series[data_id] = series
            # get preview data
            if process_id in dict_process_series:
                dict_process_series[process_id][data_name_sys] = series.tail(PREVIEW_DATA_RECORDS).reset_index(
                    drop=True,
                )
            else:
                dict_process_series[process_id] = pd.DataFrame({data_name_sys: series.tail(PREVIEW_DATA_RECORDS)})
                df_process = dict_process_series.get(process_id).reset_index(drop=True)
                master_df = master_df.reset_index(drop=True)
                dict_process_series[process_id] = pd.concat([df_process, master_df], axis=1)

        if len(dic_data_series[data_id]) >= NUMBER_RECORD_FOR_SCAN_DATA:
            full_data_ids.add(data_id)
            # all data_ids enough records
            if len(full_data_ids) >= len_all_data_ids:
                _stop_generator_(data_generator, scan_data_type_records)
                break

    yield 50

    dic_data_types = {
        data_id: guess_data_type(db_instance, dic_data_series.get(data_id), data_id, data_table_id)
        for data_id in set(all_data_ids + list(dic_data_series))
    }
    df = pd.DataFrame(
        dic_data_types.items(),
        columns=[MappingProcessData.Columns.data_id.name, MData.Columns.data_type.name],
    )

    if master_type == MasterDBType.V2_MULTI_HISTORY.name:
        # force text data type for all sub part no columns in V2 Multi History datasource
        V2MultiHistoryService.force_type_for_sub_part_no_columns(df, df_merge)

    if is_export_to_pickle_files:
        if is_user_approved_master:
            db_instance.connection.rollback()  # Rollback data type, keep user defined data types
            gen_config_and_preview_data(db_instance, cfg_data_table, dict_process_series, existed_process_ids)
        else:
            export_mapping_data_type_to_files(df, cfg_data_table.id)
        yield 95
    else:
        update_m_data_data_type_raw_sql(db_instance, df, existed_process_ids)
        yield 80

        gen_config_and_preview_data(
            db_instance,
            cfg_data_table,
            dict_process_series,
            existed_process_ids,
        )
        yield 95

    yield 100


@multiprocessing_lock(MAPPING_DATA_LOCK)
def export_mapping_data_type_to_files(df: DataFrame, data_table_id: int):
    df[CfgDataTableColumn.data_table_id.name] = data_table_id
    mapping_process_data_file_path = os.path.join(
        get_nayose_path(),
        str(data_table_id),
        f'{NAYOSE_FILE_NAMES.get(MappingProcessData.__name__)}.{FileExtension.Feather.value}',
    )

    if not os.path.exists(mapping_process_data_file_path):
        raise Exception('Mapping process data file does not exist')

    # order by data type and drop duplicate
    mapping_process_data_df = read_feather_file(mapping_process_data_file_path)
    order_data_type_dic = {
        RawDataTypeDB.BOOLEAN.value: 0,
        RawDataTypeDB.SMALL_INT.value: 1,
        RawDataTypeDB.INTEGER.value: 3,
        RawDataTypeDB.BIG_INT.value: 4,
        RawDataTypeDB.REAL.value: 5,
        RawDataTypeDB.DATETIME.value: 6,
        RawDataTypeDB.CATEGORY.value: 7,
        RawDataTypeDB.TEXT.value: 8,
    }

    mapping_process_data_df[INDEX_COL] = mapping_process_data_df.index
    update_data_type_df = mapping_process_data_df[
        mapping_process_data_df[MappingProcessData.Columns.data_id.name].isin(
            df[MappingProcessData.Columns.data_id.name].unique(),
        )
    ][
        [
            INDEX_COL,
            CfgDataTableColumn.data_table_id.name,
            CfgDataTableColumn.data_type.name,
            MappingProcessData.Columns.data_id.name,
        ]
    ]

    # Fill data type for columns without data type
    non_data_type_df = update_data_type_df[update_data_type_df[CfgDataTableColumn.data_type.name].isnull()]
    new_data_type_df = non_data_type_df.merge(
        df,
        how='inner',
        on=[
            MappingProcessData.Columns.data_id.name,
            CfgDataTableColumn.data_table_id.name,
        ],
        suffixes=Suffixes.KEEP_RIGHT,
    ).set_index(INDEX_COL)
    if not new_data_type_df.empty:
        mapping_process_data_df.loc[new_data_type_df.index, CfgDataTableColumn.data_type.name] = new_data_type_df[
            CfgDataTableColumn.data_type.name
        ]

    # Fill new data type (bigger type) for columns that exist data type
    exist_data_type_df = update_data_type_df[~update_data_type_df[CfgDataTableColumn.data_type.name].isnull()]
    new_exist_data_type_df = df[
        df[MappingProcessData.Columns.data_id.name].isin(
            exist_data_type_df[MappingProcessData.Columns.data_id.name].unique(),
        )
    ]
    new_exist_data_type_df = new_exist_data_type_df.merge(
        exist_data_type_df[
            [MappingProcessData.Columns.data_table_id.name, MappingProcessData.Columns.data_id.name, INDEX_COL]
        ],
        how='inner',
        on=[MappingProcessData.Columns.data_table_id.name, MappingProcessData.Columns.data_id.name],
    )
    update_exist_data_type_df = (
        pd.concat([exist_data_type_df, new_exist_data_type_df])
        .groupby([CfgDataTableColumn.data_table_id.name, MappingProcessData.Columns.data_id.name])
        .apply(
            lambda x: x.sort_values(
                [MData.Columns.data_type.name],
                ascending=False,
                key=lambda y: y.map(order_data_type_dic),
            ),
        )
    )
    if not update_exist_data_type_df.empty:
        update_exist_data_type_df = update_exist_data_type_df.drop_duplicates(
            subset=[CfgDataTableColumn.data_table_id.name, MappingProcessData.Columns.data_id.name],
        ).set_index(INDEX_COL)
        mapping_process_data_df.loc[
            update_exist_data_type_df.index,
            CfgDataTableColumn.data_type.name,
        ] = update_exist_data_type_df[CfgDataTableColumn.data_type.name]

    mapping_process_data_df = mapping_process_data_df.drop(columns=[INDEX_COL])
    write_feather_file(mapping_process_data_df, mapping_process_data_file_path)


def gen_config_and_preview_data(
    db_instance: PostgreSQL,
    cfg_data_table: Union[CfgDataTable, BSCfgDataTable],
    dict_process_series: dict,
    existed_process_ids: list[int],
):
    generate_config_process(db_instance, cfg_data_table=cfg_data_table, existed_processes_ids=existed_process_ids)
    insert_default_filter_config_raw_sql(db_instance, cfg_data_table.id)

    for process_id, df_process in dict_process_series.items():
        if process_id in existed_process_ids:
            save_preview_data_file(db_instance, cfg_data_table.id, process_id, df_process)


@log_execution_time()
def scan_data_type_per_column(
    db_instance: PostgreSQL,
    cfg_data_table: Union[CfgDataTable, BSCfgDataTable],
    data_steam: Iterable[Tuple[Optional[DataFrame], Optional[list], Union[int, float]]] = None,
    is_user_approved_master: bool = False,
):
    data_id_col = MappingProcessData.Columns.data_id.name
    primary_group = get_primary_group(db_instance=db_instance)
    etl_service = ETLController.get_etl_service(cfg_data_table, db_instance=db_instance)
    etl_service.is_user_approved_master = is_user_approved_master
    data_steam = etl_service.get_data_for_data_type(generator_df=data_steam, db_instance=db_instance)
    mapping_cols = [
        primary_group.PROCESS_ID,
        primary_group.PROCESS_NAME,
        primary_group.PROCESS_ABBR,
        primary_group.DATA_ID,
        primary_group.DATA_NAME,
        primary_group.DATA_ABBR,
        primary_group.PROD_FAMILY_ID,
        primary_group.PROD_FAMILY_NAME,
        primary_group.PROD_FAMILY_ABBR,
        MappingProcessData.Columns.data_table_id.name,
    ]

    df_mapping_data = None
    df_m_data = MData.get_all_as_df(
        db_instance,
        select_cols=[
            MData.Columns.id.name,
            MData.Columns.process_id.name,
            MData.Columns.data_group_id.name,
        ],
    )
    dict_data_id_process = dict(zip(df_m_data[data_id_col], df_m_data[MData.Columns.process_id.name]))

    # get serial data_id
    df_m_data_serial = df_m_data[df_m_data[MData.Columns.data_group_id.name] == DataGroupType.DATA_SERIAL.value]
    dict_process_serial = dict(zip(df_m_data_serial[MData.Columns.process_id.name], df_m_data_serial[data_id_col]))
    count_record = 0
    scanned_data_ids = []
    for df, dic_df_horizons, percentage in data_steam:
        if df is None or df.empty:
            continue
        count_record = count_record + len(df)
        # horizontal holding data
        cols = [col for col in mapping_cols if col in df.columns]
        for quality_name, data_values in dic_df_horizons.items():
            _df = pd.DataFrame()
            _df[primary_group.DATA_VALUE] = data_values
            _df[primary_group.DATA_NAME] = quality_name
            _df[cols] = df[cols]
            _df[INDEX_COL] = _df.index
            _df, df_mapping_data = join_mapping_process_data(db_instance, _df, primary_group, df_mapping_data)
            _df = _df[~_df[data_id_col].isin(scanned_data_ids)]
            for idx, (data_id, df_values) in enumerate(_df.groupby(data_id_col)):
                s_values = df_values[primary_group.DATA_VALUE]
                if data_id in scanned_data_ids:
                    break
                if len(s_values) >= NUMBER_RECORD_FOR_SCAN_DATA:
                    scanned_data_ids.append(data_id)

                process_id = dict_data_id_process[data_id]
                master_df = df.loc[df_values[INDEX_COL]]
                # get data values
                yield data_id, s_values, process_id, master_df, data_steam

                # get serial data values
                if idx == 0 and primary_group.DATA_SERIAL in df:
                    serial_data_id = dict_process_serial[process_id]
                    serial_value_counts = df[primary_group.DATA_SERIAL]
                    yield serial_data_id, serial_value_counts, process_id, master_df, data_steam

        # end one round
        if not dic_df_horizons:
            # vertical holding data
            df_not_horizons, *_ = join_mapping_process_data(db_instance, df, primary_group)
            df_not_horizons = df_not_horizons[~df_not_horizons[data_id_col].isin(scanned_data_ids)]
            for idx, (data_id, df_values) in enumerate(df_not_horizons.groupby(data_id_col)):
                process_id = dict_data_id_process[data_id]
                # get data values
                if primary_group.DATA_VALUE in df_values.columns:
                    s_values = df_values[primary_group.DATA_VALUE]
                    if len(s_values) >= NUMBER_RECORD_FOR_SCAN_DATA:
                        scanned_data_ids.append(data_id)

                    yield data_id, s_values, process_id, df_values, data_steam

                # get serial data values
                if dict_process_serial:
                    serial_data_id = int(dict_process_serial[process_id])
                    serial_value_counts = df_values[primary_group.DATA_SERIAL].drop_duplicates()
                    if len(serial_value_counts) >= NUMBER_RECORD_FOR_SCAN_DATA:
                        scanned_data_ids.append(serial_data_id)

                    yield serial_data_id, serial_value_counts, process_id, df_values, data_steam
        elif count_record >= NUMBER_RECORD_FOR_SCAN_DATA:
            yield None, None, None, None, data_steam


@log_execution_time()
def update_m_data_data_type_raw_sql(db_instance: PostgreSQL, df: DataFrame, process_ids: list[int] = None):
    df[MData.Columns.data_type.name].fillna(DataType.TEXT.value, inplace=True)
    dic_data_ids = dict(
        zip(
            df[MappingProcessData.Columns.data_id.name].astype(str),
            df[MData.Columns.data_type.name],
        ),
    )

    m_datas: List[MData]
    if process_ids:
        m_datas = MData.get_in_process_ids(db_instance, process_ids)
    else:
        m_datas = MData.get_in_ids(db_instance, list(dic_data_ids))

    for m_data in m_datas:
        if m_data.data_type is None or m_data.data_group_id == DataGroupType.DATA_SERIAL.value:
            dic_update_values = {MData.Columns.updated_at.name: get_current_timestamp()}

            data_id = str(m_data.id)
            dic_update_values[MData.Columns.data_type.name] = dic_data_ids.get(data_id, DataType.TEXT.value)

            MData.update_by_conditions(
                db_instance,
                dic_update_values,
                dic_conditions={MData.Columns.id.name: m_data.id},
            )

    return True


@log_execution_time()
def guess_data_type(db_instance: PostgreSQL, series: Series, data_id, data_table_id):
    if series is None or series.empty:
        return DataType.TEXT.value

    origin_series = series.dropna()
    if origin_series.empty:
        return DataType.TEXT.value

    if is_datetime64_ns_dtype(origin_series):
        if is_date_series(origin_series):  # in case of DATE data type
            return RawDataTypeDB.DATE.value
        elif is_time_series_match_pattern(origin_series):  # in case of TIME data type
            return RawDataTypeDB.TIME.value
        return DataType.DATETIME.value

    count_series = origin_series.size

    # check float
    s = pd.to_numeric(origin_series, errors='coerce')
    is_number = s.notnull().all()

    # check start with zero
    if is_number:
        data_type = origin_series.dtype.name
        if data_type in ('object', 'string'):
            str_series = origin_series.astype(str) if data_type == 'object' else origin_series

            if str_series.replace("'", '').str.startswith('00').any():
                is_number = False

    if is_number:
        data_type = RawDataTypeDB.REAL.value
        if (np.mod(s, 1) == 0).sum() == count_series:
            if is_boolean(s).sum() == count_series:
                data_type = RawDataTypeDB.BOOLEAN.value
            elif is_int_16(s).sum() == count_series:
                data_type = RawDataTypeDB.SMALL_INT.value
            elif is_int_32(s).sum() == count_series:
                data_type = RawDataTypeDB.INTEGER.value
                if is_category_type(origin_series):
                    data_type = RawDataTypeDB.CATEGORY.value
                    insert_mapping_category_data(db_instance, s, data_id, data_table_id)
            elif is_int64_dtype(s):
                data_type = RawDataTypeDB.BIG_INT.value
                if is_category_type(origin_series):
                    data_type = RawDataTypeDB.CATEGORY.value
                    insert_mapping_category_data(db_instance, s, data_id, data_table_id)
            else:
                data_type = RawDataTypeDB.TEXT.value
                if is_category_type(origin_series):
                    data_type = RawDataTypeDB.CATEGORY.value
                    insert_mapping_category_data(db_instance, s, data_id, data_table_id)
    else:
        # cast to string before using .str accessor
        s = origin_series.astype(pd.StringDtype()).str.strip()  # Remove leading and trailing characters
        s = pd.to_datetime(s, errors='coerce')
        if s.notnull().all():
            data_type = RawDataTypeDB.DATETIME.value
            if is_date_series(s):  # in case of DATE data type
                data_type = RawDataTypeDB.DATE.value
            elif is_time_series_match_pattern(origin_series):  # in case of TIME data type
                data_type = RawDataTypeDB.TIME.value
        elif is_date_series_match_pattern(origin_series):
            data_type = RawDataTypeDB.DATE.value
        elif is_time_series_match_pattern(origin_series):
            data_type = RawDataTypeDB.TIME.value
        else:
            data_type = RawDataTypeDB.TEXT.value
            # check if serial 'true', 'false' -> boolean
            unique_values = origin_series.unique()
            if set(unique_values).issubset({BooleanStringDefinition.true.name, BooleanStringDefinition.false.name}):
                data_type = RawDataTypeDB.BOOLEAN.value
            else:
                try:
                    max_len = origin_series.str.len().max()
                except Exception:
                    max_len = CATEGORY_TEXT_SHORTEST

                if max_len >= CATEGORY_TEXT_SHORTEST and is_category_type(origin_series):
                    data_type = RawDataTypeDB.CATEGORY.value
                    insert_mapping_category_data(db_instance, origin_series, data_id, data_table_id)

    return data_type


def is_date_series(series: Series) -> bool:
    """
    Check if series is a date without time series or not
    Args:
        series (Series): a series contains datetime or date value

    Returns:
        True if series is a date without time series, else False
    """

    series_without_tz = series.reset_index(drop=True).dt.tz_localize(None)
    s_date_without_time = pd.to_datetime(series_without_tz.dt.date)
    delta_time = (series_without_tz - s_date_without_time).apply(lambda x: x.total_seconds())
    return (delta_time == 0).all()


def is_date_series_match_pattern(series: Series) -> bool:
    """
    Check if series is a date without time series or not
    Args:
        series (Series): a series contains datetime or date value

    Returns:
        True if series is a date without time series, else False
    """

    matched_series = series.astype(str).str.match(DATE_TYPE_REGEX)
    return matched_series.all()


def is_time_series_match_pattern(series: Series):
    """
    Check if series is a time series or not
    Args:
        series (Series): a series contains datetime or time value

    Returns:
        True if series is a time series, else False
    """

    matched_series = series.astype(str).str.match(TIME_TYPE_REGEX)
    return matched_series.all()


def insert_mapping_category_data(db_instance: PostgreSQL, series, data_id, data_table_id):
    m_data: DbMData = MData.get_by_id(db_instance, int(data_id))
    if not m_data.data_type:
        df = pd.DataFrame()
        df[MappingCategoryData.Columns.t_category_data.name] = series.unique()
        df[MappingCategoryData.Columns.data_id.name] = data_id
        df[MappingCategoryData.Columns.data_table_id.name] = data_table_id
        db_instance.bulk_insert(MappingCategoryData.get_table_name(), df.columns.tolist(), df.values.tolist())


def is_category_type(s: Series):
    s_size = s.size
    if s_size < 32:
        return False

    unique_count = s.unique().size
    if s_size >= 1000:
        duplicate_ratio = unique_count / s_size
        if unique_count < CATEGORY_COUNT and duplicate_ratio <= CATEGORY_RATIO:
            return True
    elif unique_count < 4:
        return True

    return False


def collect_preview_data(dict_process_series, dict_m_data, df_sample_data, data_id):
    dic_process_with_data_name = dict_m_data.get(data_id)
    process_id = dic_process_with_data_name.get(MData.Columns.process_id.name)
    data_name = dic_process_with_data_name.get(MDataGroup.Columns.data_name_sys.name)
    df_sample_data = df_sample_data.rename(columns={data_id: data_name})
    df_sample_data[MData.Columns.process_id.name] = process_id
    if process_id not in dict_process_series:
        dict_process_series[process_id] = []
    rows_head = df_sample_data[data_name]
    dict_process_series[process_id].append({data_name: rows_head})
    return dict_process_series, process_id


def set_scan_data_type_status_done(db_instance: PostgreSQL, data_table_id: int):
    cfg_data_table: BSCfgDataTable = BSCfgDataTable.get_by_id(db_instance, data_table_id, is_cascade=True)
    etl_service = ETLController.get_etl_service(cfg_data_table, db_instance=db_instance)
    etl_service.set_all_scan_data_type_status_done()
