import os
from datetime import datetime
from typing import Any, Dict, Iterable, Iterator, Optional, Tuple, Type, Union

import numpy as np
import pandas as pd
from pandas import DataFrame

from ap import multiprocessing_lock
from ap.common.common_utils import (
    get_basename,
    get_error_path,
    get_nayose_path,
    make_dir_from_file_path,
    read_feather_file,
)
from ap.common.constants import (
    DEFAULT_NONE_VALUE,
    DUMMY_DATA_ID,
    DUMMY_DATA_NAME,
    DUMMY_PROCESS_NAME,
    EMPTY_STRING,
    MAPPING_DATA_LOCK,
    BaseMasterColumn,
    DataGroupType,
    DataType,
    EFAMasterColumn,
    FileExtension,
    MasterDBType,
    OtherMasterColumn,
    SoftwareWorkshopMasterColumn,
    Suffixes,
    V2HistoryMasterColumn,
    V2MasterColumn,
)
from ap.common.logger import log_execution_time, logger
from ap.common.pydn.dblib.postgresql import PostgreSQL
from ap.setting_module.models import CfgDataSource, CfgDataTable, CfgDataTableColumn
from bridge.models.bridge_station import BridgeStationModel
from bridge.models.cfg_data_source import CfgDataSource as BSCfgDataSource
from bridge.models.cfg_data_table import CfgDataTable as BSCfgDataTable
from bridge.models.m_data_group import MDataGroup as BSMDataGroup
from bridge.models.mapping_factory_machine import MappingFactoryMachine
from bridge.models.mapping_part import MappingPart
from bridge.models.mapping_process_data import MappingProcessData
from bridge.services.data_import import (
    get_pair_source_col_bridge_col,
    transform_horizon_columns_for_import,
)
from bridge.services.nayose_handler import ALL_DATA_RELATION, NAYOSE_FILE_NAMES


class ETLService:
    MAPPING_GROUP = {
        MappingPart: (
            # ↓=== m_prod_family ===↓
            DataGroupType.PROD_FAMILY_ID,
            DataGroupType.PROD_FAMILY_NAME,
            DataGroupType.PROD_FAMILY_ABBR,
            # ↑=== m_prod_family ===↑
            # ↓=== m_prod ===↓
            DataGroupType.PROD_ID,
            DataGroupType.PROD_NAME,
            DataGroupType.PROD_ABBR,
            # ↑=== m_prod ===↑
            # ↓=== m_location ===↓
            DataGroupType.LOCATION_NAME,
            DataGroupType.LOCATION_ABBR,
            # ↑=== m_location ===↑
            # ↓=== m_part_type ===↓
            DataGroupType.PART_TYPE,
            DataGroupType.PART_NAME,
            DataGroupType.PART_ABBR,
            # ↑=== m_part_type ===↑
            # ↓=== m_part ===↓
            DataGroupType.PART_NO,
            DataGroupType.PART_NO_FULL,
            # ↑=== m_part ===↑
        ),
        MappingFactoryMachine: (
            # ↓=== m_location ===↓
            DataGroupType.LOCATION_NAME,
            DataGroupType.LOCATION_ABBR,
            # ↑=== m_location ===↑
            # ↓=== m_factory ===↓
            DataGroupType.FACTORY_ID,
            DataGroupType.FACTORY_NAME,
            DataGroupType.FACTORY_ABBR,
            # ↑=== m_factory ===↑
            # ↓=== m_plant ===↓
            DataGroupType.PLANT_ID,
            DataGroupType.PLANT_NAME,
            DataGroupType.PLANT_ABBR,
            # ↑=== m_plant ===↑
            # ↓=== m_dept ===↓
            DataGroupType.DEPT_ID,
            DataGroupType.DEPT_NAME,
            DataGroupType.DEPT_ABBR,
            # ↑=== m_dept ===↑
            # ↓=== m_sect ===↓
            DataGroupType.SECT_ID,
            DataGroupType.SECT_NAME,
            DataGroupType.SECT_ABBR,
            # ↑=== m_sect ===↑
            # ↓=== m_prod_family ===↓
            DataGroupType.PROD_FAMILY_ID,
            DataGroupType.PROD_FAMILY_NAME,
            DataGroupType.PROD_FAMILY_ABBR,
            # ↑=== m_prod_family ===↑
            # ↓=== m_prod ===↓
            DataGroupType.PROD_ID,
            DataGroupType.PROD_NAME,
            DataGroupType.PROD_ABBR,
            # ↑=== m_prod ===↑
            # ↓=== m_line_group ===↓
            DataGroupType.LINE_NAME,
            # ↑=== m_line_group ===↑
            # ↓=== m_line ===↓
            DataGroupType.LINE_ID,
            DataGroupType.LINE_NO,
            DataGroupType.OUTSOURCE,
            # ↑=== m_line ===↑
            # ↓=== m_equip_group ===↓
            DataGroupType.EQUIP_NAME,
            # ↑=== m_equip_group ===↑
            # ↓=== m_equip ===↓
            DataGroupType.EQUIP_ID,
            DataGroupType.EQUIP_NO,
            DataGroupType.EQUIP_PRODUCT_NO,
            DataGroupType.EQUIP_PRODUCT_DATE,
            # ↑=== m_equip ===↑
            # ↓=== m_prod_family ===↓
            DataGroupType.STATION_NO,
            # ↑=== m_prod_family ===↑
            # ↓=== m_process ===↓
            DataGroupType.PROCESS_ID,
            DataGroupType.PROCESS_NAME,
            DataGroupType.PROCESS_ABBR,
            # ↑=== m_process ===↑
        ),
        MappingProcessData: (
            # ↓=== m_prod_family ===↓
            DataGroupType.PROD_FAMILY_ID,
            DataGroupType.PROD_FAMILY_NAME,
            DataGroupType.PROD_FAMILY_ABBR,
            # ↑=== m_prod_family ===↑
            # ↓=== m_process ===↓
            DataGroupType.PROCESS_ID,
            DataGroupType.PROCESS_NAME,
            DataGroupType.PROCESS_ABBR,
            # ↑=== m_process ===↑
            # ↓=== m_unit ===↓
            DataGroupType.UNIT,
            # ↑=== m_unit ===↑
            # ↓=== m_data_group ===↓
            DataGroupType.DATA_NAME,
            DataGroupType.DATA_ABBR,
            # ↑=== m_data_group ===↑
            # ↓=== m_data ===↓
            DataGroupType.DATA_ID,
            # ↑=== m_data ===↑
        ),
    }  # sub part group data
    THE_ALL = ALL_DATA_RELATION

    def __init__(self, cfg_data_table: Union[CfgDataTable, BSCfgDataTable], db_instance: PostgreSQL = None):
        self.cfg_data_table = cfg_data_table
        self.data_source_id = cfg_data_table.data_source_id
        self.master_type = cfg_data_table.get_master_type()
        self.cfg_data_table_columns = cfg_data_table.get_sorted_columns()
        self.dic_mapping_group = self.get_mapping_column_groups()
        self.source_column_names, self.bridge_column_names, self.data_group_types = get_pair_source_col_bridge_col(
            self.cfg_data_table_columns,
            db_instance=db_instance,
        )
        self.get_date_col = self.cfg_data_table.get_date_col(column_name_only=True)
        self.datetime_cols = self.cfg_data_table.get_cols_by_data_type(DataType.DATETIME)
        self.auto_increment_rec = self.cfg_data_table.get_auto_increment_col_else_get_date(column_name_only=False)
        self.auto_increment_col = self.auto_increment_rec.column_name if self.auto_increment_rec else None
        self.cfg_data_source = (
            CfgDataSource.get_by_id(cfg_data_table.data_source_id)
            if db_instance is None
            else BSCfgDataSource(
                BSCfgDataSource.get_by_id(db_instance, cfg_data_table.data_source_id),
                is_cascade=True,
                db_instance=db_instance,
            )
        )
        self.is_export_to_pickle_files = self.cfg_data_table.is_export_file()

        self.data_name_column = self.get_column_name(DataGroupType.DATA_NAME.value)
        self.data_name_column = (
            self.data_name_column[0].get('source_name') if self.data_name_column else DataGroupType.DATA_NAME.name
        )
        self.data_value_column = self.get_column_name(DataGroupType.DATA_VALUE.value)
        self.data_value_column = (
            self.data_value_column[0].get('source_name') if self.data_value_column else DataGroupType.DATA_VALUE.name
        )
        self.data_id_column = self.get_column_name(DataGroupType.DATA_ID.value)
        self.data_id_column = (
            self.data_id_column[0].get('source_name') if self.data_id_column else DataGroupType.DATA_ID.name
        )
        self.unit_column = self.get_column_name(DataGroupType.UNIT.value)
        self.unit_column = self.unit_column[0].get('source_name') if self.unit_column else DataGroupType.UNIT.name
        self.is_user_approved_master = False

    def get_column_name(self, data_group_type_value):
        result = []
        for bridge_name, source_name, data_group_type in zip(
            self.bridge_column_names,
            self.source_column_names,
            self.data_group_types,
        ):
            if data_group_type_value == data_group_type:
                result.append({'bridge_name': bridge_name, 'source_name': source_name})

        return result

    @log_execution_time()
    def split_master_data(
        self,
        generator_df: Iterable[Tuple[Optional[DataFrame], Optional[list], Union[int, float]]],
    ) -> Iterator[
        Tuple[
            dict[Type[Union[MappingProcessData, MappingPart, MappingFactoryMachine, str]], DataFrame],
            Union[int, float],
        ]
    ]:
        """
        Separate big dataframe to 3 parts of mapping master data to be served for scan master process
        :param generator_df: an Iterable of raw collecting data
        :return: an Iterable that contains 3 parts of mapping master data
        """
        default_ignore_cols = [
            DataGroupType.DATA_VALUE.name,
            DataGroupType.DATA_SERIAL.name,
            DataGroupType.DATA_TIME.name,
            DataGroupType.AUTO_INCREMENTAL.name,
        ]

        for _df, *_, progress_percentage in generator_df:
            df = _df
            if df is None or len(df) == 0:
                continue

            df, ignore_cols, horizon_cols = self.transform_to_bridge_columns_for_scan(
                df,
                ignore_cols=default_ignore_cols,
            )
            dict_target_and_df: dict[
                Type[Union[MappingProcessData, MappingPart, MappingFactoryMachine, str]],
                DataFrame,
            ] = {}

            if DataGroupType.DATA_SERIAL.name not in df.columns:
                df[DataGroupType.DATA_SERIAL.name] = DEFAULT_NONE_VALUE

            # split into 3 groups corresponding to 3 mapping model
            for target_table, column_list in self.MAPPING_GROUP.items():
                cols = [col.name for col in column_list if col.name in df.columns]
                cols.append(CfgDataTableColumn.data_table_id.name)
                df_mapping_group = (
                    df[cols].drop_duplicates().reset_index(drop=True).replace({np.nan: DEFAULT_NONE_VALUE})
                )

                if target_table == MappingProcessData and horizon_cols:  # add DATA_NAME to df
                    master_cols = list(set(cols) - {DataGroupType.DATA_NAME.name, DataGroupType.DATA_ID.name})
                    df_master = df_mapping_group[master_cols].drop_duplicates().copy()
                    dfs = [df_mapping_group] if DataGroupType.DATA_NAME.name in df_mapping_group else []
                    for horizon_col in horizon_cols:
                        if horizon_col in ignore_cols:
                            continue

                        df_temp = df_master.copy()
                        df_temp[DataGroupType.DATA_NAME.name] = horizon_col
                        df_temp[DataGroupType.DATA_ID.name] = DEFAULT_NONE_VALUE
                        dfs.append(df_temp)

                    df_mapping_group = (
                        pd.concat(dfs, ignore_index=True)
                        .drop_duplicates()
                        .reset_index(drop=True)
                        .replace({np.nan: DEFAULT_NONE_VALUE})
                    )

                dict_target_and_df[target_table] = df_mapping_group

            self.collect_relationship_of_all_masters(
                df,
                dict_target_and_df,
                self.is_export_to_pickle_files,
                horizon_cols,
            )

            yield dict_target_and_df, progress_percentage

    @multiprocessing_lock(MAPPING_DATA_LOCK)
    def collect_relationship_of_all_masters(
        self,
        df_the_all: DataFrame,
        dict_target_and_df: dict[
            Type[Union[MappingProcessData, MappingPart, MappingFactoryMachine, str]],
            DataFrame,
        ],
        is_export_to_pickle_files: bool,
        horizon_cols: list[str],
    ):
        """
        collect relationship of all masters. That will be used on Mapping Config page
        :param df_the_all: a dataframe that contains all t_... columns and extracted master data
        :param dict_target_and_df: a dictionary that contains 3 part of mapping data
        :param is_export_to_pickle_files: a flag that mean it will be exported to file or not
        :param horizon_cols: a list of horizontal columns
        :return: void
        """
        if not is_export_to_pickle_files:
            return

        from bridge.services.master_data_import import dict_config_bs_name

        necessary_columns = set(df_the_all.columns.to_list()) - {
            DataGroupType.DATA_TIME.name,
            DataGroupType.DATA_VALUE.name,
            DataGroupType.DATA_SERIAL.name,
        }
        df_the_all = df_the_all[necessary_columns].drop_duplicates().reset_index(drop=True)
        df_the_all[CfgDataTableColumn.data_table_id.name] = self.cfg_data_table.id
        index_cols = [CfgDataTableColumn.data_table_id.name]
        for key, df_mapping_group in dict_target_and_df.items():
            new_index_col = f'{key.__name__}_INDEX'
            index_cols.append(new_index_col)

            # Get max index of previous saved feather file, then recalculate next index
            file_path = os.path.join(
                get_nayose_path(),
                str(self.cfg_data_table.id),
                f'{NAYOSE_FILE_NAMES.get(key.__name__)}.{FileExtension.Feather.value}',
            )

            if os.path.exists(file_path):
                # Increase index from exist index
                previous_df_mapping_group = read_feather_file(file_path)
                transaction_cols = [
                    col
                    for col in previous_df_mapping_group.columns.tolist()
                    if col.startswith('t_') or col in [new_index_col, CfgDataTableColumn.data_table_id.name]
                ]
                previous_df_mapping_group = previous_df_mapping_group[transaction_cols].fillna(EMPTY_STRING)
                previous_index = previous_df_mapping_group[new_index_col].astype(np.int).max() + 1
                next_index = 0 if pd.isnull(previous_index) else previous_index + 1

                df_all_index = df_mapping_group.rename(columns=dict_config_bs_name).fillna(EMPTY_STRING)
                df_all_index = (
                    df_all_index.reset_index()
                    .merge(
                        previous_df_mapping_group,
                        how='left',
                        on=df_all_index.columns.to_list(),
                        suffixes=Suffixes.KEEP_LEFT,
                    )
                    .set_index('index')
                    .drop_duplicates()[[new_index_col]]
                )
                new_index_df = df_all_index[df_all_index[new_index_col].isnull()][[new_index_col]]
                if not new_index_df.empty:
                    new_index_df[new_index_col] = list(range(next_index, len(new_index_df) + next_index))
                    df_all_index[new_index_col].update(new_index_df[new_index_col])
                df_mapping_group[new_index_col] = df_all_index[new_index_col]
                df_mapping_group.reset_index(drop=True, inplace=True)
            else:
                df_mapping_group.reset_index(inplace=True)
                df_mapping_group.rename(columns={'index': new_index_col}, inplace=True)

            # Join to the all df to get index column
            join_columns = list(
                set(df_mapping_group.columns.to_list())
                - {new_index_col}
                - set(
                    [
                        DataGroupType.DATA_ID.name,
                        DataGroupType.DATA_NAME.name,
                        DataGroupType.DATA_ABBR.name,
                    ]
                    if horizon_cols
                    else [],
                ),
            )
            join_columns = [col for col in join_columns if col in df_the_all and col in df_mapping_group]
            df_the_all = df_the_all.merge(
                df_mapping_group,
                on=join_columns,
                how='left',
                suffixes=Suffixes.KEEP_LEFT,
            )

        dict_target_and_df[ETLService.THE_ALL] = df_the_all[index_cols]

    def get_master_data(
        self,
        db_instance: PostgreSQL = None,
    ) -> Iterator[
        Tuple[
            dict[Type[Union[MappingProcessData, MappingPart, MappingFactoryMachine, str]], DataFrame],
            Union[int, float],
        ]
    ]:
        raise Exception

    def get_data_for_data_type(self, generator_df=None, db_instance: PostgreSQL = None):
        raise Exception

    @BridgeStationModel.use_db_instance()
    def set_all_scan_data_type_status_done(
        self,
        db_instance: PostgreSQL = None,
    ):
        raise NotImplementedError

    @BridgeStationModel.use_db_instance()
    def get_mapping_column_groups(self, db_instance: PostgreSQL = None):
        m_data_group_ids = [col.data_group_type for col in self.cfg_data_table_columns if col.data_group_type]
        bridge_columns = BSMDataGroup.get_data_group_in_group_types(db_instance, m_data_group_ids)
        dict_bridge_columns = {data_group.data_group_type: data_group.get_sys_name() for data_group in bridge_columns}
        dic_col_groups = {}
        for target, distinct_group in self.MAPPING_GROUP.items():
            dic_col_groups[target] = [
                dict_bridge_columns[item.value] for item in distinct_group if item.value in dict_bridge_columns
            ]
        return dic_col_groups

    def is_horizon_data(self):
        """
        check if the data is horizon
        :return:
        """
        data_types = [DataGroupType(column.data_group_type) for column in self.cfg_data_table_columns]
        if DataGroupType.DATA_VALUE in data_types:
            return False
        return True

    def get_horizontal_data_columns(self):
        """
        Get list of column name which are horizontal column
        :return: a list of column name
        """
        data_type_names = [
            (DataGroupType(column.data_group_type), column.column_name) for column in self.cfg_data_table_columns
        ]
        return [column_name for data_type, column_name in data_type_names if data_type == DataGroupType.HORIZONTAL_DATA]

    def have_real_master_columns(self):
        """
        check if the data table contain any master columns or not
        :return:
        """
        data_type_names = [
            (DataGroupType(column.data_group_type), column.column_name) for column in self.cfg_data_table_columns
        ]
        for data_type, column_name in data_type_names:
            if data_type not in [
                DataGroupType.DATA_SERIAL,
                DataGroupType.DATA_TIME,
                DataGroupType.AUTO_INCREMENTAL,
                DataGroupType.HORIZONTAL_DATA,
            ] and not BaseMasterColumn.is_dummy_column(column_name):
                return True
        return False

    def have_real_process_master_column(self):
        """
        check if the data table contain processID or processName master columns or not
        :return:
        """
        data_type_names = [
            (DataGroupType(column.data_group_type), column.column_name) for column in self.cfg_data_table_columns
        ]
        for data_type, column_name in data_type_names:
            if data_type in [
                DataGroupType.PROCESS_ID,
                DataGroupType.PROCESS_NAME,
            ] and not BaseMasterColumn.is_dummy_column(column_name):
                return True
        return False

    def generate_input_scan_master_for_horizontal_columns(self):
        horizontal_cols = [
            column.column_name
            for column in filter(
                lambda x: x.data_group_type == DataGroupType.HORIZONTAL_DATA.value,
                self.cfg_data_table_columns,
            )
        ]
        df = pd.DataFrame([[DEFAULT_NONE_VALUE] * len(horizontal_cols)], columns=horizontal_cols)

        dic_use_cols = {col.column_name: col.data_type for col in self.cfg_data_table_columns}
        master_columns, master_values = self.get_dummy_master_column_value()
        self.add_dummy_master_columns(df, master_columns, master_values, dic_use_cols)

        return df

    def get_dummy_master_column_value(self) -> Tuple[Dict[str, str], Dict[str, Any]]:
        return get_dummy_master_column_value(self.master_type, self.cfg_data_source.is_direct_import)

    def add_dummy_master_columns(
        self,
        df: DataFrame,
        master_columns: dict[str, str],
        master_values: dict[str, Any],
        dic_use_cols: dict[str, str],
    ):
        if not master_columns or not master_values:
            return

        df_columns = set(df.columns)
        use_cols = set(dic_use_cols.keys())
        add_cols = use_cols - df_columns
        for col in add_cols:
            if col not in master_columns:
                continue

            if col == DUMMY_PROCESS_NAME:
                default_value = self.cfg_data_table.name
            else:
                default_value = master_values.get(col, DEFAULT_NONE_VALUE)
            df[col] = default_value

    def add_dummy_horizon_columns(self, df):
        data_table_columns = self.cfg_data_table_columns
        data_table_columns = [data_table_column.column_name for data_table_column in data_table_columns]
        for col in data_table_columns:
            if col not in df.columns:
                df[col] = DEFAULT_NONE_VALUE

    @log_execution_time()
    def transform_to_bridge_columns_for_scan(
        self,
        df_origin: DataFrame,
        ignore_cols: list = None,
    ) -> Tuple[DataFrame, set[str], list[str]]:
        """
        :return:
            * set_ignore_cols: source columns where bridge columns existed in ignore_cols
            * horizon_cols: source column where its bridge column is not with HORIZONTAL_DATA and AUTO_INCREMENTAL
            * df: dataframe without horizontal columns which addition cfg_data_table_id
                this dataframe is renamed into bridge station name
        """
        set_ignore_cols = set(ignore_cols)

        horizon_cols: list[str] = []
        for source_col, bridge_col in zip(self.source_column_names, self.bridge_column_names):
            if bridge_col in [
                DataGroupType.HORIZONTAL_DATA.name,
                DataGroupType.AUTO_INCREMENTAL.name,
            ] and self.master_type not in [MasterDBType.V2_MULTI.name, MasterDBType.SOFTWARE_WORKSHOP.name]:
                # Not return horizon_cols because DATA_NAME & DATA_VALUE columns contain horizontal data already
                # for V2 Multi
                horizon_cols.append(source_col)
            elif bridge_col in set_ignore_cols:
                set_ignore_cols.add(source_col)

        df: DataFrame = df_origin.drop(columns=[col for col in horizon_cols if col in df_origin]).drop_duplicates()
        if CfgDataTableColumn.data_table_id.name not in df.columns:
            df[CfgDataTableColumn.data_table_id.name] = self.cfg_data_table.id

        self.rename_transaction_column_to_bridge_name(df, inplace=True)

        return df, set_ignore_cols, horizon_cols

    def rename_transaction_column_to_bridge_name(self, df: DataFrame, inplace=False):
        """
        Rename to bridge station name
        :param df: a dataframe that want to be renamed
        :param inplace : bool, default False
            Whether to return a new DataFrame. If True then value of copy is
            ignored.
        :return: a dataframe with column names were changed
        """
        dict_pair_source_bridge = {}
        for bridge_name, source_name in zip(self.bridge_column_names, self.source_column_names):
            if bridge_name != DataGroupType.HORIZONTAL_DATA.name and source_name in df:
                dict_pair_source_bridge[source_name] = bridge_name
        if inplace:
            df.rename(columns=dict_pair_source_bridge, inplace=True)
        else:
            return df.rename(columns=dict_pair_source_bridge)

    @log_execution_time()
    def transform_horizon_columns_for_import(
        self,
        df_original: DataFrame,
        only_horizon_col: bool = None,
        ignore_cols: list = None,
    ):
        return transform_horizon_columns_for_import(
            self.cfg_data_table,
            df_original,
            only_horizon_col=only_horizon_col,
            ignore_cols=ignore_cols,
        )

    def add_dummy_data_name_data_value_for_scan(self, df: DataFrame) -> DataFrame:
        # in case of horizontal data and have horizontal data columns in dataset
        horizontal_data_columns = []
        column_groups = {
            column.column_name: DataGroupType(column.data_group_type) for column in self.cfg_data_table_columns
        }
        for column_name in df.columns:
            data_group_type = column_groups.get(column_name)
            if data_group_type == DataGroupType.HORIZONTAL_DATA:
                horizontal_data_columns.append(column_name)

        if horizontal_data_columns:
            result_df = df.drop(columns=horizontal_data_columns)
            result_df.drop_duplicates(inplace=True)
            df_data_name = pd.DataFrame(
                data=zip([DEFAULT_NONE_VALUE] * len(horizontal_data_columns), horizontal_data_columns),
                columns=[DUMMY_DATA_ID, DUMMY_DATA_NAME],
            )
            df = result_df.merge(df_data_name, how='cross')

        return df

    @classmethod
    def add_dic_replace_dummy_data_col(cls, df, dic):
        if DUMMY_DATA_ID in df:
            dic[DUMMY_DATA_ID] = DataGroupType.DATA_ID.name
        if DUMMY_DATA_NAME in df:
            dic[DUMMY_DATA_NAME] = DataGroupType.DATA_NAME.name

    @classmethod
    def export_duplicate_data_to_file(cls, df_duplicate, data_table_name, file_name: str = None):
        if not df_duplicate.empty:
            now = datetime.now().strftime('%Y%m%d%H%M%S')
            file_name = f'_{get_basename(file_name)}' if file_name else ''
            duplicate_file_name = os.path.join(get_error_path(), f'pull_data_{data_table_name}{file_name}_{now}.csv')
            make_dir_from_file_path(duplicate_file_name)
            df_duplicate.to_csv(duplicate_file_name, index=False)
            logger.info('[CHECK_LOST_IMPORTED_DATA] Export duplicate data')

    def convert_df_horizontal_to_vertical(self, df_horizontal_data: DataFrame):
        raise Exception('Not implemented')

    @log_execution_time(prefix='etl_service')
    def convert_to_standard_data(
        self,
        df: DataFrame,
    ) -> DataFrame:
        """Convert dataframe with horizontal + vertical data to dataframe with only one type of vertical data

        :param df: a dataframe with horizontal + vertical data
        :return: dataframe with vertical data
        """

        horizontal_cols: list[str] = [
            data_table_column.column_name
            for data_table_column in self.cfg_data_table_columns
            if data_table_column.data_group_type == DataGroupType.HORIZONTAL_DATA.value
            and data_table_column.column_name in df
        ]
        df_vertical_data_from_horizontal_columns = self.convert_df_horizontal_to_vertical(df)
        df_vertical_data_without_horizontal_columns = df.drop(columns=horizontal_cols)

        return pd.concat(
            [df_vertical_data_from_horizontal_columns, df_vertical_data_without_horizontal_columns],
        ).reset_index(
            drop=True,
        )


def get_dummy_master_column_value(master_type: str, is_direct_import: bool) -> Tuple[Dict[str, str], Dict[str, object]]:
    master_columns = None
    master_values = None
    if master_type == MasterDBType.OTHERS.name:
        master_columns = OtherMasterColumn.get_default_column()
        master_values = OtherMasterColumn.get_default_value(is_direct_import)
    elif master_type in [MasterDBType.V2.name, MasterDBType.V2_MULTI.name]:
        master_columns = V2MasterColumn.get_default_column()
        master_values = V2MasterColumn.get_default_value()
    elif master_type == MasterDBType.EFA.name:
        master_columns = EFAMasterColumn.get_default_column()
        master_values = EFAMasterColumn.get_default_value()
    elif master_type in [MasterDBType.V2_HISTORY.name, MasterDBType.V2_MULTI_HISTORY.name]:
        master_columns = V2HistoryMasterColumn.get_default_column()
        master_values = V2HistoryMasterColumn.get_default_value()
    elif master_type == MasterDBType.SOFTWARE_WORKSHOP.name:
        master_columns = SoftwareWorkshopMasterColumn.get_default_column()
        master_values = SoftwareWorkshopMasterColumn.get_default_value()

    return master_columns, master_values
