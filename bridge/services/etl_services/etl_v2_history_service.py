from typing import Iterable, Iterator, Optional, Tuple, Type, Union

import numpy as np
import pandas as pd
from pandas import DataFrame

from ap.common.common_utils import format_df, get_common_element
from ap.common.constants import (
    DEFAULT_NONE_VALUE,
    DataGroupType,
    TransactionForPurpose,
)
from ap.common.logger import log_execution_time
from ap.common.pydn.dblib.postgresql import PostgreSQL
from ap.setting_module.models import CfgDataTable
from bridge.models.bridge_station import BridgeStationModel
from bridge.models.cfg_data_table import CfgDataTable as BSCfgDataTable
from bridge.models.m_data import MData
from bridge.models.m_data_group import MDataGroup, get_primary_group
from bridge.models.m_part_type import AssyFlag, MPartType
from bridge.models.m_unit import MUnit
from bridge.models.mapping_factory_machine import MappingFactoryMachine
from bridge.models.mapping_part import MappingPart
from bridge.models.mapping_process_data import MappingProcessData
from bridge.services.etl_services.etl_v2_measure_service import V2MeasureService


class V2HistoryService(V2MeasureService):
    # This group has a lot special handling, DO NOT learn from this case
    TEM_GROUP_ID_COL = 'temp_group_id'

    MAPPING_GROUP = {
        MappingPart: (DataGroupType.PART_NO, DataGroupType.SUB_PART_NO),
        MappingFactoryMachine: (
            DataGroupType.LINE_ID,
            DataGroupType.EQUIP_ID,
            DataGroupType.LINE_NAME,
            DataGroupType.EQUIP_NAME,
            DataGroupType.PROCESS_ID,
            DataGroupType.PROCESS_NAME,
        ),
        MappingProcessData: (
            DataGroupType.PROCESS_ID,
            DataGroupType.DATA_ID,
            DataGroupType.PROCESS_NAME,
            DataGroupType.DATA_NAME,
        ),
    }

    def __init__(
        self,
        cfg_data_table: Union[CfgDataTable, BSCfgDataTable],
        root_directory=None,
        db_instance: PostgreSQL = None,
    ):
        super().__init__(cfg_data_table, root_directory, db_instance=db_instance)
        self.part_no_column = self.get_column_name(DataGroupType.PART_NO.value)
        self.part_no_column = (
            self.part_no_column[0].get('source_name') if self.part_no_column else DataGroupType.PART_NO.name
        )
        self.sub_part_no_column = self.get_column_name(DataGroupType.SUB_PART_NO.value)
        self.sub_part_no_column = (
            self.sub_part_no_column[0].get('source_name') if self.sub_part_no_column else DataGroupType.SUB_PART_NO.name
        )
        self.sub_lot_no_column = self.get_column_name(DataGroupType.SUB_LOT_NO.value)
        self.sub_lot_no_column = (
            self.sub_lot_no_column[0].get('source_name') if self.sub_lot_no_column else DataGroupType.SUB_LOT_NO.name
        )
        self.sub_tray_no_column = self.get_column_name(DataGroupType.SUB_TRAY_NO.value)
        self.sub_tray_no_column = (
            self.sub_tray_no_column[0].get('source_name') if self.sub_tray_no_column else DataGroupType.SUB_TRAY_NO.name
        )
        self.sub_serial_column = self.get_column_name(DataGroupType.SUB_SERIAL.value)
        self.sub_serial_column = (
            self.sub_serial_column[0].get('source_name') if self.sub_serial_column else DataGroupType.SUB_SERIAL.name
        )

        self.__IGNORE_SUB_COLUMNS__ = [
            self.sub_part_no_column,
            self.sub_lot_no_column,
            self.sub_tray_no_column,
            self.sub_serial_column,
        ]

    def drop_all_sub_columns(self, df: DataFrame):
        ignore_cols = [col for col in self.__IGNORE_SUB_COLUMNS__ if col in df.columns]
        if ignore_cols:
            df.drop(columns=ignore_cols, inplace=True)

    @log_execution_time(prefix='etl_v2_history_service')
    @BridgeStationModel.use_db_instance_generator()
    def get_master_data(
        self,
        db_instance: PostgreSQL = None,
    ) -> Iterator[
        Tuple[
            dict[Type[Union[MappingProcessData, MappingPart, MappingFactoryMachine, str]], DataFrame],
            Union[int, float],
        ]
    ]:
        generator_df: Iterator[
            Tuple[Optional[DataFrame], Optional[list], Union[int, float]]
        ] = super().get_transaction_data(for_purpose=TransactionForPurpose.FOR_SCAN_MASTER, db_instance=db_instance)
        yield from self.split_master_data(generator_df)

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
        for df_origin, target_files, progress_percentage in generator_df:
            df = df_origin.copy()  # avoid input origin df changes when determine new master data in pull data job

            data_value_column = self.get_column_name(DataGroupType.DATA_VALUE.value)
            data_value_column = (
                data_value_column[0].get('source_name') if data_value_column else DataGroupType.DATA_VALUE.name
            )
            data_serial_column = self.get_column_name(DataGroupType.DATA_SERIAL.value)
            data_serial_column = (
                data_serial_column[0].get('source_name') if data_serial_column else DataGroupType.DATA_SERIAL.name
            )
            data_time_column = self.get_column_name(DataGroupType.DATA_TIME.value)
            data_time_column = (
                data_time_column[0].get('source_name') if data_time_column else DataGroupType.DATA_TIME.name
            )

            df.drop(
                columns=[
                    col
                    for col in [
                        data_value_column,
                        data_serial_column,
                        data_time_column,
                    ]
                    if col in df.columns
                ],
                inplace=True,
            )
            df.drop_duplicates(inplace=True)
            df.reset_index(drop=True, inplace=True)

            yield from super().split_master_data([(df, target_files, progress_percentage)])

    @log_execution_time(prefix='etl_v2_history_service')
    def convert_to_standard_v2(
        self,
        df: DataFrame,
        for_purpose: TransactionForPurpose = None,
    ) -> DataFrame:
        # Convert horizontal columns to Vertical dataframe
        horizontal_cols: list[str] = [
            data_table_column.column_name
            for data_table_column in self.cfg_data_table_columns
            if data_table_column.data_group_type
            in [
                DataGroupType.HORIZONTAL_DATA.value,
            ]
            and data_table_column.column_name in df
        ]
        df_vertical_data_from_horizontal_columns = self.convert_df_horizontal_to_vertical(df)
        df = df.drop(columns=horizontal_cols)

        # Convert sub columns to Vertical dataframe
        df_error = df[(df[self.sub_part_no_column].isna()) | (df[self.sub_part_no_column].str.len() < 12)]  # temp
        df.drop(index=df_error.index, inplace=True)

        df_vertical_data_from_sub_columns = self.convert_v2_history_to_vertical_holding(df, for_purpose=for_purpose)

        return pd.concat([df_vertical_data_from_horizontal_columns, df_vertical_data_from_sub_columns]).reset_index(
            drop=True,
        )

    @log_execution_time()
    def convert_v2_history_to_vertical_holding(
        self,
        df: DataFrame,
        for_purpose: TransactionForPurpose = None,
    ):
        df = df.copy()

        if for_purpose is TransactionForPurpose.FOR_SCAN_MASTER:
            sub_part_df = df.dropna(subset=[self.sub_part_no_column])
            sub_part_df[self.part_no_column] = sub_part_df[self.sub_part_no_column]
            df = df.append(sub_part_df, ignore_index=True)

        df[self.sub_part_no_column] = df[self.sub_part_no_column].str[2:8]

        dfs = []
        keep_cols = list(
            set(df.columns.tolist())
            - {
                self.sub_part_no_column,
                self.sub_tray_no_column,
                self.sub_lot_no_column,
                self.sub_serial_column,
            },
        )
        for idx, df_grouped in df.groupby(self.sub_part_no_column):
            part_no = df_grouped[self.sub_part_no_column].iloc[0]
            for sub_col, sys_sub_col in [
                (self.sub_tray_no_column, DataGroupType.SUB_TRAY_NO.name),
                (self.sub_lot_no_column, DataGroupType.SUB_LOT_NO.name),
                (self.sub_serial_column, DataGroupType.SUB_SERIAL.name),
            ]:
                df_sub_col = df_grouped[keep_cols + [sub_col]].rename(
                    columns={
                        sub_col: self.data_value_column,
                    },
                )
                df_sub_col[self.data_name_column] = f'{sys_sub_col}_{part_no}'
                dfs.append(df_sub_col)

        return pd.concat(dfs).reset_index(drop=True)


@log_execution_time()
def get_distinct_sub_part(df: DataFrame):
    sub_part_no_col = DataGroupType.SUB_PART_NO.name
    part_no_col = DataGroupType.PART_NO.name
    assy_flag_col = MPartType.Columns.assy_flag.name

    df_part = pd.DataFrame()
    df_part[part_no_col] = df[part_no_col].drop_duplicates()
    df_part[assy_flag_col] = AssyFlag.Assy.value

    df_sub_part = pd.DataFrame()
    df_sub_part[part_no_col] = df[sub_part_no_col].drop_duplicates()
    df_sub_part[assy_flag_col] = AssyFlag.Part.value

    df_part = df_part.append(df_sub_part)
    df_part.drop_duplicates(subset=[part_no_col], inplace=True, keep='first')

    return df_part


def gen_the_all_df(df: DataFrame, unique_cols):
    sys_name = MDataGroup.Columns.data_name_sys.name
    primary_groups = get_primary_group()
    fact_id_col = MPartType.Columns.part_type_factid.name
    cols = get_common_element(unique_cols, df.columns) + [primary_groups.SUB_PART_NO]
    df_output = df.drop_duplicates(subset=cols)[cols]
    df_output[fact_id_col] = df_output[primary_groups.SUB_PART_NO].str[2:8]

    data_group_types = [
        DataGroupType.SUB_TRAY_NO.name,
        DataGroupType.SUB_LOT_NO.name,
        DataGroupType.SUB_SERIAL.name,
    ]
    df_group = pd.DataFrame(data_group_types, columns=[sys_name])

    df_output = df_output.merge(df_group, how='cross')

    df_output[sys_name] = df_output[sys_name] + '_' + df_output[fact_id_col]

    df_output[primary_groups.DATA_ID] = df_output[sys_name]
    df_output[primary_groups.DATA_NAME] = df_output[sys_name]

    df_output = df_output[unique_cols].drop_duplicates()

    return df_output


@BridgeStationModel.use_db_instance()
def get_distinct_data_by_sub_product_no(df: DataFrame, db_instance: PostgreSQL = None):
    primary_groups = get_primary_group(db_instance=db_instance)
    fact_id_col = MPartType.Columns.part_type_factid.name
    main_cols = [primary_groups.PROCESS_ID, primary_groups.PROCESS_NAME, fact_id_col]
    # order is DaiJi
    data_group_types = [
        DataGroupType.SUB_TRAY_NO,
        DataGroupType.SUB_LOT_NO,
        DataGroupType.SUB_SERIAL,
    ]
    group_types = [group_type.value for group_type in data_group_types]
    data_groups = MDataGroup.get_data_group_in_group_types(db_instance, group_types)
    empty_unit_id = MUnit.get_empty_unit_id(db_instance)

    df[fact_id_col] = df[primary_groups.SUB_PART_NO].str[2:8]
    df_output = df.drop_duplicates(subset=main_cols)[main_cols]
    df_output.set_index(fact_id_col, inplace=True)

    select_cols = [
        fact_id_col,
        MPartType.Columns.part_name_en.name,
        MPartType.Columns.part_name_jp.name,
        MPartType.Columns.part_name_local.name,
    ]
    cols, rows = MPartType.select_records(db_instance, select_cols=select_cols, row_is_dict=False)
    df_part_types = pd.DataFrame(rows, columns=cols, dtype='object')
    df_part_types = format_df(df_part_types)
    df_part_types.set_index(fact_id_col, inplace=True)
    df_output = df_output.join(df_part_types, how='left')
    # drop column if the column all nan
    df_output.dropna(axis=1, how='all', inplace=True)

    group_sys_names = [
        (group.get_sys_name(), group.get_jp_name(), group.get_en_name(), group.get_local_name())
        for group in data_groups
    ]
    df_group = pd.DataFrame(
        group_sys_names,
        columns=[
            MDataGroup.Columns.data_name_sys.name,
            MDataGroup.Columns.data_name_jp.name,
            MDataGroup.Columns.data_name_en.name,
            MDataGroup.Columns.data_name_local.name,
        ],
        dtype='object',
    )
    df_group = format_df(df_group)

    # drop column if the column all nan
    df_group.dropna(axis=1, how='all', inplace=True)

    df_output.reset_index(inplace=True)
    df_output = df_output.merge(df_group, how='cross')

    df_output[MDataGroup.Columns.data_name_sys.name] = (
        df_output[MDataGroup.Columns.data_name_sys.name] + '_' + df_output[fact_id_col]
    )
    df_output = gen_sub_name(df_output, MPartType.Columns.part_name_jp.name, MDataGroup.Columns.data_name_jp.name)

    df_output = gen_sub_name(df_output, MPartType.Columns.part_name_en.name, MDataGroup.Columns.data_name_en.name)

    df_output = gen_sub_name(df_output, MPartType.Columns.part_name_local.name, MDataGroup.Columns.data_name_local.name)

    df_output[MDataGroup.Columns.data_group_type.name] = DataGroupType.GENERATED.value
    df_output[MData.Columns.unit_id.name] = empty_unit_id
    df_output[MappingProcessData.Columns.t_data_id.name] = df_output[MDataGroup.Columns.data_name_sys.name]
    df_output[MappingProcessData.Columns.t_data_name.name] = df_output[MDataGroup.Columns.data_name_sys.name]
    df_output[MDataGroup.Columns.data_name_jp.name] = df_output[MDataGroup.Columns.data_name_sys.name]

    return df_output


def gen_sub_name(df: DataFrame, part_type_col, output_col):
    if part_type_col not in df.columns or output_col not in df.columns:
        for col in (part_type_col, output_col):
            if col in df.columns:
                df.drop(col, axis=1, inplace=True)

        return df

    part_type_series = df[part_type_col]
    data = df[output_col]
    df[output_col] = np.where(part_type_series.isna(), DEFAULT_NONE_VALUE, data + '_' + part_type_series)

    return df
