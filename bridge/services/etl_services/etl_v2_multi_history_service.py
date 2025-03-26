import re
from typing import Iterable, Iterator, Optional, Tuple, Type, Union

import numpy as np
import pandas as pd
from pandas import DataFrame, Series

from ap import log_execution_time
from ap.common.constants import (
    EMPTY_STRING,
    BaseEnum,
    DataGroupType,
    JobType,
    RawDataTypeDB,
    TransactionForPurpose,
)
from ap.common.pydn.dblib.postgresql import PostgreSQL
from bridge.models.bridge_station import BridgeStationModel
from bridge.models.m_data import MData
from bridge.models.mapping_factory_machine import MappingFactoryMachine
from bridge.models.mapping_part import MappingPart
from bridge.models.mapping_process_data import MappingProcessData
from bridge.services.etl_services.etl_v2_history_service import V2HistoryService


class V2MultiHistoryService(V2HistoryService):
    class Regexes(BaseEnum):
        VALUE_SUB_PART_NO = r'JP\d{10}$'

        NAME_SYS_SUB_PART_NO = r'^Ko(\d+)[Ss]hinaban$'
        NAME_SYS_SUB_LOT = r'^Ko(\d+)[Ll]ot$'
        NAME_SYS_SUB_TRAY = r'^Ko(\d+)[Tt]ray$'
        NAME_SYS_SUB_SERIAL = r'^Ko(\d+)[Ss]erial$'

        NEW_NAME_SYS_SUB_PART_NO = r'Sub\1Part'
        NEW_NAME_SYS_SUB_LOT = r'Sub\1Lot'
        NEW_NAME_SYS_SUB_TRAY = r'Sub\1Tray'
        NEW_NAME_SYS_SUB_SERIAL = r'Sub\1Serial'

    __SUB_PART_NO_COLUMN_NAME__ = '子{}品番'
    __SUB_LOT_COLUMN_NAME__ = '子{}ロット'
    __SUB_TRAY_COLUMN_NAME__ = '子{}トレイ'
    __SUB_SERIAL_COLUMN_NAME__ = '子{}シリアル'

    @log_execution_time(prefix='etl_v2_multi_history_service')
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
        ] = self.get_transaction_data(for_purpose=TransactionForPurpose.FOR_SCAN_MASTER, db_instance=db_instance)
        yield from self.split_master_data(generator_df, is_rename=False)

    @log_execution_time()
    def split_master_data(
        self,
        generator_df: Iterable[Tuple[Optional[DataFrame], Optional[list], Union[int, float]]],
        is_rename: bool = True,
    ) -> Iterator[
        Tuple[
            dict[Type[Union[MappingProcessData, MappingPart, MappingFactoryMachine, str]], DataFrame],
            Union[int, float],
        ]
    ]:
        """
        Separate big dataframe to 3 parts of mapping master data to be served for scan master process
        :param generator_df: an Iterable of raw collecting data
        :param is_rename: a flag that want to rename columns to bridge name or not
        :return: an Iterable that contains 3 parts of mapping master data
        """
        for df_origin, target_files, progress_percentage in generator_df:
            df = df_origin.copy()  # avoid input origin df changes when determine new master data in pull data job

            # Convert sub part no to part no
            if self.sub_part_no_column in df:
                part_no_column = self.get_column_name(DataGroupType.PART_NO.value)[0].get('source_name')
                sub_df_cols = df.columns.to_list()
                sub_df_cols.remove(part_no_column)
                sub_part_df = (
                    df[sub_df_cols]
                    .rename(columns={self.sub_part_no_column: part_no_column})
                    .dropna(subset=[part_no_column])
                )
                df = (
                    pd.concat([df, sub_part_df])
                    .drop(columns=[self.sub_part_no_column])
                    .drop_duplicates()
                    .reset_index(drop=True)
                )

            yield from super().split_master_data([(df, target_files, progress_percentage)])

    @log_execution_time(prefix='etl_v2_multi_history_service')
    @BridgeStationModel.use_db_instance_generator()
    def get_data_for_data_type(
        self,
        generator_df: iter = None,
        db_instance: PostgreSQL = None,
    ) -> [DataFrame, dict, float]:
        if generator_df is None:
            generator_df = self.get_transaction_data(
                for_purpose=TransactionForPurpose.FOR_SCAN_DATA_TYPE,
                db_instance=db_instance,
            )
        for (
            _df,
            target_files,
            progress_percentage,
        ) in generator_df:  # type: (DataFrame, list[str], float)
            df = _df
            if df is None or len(df) == 0:
                continue

            ignore_cols = None
            self.drop_all_sub_columns(df)

            df, dic_df_horizons = self.transform_horizon_columns_for_import(
                df,
                ignore_cols=ignore_cols,
            )
            yield df, {}, progress_percentage

    @log_execution_time(prefix='etl_v2_multi_history_service')
    @BridgeStationModel.use_db_instance_generator()
    def get_transaction_data(
        self,
        for_purpose: TransactionForPurpose = None,
        job_type: JobType = None,
        db_instance: PostgreSQL = None,
    ) -> Iterator[Tuple[Optional[DataFrame], Optional[list], Union[int, float]]]:
        generator_df: Iterator[
            Tuple[Optional[DataFrame], Optional[list], Union[int, float]]
        ] = super().get_transaction_data(for_purpose=for_purpose, db_instance=db_instance)
        if for_purpose in [
            TransactionForPurpose.FOR_SCAN_MASTER,
            TransactionForPurpose.FOR_SCAN_DATA_TYPE,
        ]:
            for df, target_files, progress_percentage in generator_df:
                if df is None or df.empty:
                    continue

                self.drop_all_sub_columns(df)
                yield df, target_files, progress_percentage
        else:
            yield from generator_df

    @log_execution_time(prefix='etl_v2_multi_history_service')
    def convert_to_standard_v2(
        self,
        df: DataFrame,
        for_purpose: TransactionForPurpose = None,
    ) -> DataFrame:
        sub_part_no_col: str = EMPTY_STRING
        sub_lot_no_col: str = EMPTY_STRING
        sub_tray_no_col: str = EMPTY_STRING
        sub_serial_no_col: str = EMPTY_STRING
        unique_cols: list[str] = []
        horizontal_cols = []

        for data_table_column in self.cfg_data_table_columns:
            if data_table_column.column_name not in df.columns:
                continue
            if data_table_column.data_group_type == DataGroupType.SUB_PART_NO.value:
                sub_part_no_col = data_table_column.column_name
            elif data_table_column.data_group_type == DataGroupType.SUB_LOT_NO.value:
                sub_lot_no_col = data_table_column.column_name
            elif data_table_column.data_group_type == DataGroupType.SUB_TRAY_NO.value:
                sub_tray_no_col = data_table_column.column_name
            elif data_table_column.data_group_type == DataGroupType.SUB_SERIAL.value:
                sub_serial_no_col = data_table_column.column_name
            elif data_table_column.data_group_type == DataGroupType.HORIZONTAL_DATA.value:
                horizontal_cols.append(data_table_column.column_name)
            else:
                unique_cols.append(data_table_column.column_name)

        sub_part_no_like_cols = [col for col in df.columns if col.startswith(sub_part_no_col)]
        sub_lot_no_like_cols = [col for col in df.columns if col.startswith(sub_lot_no_col)]
        sub_tray_no_like_cols = [col for col in df.columns if col.startswith(sub_tray_no_col)]
        sub_serial_no_like_cols = [col for col in df.columns if col.startswith(sub_serial_no_col)]

        dfs = [self.convert_df_horizontal_to_vertical(df)]
        df.drop(columns=horizontal_cols, inplace=True)

        if for_purpose is TransactionForPurpose.FOR_SCAN_MASTER:
            sub_part_dfs = []
            for sub_part_no_col in sub_part_no_like_cols:
                sub_part_df = df.dropna(subset=[sub_part_no_col])
                sub_part_df[self.part_no_column] = sub_part_df[sub_part_no_col]
                sub_part_dfs.append(sub_part_df)

            df = df.append(pd.concat(sub_part_dfs), ignore_index=True)

        # Convert multi columns to Vertical data type
        for idx, (part_col, lot_col, tray_col, serial_col) in enumerate(
            zip(
                sub_part_no_like_cols,
                sub_lot_no_like_cols,
                sub_tray_no_like_cols,
                sub_serial_no_like_cols,
            ),
        ):
            master_df = df.dropna(subset=[part_col])[unique_cols]
            group_sub_number = idx + 1
            for name_col, value_col in zip(
                [
                    self.__SUB_PART_NO_COLUMN_NAME__,
                    self.__SUB_LOT_COLUMN_NAME__,
                    self.__SUB_TRAY_COLUMN_NAME__,
                    self.__SUB_SERIAL_COLUMN_NAME__,
                ],
                [part_col, lot_col, tray_col, serial_col],
            ):
                _df = master_df.copy()
                _df[DataGroupType.DATA_NAME.name] = name_col.format(group_sub_number)
                data_series = df.loc[master_df.index][value_col]
                if value_col == part_col:
                    # _df[sub_part_no_col] = data_series
                    data_series = self.transform_part_no(data_series)
                _df[DataGroupType.DATA_VALUE.name] = data_series

                dfs.append(_df)

        return pd.concat(dfs).reset_index(drop=True)

    @classmethod
    @log_execution_time()
    def transform_part_no(cls, series: Series) -> Series:
        """
        transform part-no value to import data
        input: JP1234567890
        output: 7890
        """
        if series.empty:
            return series

        r = re.compile(cls.Regexes.VALUE_SUB_PART_NO.value)
        transformed_series = np.vectorize(lambda x: x[-4:] if isinstance(x, str) and bool(r.match(x)) else x)(series)

        return transformed_series

    @classmethod
    def rename_column_name(cls, series_col_name):
        """
        rename column if v2 multi history
        Ko1Lot → Sub1Lot
        Ko1Shinaban → Sub1Part
        """
        patterns = [
            # Pattern for "Ko{number}Shinaban" -> "Sub{number}Part"
            (cls.Regexes.NAME_SYS_SUB_PART_NO.value, cls.Regexes.NEW_NAME_SYS_SUB_PART_NO.value),
            # Pattern for "Ko{number}Lot" -> "Sub{number}Lot"
            (cls.Regexes.NAME_SYS_SUB_LOT.value, cls.Regexes.NEW_NAME_SYS_SUB_LOT.value),
            # Pattern for "Ko{number}Tray" -> "Sub{number}Tray"
            (cls.Regexes.NAME_SYS_SUB_TRAY.value, cls.Regexes.NEW_NAME_SYS_SUB_TRAY.value),
            # Pattern for "Ko{number}Serial" -> "Sub{number}Serial"
            (cls.Regexes.NAME_SYS_SUB_SERIAL.value, cls.Regexes.NEW_NAME_SYS_SUB_SERIAL.value),
        ]
        for pattern, replacement in patterns:
            match = re.match(pattern, series_col_name)
            if match:
                new_part = re.sub(pattern, replacement, series_col_name)  # Perform the substitution
                return new_part
        return series_col_name  # Return the original string if it doesn't match any pattern

    @classmethod
    def force_type_for_sub_part_no_columns(cls, df: DataFrame, df_m_data: DataFrame):
        # force small int data type for all sub part no columns in V2 Multi History datasource
        sub_part_no_data_ids = df_m_data[
            df_m_data.data_name_sys.str.match(cls.Regexes.NAME_SYS_SUB_PART_NO.value)
        ].data_id.to_list()
        df.loc[df.data_id.isin(sub_part_no_data_ids), MData.Columns.data_type.name] = RawDataTypeDB.SMALL_INT.value
