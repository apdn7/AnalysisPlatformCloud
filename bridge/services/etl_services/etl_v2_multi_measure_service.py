import pandas as pd
from pandas import DataFrame

from ap import log_execution_time
from ap.common.constants import (
    DataGroupType,
    TransactionForPurpose,
)
from bridge.services.etl_services.etl_v2_measure_service import V2MeasureService


class V2MultiMeasureService(V2MeasureService):
    @log_execution_time(prefix='etl_v2_multi_measure_service')
    def add_suffix_for_duplicate_data_name(
        self,
        df: DataFrame,
        data_name_like_cols: list[str],
        horizontal_cols: list[str],
    ):
        df_data_name_transposed = df[data_name_like_cols].transpose()
        df_data_name_horizontal_transposed = pd.DataFrame(columns=df_data_name_transposed.columns)
        for horizontal_col in horizontal_cols:
            df_data_name_horizontal_transposed.loc[horizontal_col] = [horizontal_col] * len(
                df_data_name_horizontal_transposed.columns,
            )
        df_data_name_transposed = pd.concat([df_data_name_horizontal_transposed, df_data_name_transposed])
        for col in df_data_name_transposed.columns:
            serial_suffix = (
                df_data_name_transposed[df_data_name_transposed[col].notnull()][[col]]
                .groupby([col])
                .cumcount()
                .apply(lambda a: f'_{a}' if a else pd.NA)
            )
            df_data_name_transposed[col].update(serial_suffix[serial_suffix.notnull()])

        df_data_name_transposed.drop(horizontal_cols, inplace=True)
        df[data_name_like_cols] = df_data_name_transposed.transpose()

    @log_execution_time(prefix='etl_v2_multi_measure_service')
    def convert_to_standard_v2(
        self,
        df: DataFrame,
        for_purpose: TransactionForPurpose = None,
    ) -> DataFrame:
        # Determine group type of all columns
        data_name_col = None
        data_value_col = None
        unique_cols = []

        for data_table_column in self.cfg_data_table_columns:
            if data_table_column.column_name not in df.columns:
                continue
            if data_table_column.data_group_type == DataGroupType.DATA_NAME.value:
                data_name_col = data_table_column.column_name
            elif data_table_column.data_group_type == DataGroupType.DATA_VALUE.value:
                data_value_col = data_table_column.column_name
            elif data_table_column.data_group_type == DataGroupType.HORIZONTAL_DATA.value:
                continue
            else:
                unique_cols.append(data_table_column.column_name)

        data_name_like_cols = [col for col in df.columns if col.startswith(data_name_col)]
        data_value_like_cols = [col for col in df.columns if col.startswith(data_value_col)]
        dfs: list[DataFrame] = [
            pd.DataFrame(columns=unique_cols + [data_name_col, data_value_col]),
            self.convert_df_horizontal_to_vertical(df),
        ]

        # TODO: Add suffix for duplicate data names if exist - IMPROVE PERFORMANCE
        # self.add_suffix_for_duplicate_data_name(df, data_name_like_cols, horizontal_cols)

        # Convert multi columns to Vertical data type
        for name_col, value_col in zip(data_name_like_cols, data_value_like_cols):
            _df = df.dropna(subset=[name_col])[[*unique_cols, name_col, value_col]].rename(
                columns={name_col: data_name_col, value_col: data_value_col},
            )

            if not _df.empty:
                dfs.append(_df)

        return pd.concat(dfs).reset_index(drop=True)
