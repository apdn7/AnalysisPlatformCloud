from __future__ import annotations

from abc import abstractmethod
from typing import Any

from pandas import DataFrame

from ap import log_execution_time
from ap.common.constants import (
    MasterDBType,
    TransactionForPurpose,
)
from bridge.services.etl_services.etl_csv_service import EtlCsvService
from bridge.services.utils import get_master_type_based_on_column_names, get_well_known_columns


class V2MeasureService(EtlCsvService):
    @abstractmethod
    @log_execution_time(prefix='etl_v2_measure_service')
    def convert_to_standard_v2(
        self,
        df: DataFrame,
        for_purpose: TransactionForPurpose = None,
    ) -> DataFrame:
        return self.convert_to_standard_data(df)

    def get_alternative_params(
        self,
        file_path: str,
        read_csv_param: dict[str, Any],
        dic_use_cols: dict,
    ) -> [dict[str, Any], dict[str, Any]]:
        if self.master_type in [MasterDBType.OTHERS.name, MasterDBType.V2.name]:
            # MasterDBType.OTHERS.name for long general data source type
            return super().get_alternative_params(file_path, read_csv_param, dic_use_cols)

        header = self.get_header_row(
            file_path,
            sep=read_csv_param.get('sep', None),
            encoding=read_csv_param.get('encoding', None),
            skip_head=self.get_skip_head(),
        )
        master_type = get_master_type_based_on_column_names(MasterDBType.V2.name, header)
        well_known_columns = get_well_known_columns(master_type, header)

        params = read_csv_param.copy()
        params.update(
            {
                'usecols': lambda c: any(c.startswith(col) for col in well_known_columns),
                'dtype': 'str',
            },
        )

        return params, None
