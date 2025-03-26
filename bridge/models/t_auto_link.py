from __future__ import annotations

from typing import Any, Optional, Union

import numpy as np
import pandas as pd
from pandas import DataFrame

from ap.common.common_utils import convert_nan_to_none
from ap.common.constants import DataType
from ap.common.pydn.dblib.postgresql import PostgreSQL
from ap.setting_module.models import AutoLink as ESAutoLink
from bridge.models.bridge_station import BridgeStationModel, OthersDBModel
from bridge.models.model_utils import TableColumn
from bridge.services.sql.utils import df_from_query


class AutoLink(OthersDBModel):
    class Columns(TableColumn):
        id = (1, DataType.INTEGER)
        process_id = (2, DataType.INTEGER)
        serial = (3, DataType.TEXT)
        date_time = (4, DataType.DATETIME)

    _table_name = 't_auto_link'

    def __init__(self, dict_proc: Optional[dict[str, Any]]):
        if not dict_proc:
            dict_proc = {}
        self.id = dict_proc.get(AutoLink.Columns.id.name)
        self.process_id = dict_proc.get(AutoLink.Columns.process_id.name)
        self.serial = dict_proc.get(AutoLink.Columns.serial.name)
        self.date_time = dict_proc.get(AutoLink.Columns.date_time.name)

    @classmethod
    @BridgeStationModel.use_db_instance()
    def update_data(cls, df: DataFrame, db_instance: PostgreSQL = None):
        process_ids = df[cls.Columns.process_id.name].unique()
        exist_process_ids = []

        # TODO: check exist records and drop duplicate data before inserting
        for proc_id in process_ids:  # type: np.int
            process_id = proc_id.item()  # convert np.int64 to python int
            df_exist: DataFrame = AutoLink.get_by_process_id(process_id, return_df=True, db_instance=db_instance)
            if not df_exist.empty:
                exist_process_ids.append(process_id)

        df_insert = df[~df[cls.Columns.process_id.name].isin(exist_process_ids)]
        if not df_insert.empty:
            rows = convert_nan_to_none(df_insert, convert_to_list=True)
            db_instance.bulk_insert(cls.get_table_name(), df_insert.columns, rows)

    @classmethod
    @BridgeStationModel.use_db_instance()
    def get_by_process_id(
        cls,
        process_id: int,
        return_df: bool = False,
        db_instance: PostgreSQL = None,
        limit: int | None = None,
    ) -> Union[DataFrame, list[dict]]:
        dic_conditions = {
            cls.Columns.process_id.name: process_id,
        }

        _, rows = cls.select_records(db_instance, dic_conditions=dic_conditions, limit=limit)

        if return_df:
            df = (
                pd.DataFrame(rows, dtype=np.object)
                if rows
                else pd.DataFrame(columns=cls.Columns.get_column_names(), dtype=np.object)
            )

            dict_data_type = cls.Columns.get_dict_data_types_pandas()
            for column, (data_type, pd_data_type) in dict_data_type.items():
                df[column] = df[column].astype(pd_data_type)

            return df

        return rows

    @classmethod
    def get_by_process_id_to_df(cls, db_instance: PostgreSQL, *, process_id: int, limit: int | None) -> pd.DataFrame:
        query = ESAutoLink.query.filter(ESAutoLink.process_id == process_id)
        if limit is not None:
            query = query.limit(limit)
        return df_from_query(query=query, db_instance=db_instance)
