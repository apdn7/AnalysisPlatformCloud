from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, Generator, List, Optional, Set

import numpy as np
import pandas as pd
from pandas import DataFrame, Series

from ap.common.logger import log_execution_time

AUTO_LINK_ID = 'id'
PROCESS = 'process'
DATE = 'date'
SERIAL = 'serial'
COUNT = 'count'
ORDER = 'order'
REVERSED_ORDER = 'reversed_order'
SCORE = 'score'

LOG_PREFIX = 'AUTOLINK_'

AUTOLINK_TOTAL_RECORDS_PER_SOURCE = 100000


class SortMethod(Enum):
    CountOrderKeepMax = 1
    CountOrderKeepMean = 2
    CountOrderKeepAll = 3
    CountReversedOrder = 4
    FunctionCountReversedOrder = 5


@log_execution_time(LOG_PREFIX)
def get_processes_id_order(
    list_params: List[Dict[str, Any]],
    method: SortMethod = SortMethod.CountOrderKeepMax,
) -> List[List[int]]:
    from bridge.services.pull_for_auto_link import PullForAutoLink

    auto_link_data = AutoLinkData.from_params(list_params)
    pull_for_auto_link = PullForAutoLink(auto_link_data)
    pull_for_auto_link.get_data_from_local()
    pull_for_auto_link.get_data_from_source()

    auto_link_df = pull_for_auto_link.auto_link_data.auto_link_df
    return AutoLinkAlgo.get_auto_link_group(auto_link_df, method)


class AutoLinkDataProcess:
    DATA_TYPES: dict[str, pd.ExtensionDtype] = {
        SERIAL: pd.StringDtype(),
        DATE: np.datetime64(),
        AUTO_LINK_ID: pd.Int64Dtype(),
    }

    process_id: int
    df: pd.DataFrame

    def __init__(self, process_id: int, df: pd.DataFrame | None = None):
        self.process_id = process_id
        if df is None:
            self.df = pd.DataFrame(columns=[SERIAL, DATE]).astype(self.filter_dict_key_type([SERIAL, DATE]))
        else:
            self.df = self.validate_df(df)

    @classmethod
    def filter_dict_key_type(cls, columns: list[str] | pd.Index) -> dict[str, pd.ExtensionDtype]:
        return {key: cls.DATA_TYPES[key] for key in columns if key in cls.DATA_TYPES}

    @classmethod
    def validate_df(cls, df: pd.DataFrame) -> pd.DataFrame:
        if SERIAL not in df:
            raise ValueError('No auto link serial')
        if DATE not in df:
            raise ValueError('No auto link date time')
        return pd.DataFrame(
            {
                SERIAL: df[SERIAL],
                DATE: pd.to_datetime(df[DATE]).dt.tz_localize(None),
            },
        ).astype(
            cls.filter_dict_key_type([SERIAL, DATE]),
        )

    def update(self, df: pd.DataFrame) -> 'AutoLinkDataProcess':
        concatenated_df = pd.concat([self.df, self.validate_df(df)], ignore_index=True)
        updated_df = AutoLinkData.drop_duplicates(concatenated_df)
        return AutoLinkDataProcess(process_id=self.process_id, df=updated_df)

    @property
    def auto_link_df(self) -> pd.DataFrame:
        df = self.df.copy()
        df[AUTO_LINK_ID] = self.process_id
        return df.astype(self.filter_dict_key_type(df.columns))

    @property
    def total_records(self) -> int:
        return len(self.df)

    @property
    def records_needed(self) -> int:
        return max(0, AUTOLINK_TOTAL_RECORDS_PER_SOURCE - self.total_records)

    @property
    def enough_data(self) -> bool:
        return self.records_needed == 0


@dataclass
class AutoLinkData:
    """
    Class contains auto link data for multiple processes
    """

    data: dict[int, AutoLinkDataProcess]

    @classmethod
    def from_processes(cls, process_ids: list[int] | set[int]) -> 'AutoLinkData':
        return cls(data={process_id: AutoLinkDataProcess(process_id=process_id) for process_id in process_ids})

    @classmethod
    def from_params(cls: 'AutoLinkData', params: list[Dict[str, Any]]) -> 'AutoLinkData':
        # TODO: create mashmallow for params later
        process_ids = {process_id for param in params for process_id in param['ids']}
        return cls.from_processes(process_ids)

    @property
    def data_process_for_update(self) -> Generator[AutoLinkDataProcess, None, None]:
        """Yield all auto link data which is not done"""
        for process_data in self.data.values():
            if process_data.enough_data:
                continue
            yield process_data

    def get(self, process_id: int) -> AutoLinkDataProcess:
        if not self.has_process(process_id):
            raise KeyError(f'No such process: {process_id}')
        return self.data[process_id]

    def has_process(self, process_id: int) -> bool:
        return process_id in self.data

    def enough_data_for_process(self, process_id: int) -> bool:
        return self.get(process_id).enough_data

    def update_per_process(self, process_id: int, process_df: pd.DataFrame):
        if process_df.empty:
            return
        if not self.has_process(process_id):
            return
        if not self.enough_data_for_process(process_id):
            self.data[process_id] = self.data[process_id].update(process_df)

    def update(self, df: pd.DataFrame) -> None:
        assert AUTO_LINK_ID in df.columns
        assert DATE in df.columns
        assert SERIAL in df.columns
        for process_id, process_df in df.groupby(AUTO_LINK_ID):
            assert isinstance(process_id, int)
            self.update_per_process(process_id, process_df)

    @property
    def auto_link_df(self) -> pd.DataFrame:
        return pd.concat(
            (auto_link_data.auto_link_df for auto_link_data in self.data.values()),
            ignore_index=True,
        )

    @staticmethod
    def drop_duplicates(df: DataFrame) -> DataFrame:
        """
        extract the latest record from same process and same serial
        to remove duplicate record from each process.
        """
        subset = []
        if PROCESS in df.columns:
            subset.append(PROCESS)
        if SERIAL in df.columns:
            subset.append(SERIAL)
        if AUTO_LINK_ID in df.columns:
            subset.append(AUTO_LINK_ID)
        return df.sort_values(DATE).drop_duplicates(subset=subset, keep='last').reset_index(drop=True)


class SortAlgo:
    def sorted_processes(self, df: DataFrame) -> List[str]:
        raise NotImplementedError

    @staticmethod
    def verify(df: DataFrame) -> bool:
        return {AUTO_LINK_ID, DATE, SERIAL} == set(df.columns)

    def get_count_by_serial(self, df: DataFrame) -> DataFrame:
        if not self.verify(df):
            raise NotImplementedError(f'df contains unexpected columns: {df.columns}')
        df = df.reset_index()
        df[COUNT] = df[[DATE, SERIAL]].groupby(SERIAL).transform('count')
        return df


class SortByCountOrderKeep(SortAlgo):
    def _sorted_processes(self, df: DataFrame, loop_count: Optional[int] = None) -> List[int]:
        """
        if loop_count = 1, we just loop 1 time and emit found ordered processes
        if loop_count = None, we loop until we found all processes in dataframe
        """
        df = self.get_count_by_serial(df)

        loop = 0
        ordered_processes = []
        while not df.empty:
            # extract subset which has the biggest count.
            max_count = df[COUNT].max()
            df_count = df[df[COUNT] == max_count]
            df_count[ORDER] = (
                df_count.sort_values([DATE, AUTO_LINK_ID], ascending=[True, False]).groupby(SERIAL).cumcount() + 1
            )

            # calculate mean of order against each process
            agg_params = {ORDER: 'mean', DATE: 'min'}
            current_ordered = (
                df_count[[AUTO_LINK_ID, DATE, ORDER]]
                .groupby(AUTO_LINK_ID)
                .agg(agg_params)
                .sort_values([ORDER, DATE, AUTO_LINK_ID], ascending=[True, True, True])
                .index.to_list()
            )
            ordered_processes.extend(current_ordered)

            loop += 1
            if loop_count is not None and loop >= loop_count:
                break
            # remove ordered processes
            df = df[~df[AUTO_LINK_ID].isin(current_ordered)]

        return ordered_processes


class SortByCountOrderKeepMax(SortByCountOrderKeep):
    @log_execution_time(LOG_PREFIX)
    def sorted_processes(self, df: DataFrame) -> List[int]:
        return self._sorted_processes(df, loop_count=1)


class SortByCountOrderKeepAll(SortByCountOrderKeep):
    @log_execution_time(LOG_PREFIX)
    def sorted_processes(self, df: DataFrame) -> List[int]:
        return self._sorted_processes(df, loop_count=None)


class SortByCountOrderKeepMean(SortAlgo):
    @log_execution_time(LOG_PREFIX)
    def sorted_processes(self, df: DataFrame) -> List[str]:
        df = self.get_count_by_serial(df)
        mean = df[COUNT].mean()
        df = df[df[COUNT] >= mean]
        df[ORDER] = df.sort_values([DATE, AUTO_LINK_ID], ascending=[True, False]).groupby(SERIAL).cumcount() + 1

        # calculate mean of order against each process
        agg_params = {ORDER: 'mean', DATE: 'min'}
        return (
            df[[AUTO_LINK_ID, DATE, ORDER]]
            .groupby(AUTO_LINK_ID)
            .agg(agg_params)
            .sort_values([ORDER, DATE, AUTO_LINK_ID], ascending=[True, True, True])
            .index.to_list()
        )


@dataclass
class SortByFunctionCountReversedOrder(SortAlgo):
    kind: str = 'ident'

    def function_count(self, s: Series) -> Series:
        if self.kind == 'ident':
            return s
        if self.kind == 'square':
            return s * s
        if self.kind == 'cube':
            return s * s * s
        if self.kind == 'power_of_two':
            return s.pow(2)

    def _sorted_processes(self, df: DataFrame) -> List[str]:
        df = self.get_count_by_serial(df)
        df[REVERSED_ORDER] = (
            df.sort_values([DATE, AUTO_LINK_ID], ascending=[False, False]).groupby(SERIAL).cumcount() + 1
        )
        df[SCORE] = df[REVERSED_ORDER] * self.function_count(df[COUNT])

        # TODO: should we calculate score by sum or mean?
        agg_params = {SCORE: 'sum', DATE: 'min'}
        return (
            df[[AUTO_LINK_ID, DATE, SCORE]]
            .groupby(AUTO_LINK_ID)
            .agg(agg_params)
            .sort_values([SCORE, DATE, AUTO_LINK_ID], ascending=[False, True, True])
            .index.to_list()
        )

    @log_execution_time(LOG_PREFIX)
    def sorted_processes(self, df: DataFrame) -> List[str]:
        return self._sorted_processes(df)


class SortByCountReversedOrder(SortByFunctionCountReversedOrder):
    @log_execution_time(LOG_PREFIX)
    def sorted_processes(self, df: DataFrame) -> List[str]:
        return super()._sorted_processes(df)


class AutoLinkAlgo:
    @staticmethod
    @log_execution_time(LOG_PREFIX)
    def group(df: pd.DataFrame, ordered_processes: List[int]) -> List[List[int]]:
        """Same with above grouping method, but we loop all over group"""
        unique_serials: Dict[int, Set] = {}
        for process in ordered_processes:
            unique_serials[process] = set(
                df[df[AUTO_LINK_ID] == process][SERIAL],
            )
        groups = []
        for process in ordered_processes:
            if not groups:
                groups.append([process])
                continue
            index = 0
            inserted = False
            while any(index < len(group) for group in groups):
                for group in groups:
                    if len(group) <= index:
                        continue
                    if not unique_serials[group[index]].isdisjoint(unique_serials[process]):
                        group.append(process)
                        inserted = True
                        break
                if inserted:
                    break
                index += 1
            if not inserted:
                groups.append([process])
        return groups

    @staticmethod
    @log_execution_time(LOG_PREFIX)
    def sort(
        df: DataFrame,
        method: SortMethod = SortMethod.CountOrderKeepMax,
    ) -> List[int]:
        if method is SortMethod.CountOrderKeepMax:
            sort_algo = SortByCountOrderKeepMax()
        elif method is SortMethod.CountOrderKeepMean:
            sort_algo = SortByCountOrderKeepMean()
        elif method is SortMethod.CountOrderKeepAll:
            sort_algo = SortByCountOrderKeepAll()
        elif method is SortMethod.CountReversedOrder:
            sort_algo = SortByCountReversedOrder()
        elif method is SortMethod.FunctionCountReversedOrder:
            sort_algo = SortByFunctionCountReversedOrder(kind='cube')
        else:
            raise NotImplementedError(f'{method.name} method is not supported')
        return sort_algo.sorted_processes(df)

    @staticmethod
    @log_execution_time(LOG_PREFIX)
    def get_auto_link_group(
        df: DataFrame,
        sort_method: SortMethod = SortMethod.CountOrderKeepMax,
    ) -> list[list[int]]:
        ordered_processes = AutoLinkAlgo.sort(df, sort_method)
        ordered_groups = AutoLinkAlgo.group(df, ordered_processes)
        return ordered_groups
