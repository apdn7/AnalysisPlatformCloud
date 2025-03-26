from typing import List, Union

import numpy as np
import pandas as pd
from pandas.core.dtypes.base import ExtensionDtype

from ap.common.constants import BaseEnum, DataType


class TableColumn(BaseEnum):
    @property
    def data_type(self):
        return self.value[1]

    @classmethod
    def get_column_names(cls):
        return tuple(cls.__members__.keys())

    @classmethod
    def get_column_data_types(cls):
        return [tuple_value.value[1] for tuple_value in list(cls.__members__.values())]

    @classmethod
    def get_column_name_by_data_type(cls, data_types: List[DataType]):
        return [column.name for column in cls if column.value[1] in data_types]

    @classmethod
    def get_column_by_name_like(cls, like_compare_str: str):
        return [column for column in cls if like_compare_str in column.name]

    @classmethod
    def get_dict_column_data_type(cls) -> dict:  # only use for cfg_ and m_
        """
        gen dict column name: datat type
        :return:
        """
        names = cls.get_column_names()
        data_types = cls.get_column_data_types()
        return dict(zip(names, data_types))

    @classmethod
    def get_dict_data_types_pandas(cls) -> dict[str, tuple[DataType, Union[ExtensionDtype, str, np.datetime64]]]:
        """
        gen dict column name: datat type
        :return:
        """
        columns = cls.get_column_names()
        data_types = cls.get_column_data_types()
        pd_data_types = []
        for datatype in data_types:
            if datatype is DataType.INTEGER:
                pd_data_types.append(pd.Int64Dtype())
            elif datatype is DataType.TEXT:
                pd_data_types.append(pd.StringDtype())
            elif datatype is DataType.DATETIME:
                pd_data_types.append(np.datetime64())
            elif datatype is DataType.REAL:
                pd_data_types.append(pd.Float64Dtype())
            elif datatype is DataType.BIG_INT:
                pd_data_types.append(pd.Int64Dtype())
            elif datatype is DataType.BOOLEAN:
                pd_data_types.append(pd.BooleanDtype())

        return dict(zip(columns, list(zip(data_types, pd_data_types))))
