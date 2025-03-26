from pandas import DataFrame

from ap import log_execution_time
from ap.common.constants import DataGroupType, DataType
from ap.common.pydn.dblib.postgresql import PostgreSQL
from ap.setting_module.models import MProd, MProdFamily
from bridge.models.m_data import MData
from bridge.models.m_data_group import MDataGroup


class ExtendSensorInfo:
    class _SensorInfo:
        def __init__(self, **kwargs):
            for key, value in kwargs.items():
                self.__setattr__(key, value)

        def get_id(self):
            return self.__getattribute__('id')

        def get_value(self):
            return self.__getattribute__('value')

        def get_name(self):
            return self.__getattribute__('name')

        def is_equal(self, _id):
            return self.__getattribute__('id') == _id

        def get_data_type(self):
            return self.__getattribute__('data_type')

        def get_data_group_id(self, db_instance: PostgreSQL):
            df = MDataGroup.get_all_as_df(db_instance)
            target_item_df = df[df[MDataGroup.Columns.data_name_sys.name] == self.get_name()]
            if not target_item_df.empty:
                return target_item_df.iloc[0][MData.Columns.data_group_id.name]

            return None

    ProdName = _SensorInfo(id='__PROD_NAME__', value='製品名', name='SeihinMei', data_type=DataType.TEXT.value)


@log_execution_time()
def add_extend_sensor_column(
    df: DataFrame,
    data_name_col=None,
    data_id_col=None,
    data_value_col=None,
    m_prod_df=None,
    m_prod_family_df=None,
):
    from bridge.services.master_data_efa_v2_transform_pattern import (
        dict_config_efa_v2_transform_column,  # prevent a circular import
    )

    # Extract LINE_NAME to prod_name
    to_cols = dict_config_efa_v2_transform_column[DataGroupType.LINE_NAME.name]
    ignore_cols = list(set(to_cols) - {MProd.prod_name_jp.name})
    extend_args = {}
    if m_prod_df is None:
        m_prod_df = MProd.get_all_as_df()
    extend_args['m_prod_df'] = m_prod_df

    if m_prod_family_df is None:
        m_prod_family_df = MProdFamily.get_all_as_df()
    extend_args['m_prod_family_df'] = m_prod_family_df
    extend_args['is_get_prod_abbr_jp'] = True

    _df = df[[DataGroupType.LINE_NAME.name]].drop_duplicates()
    from bridge.services.master_data_import import transform_column

    transform_column(_df, DataGroupType.LINE_NAME.name, to_cols, ignore_cols, **extend_args)

    # Add it to data name & data value
    df = df.merge(_df, on=[DataGroupType.LINE_NAME.name])
    data_name_col = data_name_col if data_name_col else DataGroupType.DATA_NAME.name
    data_id_col = data_id_col if data_id_col else DataGroupType.DATA_ID.name
    data_value_col = data_value_col if data_value_col else DataGroupType.DATA_VALUE.name

    group_cols = set(df.columns.tolist()) - {data_id_col, data_name_col, data_value_col}
    temp_df = df[group_cols].drop_duplicates(subset=group_cols)
    temp_df.rename(columns={MProd.prod_name_jp.name: data_value_col}, inplace=True)
    temp_df[data_id_col] = ExtendSensorInfo.ProdName.get_id()
    temp_df[data_name_col] = ExtendSensorInfo.ProdName.get_value()
    df.drop(columns=[MProd.prod_name_jp.name], inplace=True)
    return df.append(temp_df[df.columns.tolist()], ignore_index=True), m_prod_df, m_prod_family_df
