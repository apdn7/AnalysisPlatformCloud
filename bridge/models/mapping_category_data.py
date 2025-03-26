from typing import Union

import pandas as pd
from pandas import Series
from sqlalchemy.orm import scoped_session

from ap.common.constants import RawDataTypeDB
from ap.common.pydn.dblib.mssqlserver import MSSQLServer
from ap.common.pydn.dblib.mysql import MySQL
from ap.common.pydn.dblib.oracle import Oracle
from ap.common.pydn.dblib.postgresql import PostgreSQL
from ap.common.pydn.dblib.sqlite import SQLite3
from bridge.models.bridge_station import BridgeStationModel
from bridge.models.mapping_column import gen_default_m_group
from bridge.models.model_utils import TableColumn
from bridge.models.semi_master import SemiMaster


class MappingCategoryData(BridgeStationModel):
    class Columns(TableColumn):
        id = (1, RawDataTypeDB.INTEGER)
        factor = (2, RawDataTypeDB.SMALL_INT)
        t_category_data = (3, RawDataTypeDB.TEXT)
        group_id = (4, RawDataTypeDB.INTEGER)
        data_id = (5, RawDataTypeDB.INTEGER)
        data_table_id = (6, RawDataTypeDB.INTEGER)
        created_at = (7, RawDataTypeDB.DATETIME)
        updated_at = (8, RawDataTypeDB.DATETIME)

    _table_name = 'mapping_category_data'
    primary_keys = []
    partition_columns = []

    def __init__(self, dict_proc=None):
        dict_proc = dict_proc if dict_proc else {}
        self.id = dict_proc.get(MappingCategoryData.Columns.id.name)
        self.factor = dict_proc.get(MappingCategoryData.Columns.factor.name)
        self.t_category_data = dict_proc.get(MappingCategoryData.Columns.t_category_data.name)
        self.group_id = dict_proc.get(MappingCategoryData.Columns.group_id.name)
        self.data_id = dict_proc.get(MappingCategoryData.Columns.data_id.name)
        self.data_table_id = dict_proc.get(MappingCategoryData.Columns.data_table_id.name)
        self.created_at = dict_proc.get(MappingCategoryData.Columns.created_at.name)
        self.updated_at = dict_proc.get(MappingCategoryData.Columns.updated_at.name)

    @classmethod
    def get_by_factor(cls, db_instance, factor):
        _, rows = cls.select_records(
            db_instance,
            dic_conditions={cls.Columns.factor.name: factor},
            select_cols=[
                cls.Columns.id.name,
                cls.Columns.factor.name,
                cls.Columns.t_category_data.name,
                cls.Columns.group_id.name,
                cls.Columns.data_id.name,
                cls.Columns.data_table_id.name,
                cls.Columns.created_at.name,
                cls.Columns.updated_at.name,
            ],
        )
        return rows

    @classmethod
    def reset_all_factor_n_group_id(
        cls,
        db_instance: Union[SQLite3, PostgreSQL, Oracle, MySQL, MSSQLServer, scoped_session],
    ):
        sql_statement = (
            f'UPDATE {cls.get_table_name()} '
            f'SET {cls.Columns.factor.name}=NULL, '
            f'    {cls.Columns.group_id.name}=NULL '
            f'WHERE 1=1;'
        )
        if isinstance(db_instance, scoped_session):
            db_instance.execute(sql_statement)
        else:
            db_instance.execute_sql(sql_statement)


def ungroup_category_values(
    db_instance: Union[SQLite3, PostgreSQL, Oracle, MySQL, MSSQLServer, scoped_session],
    group_id: int,
    mapping_category_data_dict: dict[str, list[int]],
):
    _, rows = SemiMaster.select_records(
        db_instance,
        dic_conditions={SemiMaster.Columns.group_id.name: group_id},
        select_cols=[SemiMaster.Columns.factor.name, SemiMaster.Columns.value.name],
    )
    df_semi = pd.DataFrame(rows)
    for idx, exist_semi_group in df_semi.iterrows():
        rows = MappingCategoryData.get_by_factor(db_instance, exist_semi_group.factor)
        df_mapping = pd.DataFrame(rows)
        dic_update_values = {
            MappingCategoryData.Columns.factor.name: None,
            MappingCategoryData.Columns.group_id.name: None,
        }
        new_presented_ids = mapping_category_data_dict.get(exist_semi_group.value, [])
        if new_presented_ids:  # In case still update present value
            ungroup_ids = df_mapping[~df_mapping.id.isin(new_presented_ids)].id.to_list()

        else:  # In case present value is removed on GUI
            ungroup_ids = df_mapping.id.to_list()
            SemiMaster.delete_by_condition(
                db_instance,
                dic_conditions={
                    SemiMaster.Columns.factor.name: exist_semi_group.factor,
                    SemiMaster.Columns.group_id.name: group_id,
                },
                mode=0,
            )
        if ungroup_ids:
            MappingCategoryData.bulk_update_by_ids(db_instance, ungroup_ids, dic_update_values)


def gen_factor_n_group_id(
    db_instance: Union[SQLite3, PostgreSQL, Oracle, MySQL, MSSQLServer, scoped_session],
    group_id,
    dict_mapping_category_data,
):
    """
    :param db_instance: db_instance
    :param group_id: 1
    :param dict_mapping_category_data: {gia tri dai dien: [data_id,data_id], 'OK': [3,4], 'NG': [1,2]}
    :return:
    """

    series = pd.Series(list(dict_mapping_category_data.keys()))
    dic_factor = gen_semi_master_data_by_type(db_instance, group_id, series)
    for key, ids in dict_mapping_category_data.items():
        factor_id = dic_factor.get(key)
        dic_update_values = {
            MappingCategoryData.Columns.factor.name: factor_id,
            MappingCategoryData.Columns.group_id.name: group_id,
        }
        MappingCategoryData.bulk_update_by_ids(db_instance, ids, dic_update_values)


def gen_semi_master_data_by_type(
    db_instance: Union[SQLite3, PostgreSQL, Oracle, MySQL, MSSQLServer, scoped_session],
    group_id,
    series: Series,
):
    group_id_col = SemiMaster.Columns.group_id.name
    factor_col = SemiMaster.Columns.factor.name
    semi_master_val_col = SemiMaster.Columns.value.name

    series = series.dropna().astype(pd.StringDtype())

    # exist semi master
    _, rows = SemiMaster.select_records(
        db_instance,
        dic_conditions={group_id_col: group_id},
        select_cols=[semi_master_val_col, factor_col],
        row_is_dict=False,
    )

    uniques = series.drop_duplicates()
    dic_factor = dict(rows)
    if dic_factor:
        next_factor = max(dic_factor.values()) + 1
        new_factor_vals = uniques[~uniques.isin(list(dic_factor))].sort_values().tolist()
    else:
        next_factor = 1
        new_factor_vals = uniques.sort_values().tolist()

    if new_factor_vals:
        df_insert = pd.DataFrame(new_factor_vals, columns=[semi_master_val_col])
        df_insert[group_id_col] = group_id
        df_insert[factor_col] = df_insert.index + next_factor
        dic_new_factor = dict(df_insert[[semi_master_val_col, factor_col]].values.tolist())
        dic_factor.update(dic_new_factor)

        # insert to db
        from bridge.services.data_import import insert_semi_master

        insert_semi_master(
            db_instance,
            df_insert,
            select_cols=[factor_col, group_id_col, semi_master_val_col],
        )
        # return df_insert

    # series = series.map(dic_factor)

    return dic_factor


def transform_value_to_factor(
    db_instance: Union[SQLite3, PostgreSQL, Oracle, MySQL, MSSQLServer],
    data_id: int,
    series: Series,
    data_table_id: int,
):
    series = series.dropna().astype(str)
    dic_conditions = {
        MappingCategoryData.Columns.data_id.name: data_id,
        MappingCategoryData.Columns.data_table_id.name: data_table_id,
    }
    _, rows = MappingCategoryData.select_records(
        db_instance,
        dic_conditions=dic_conditions,
        row_is_dict=False,
        select_cols=[
            MappingCategoryData.Columns.t_category_data.name,
            MappingCategoryData.Columns.factor.name,
        ],
    )
    dic_factor = dict(rows)
    series = series.map(dic_factor)
    return series


def gen_factor_value_not_mapping(
    db_instance: Union[SQLite3, PostgreSQL, Oracle, MySQL, MSSQLServer],
    series,
    data_id,
    data_table_id,
):
    data_id = int(data_id)
    group_id = gen_default_m_group(data_id)
    # TODO: apply for CATEGORY_INT
    unique_values = series.dropna().astype(str).unique().tolist()
    for value in unique_values:
        dic_conditions = {
            MappingCategoryData.Columns.data_id.name: data_id,
            MappingCategoryData.Columns.t_category_data.name: str(value),
            MappingCategoryData.Columns.data_table_id.name: data_table_id,
        }
        _, rows = MappingCategoryData.select_records(
            db_instance,
            dic_conditions=dic_conditions,
            row_is_dict=True,
            select_cols=[
                MappingCategoryData.Columns.id.name,
                MappingCategoryData.Columns.factor.name,
            ],
        )
        if not rows:
            from ap.setting_module.models import MappingCategoryData as EdgeMappingCategoryData
            from ap.setting_module.models import insert_or_update_config, make_session

            mapping_obj = EdgeMappingCategoryData()
            mapping_obj.data_id = data_id
            mapping_obj.data_table_id = data_table_id
            mapping_obj.t_category_data = value
            with make_session() as meta_session:
                mapping_obj = insert_or_update_config(meta_session, mapping_obj)
            rows = [{'id': mapping_obj.id, 'factor': mapping_obj.factor}]

        m_category_data_id = rows[0].get(MappingCategoryData.Columns.id.name)
        factor = rows[0].get(MappingCategoryData.Columns.factor.name)
        if factor:
            continue

        dict_mapping_category_data = {value: [m_category_data_id]}
        gen_factor_n_group_id(db_instance, group_id, dict_mapping_category_data)
