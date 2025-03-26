# tunghh: có vài hàm common nhỏ vẫn đang được sử dụng ở file khác
# todo refactor reference, sau đó xoá file này

from itertools import groupby
from typing import Set, Tuple

import pandas as pd

from ap.common.pydn.dblib.db_common import gen_check_exist_sql, gen_insert_sql
from ap.common.pydn.dblib.postgresql import PostgreSQL
from bridge.models.bridge_station import BridgeStationModel, MasterModel, MasterModelMapping
from bridge.models.etl_mapping import EtlMapping


# todo remove
def gen_mapping_obj_master_table(db_id=None):
    sql, params = EtlMapping.get_records_by_data_src(db_id)
    with BridgeStationModel.get_db_proxy() as db_instance:
        _, data = db_instance.run_sql(sql, row_is_dict=False, params=params)

        dic_output = {}
        for table_name, rows in groupby(data, lambda x: tuple(x[0:3])):
            dic_output[table_name] = [tuple(row[3:6]) for row in rows]

    return dic_output


# todo remove
def drop_existing_records(df):
    """
    Drops WHERE "master_data_id" is not NULL
    :param df:
    :return:
    """
    df.drop(index=df[~df['master_data_id__NON_STANDARD__'].isna()].index, inplace=True)


# todo remove
def drop_non_standard_records(df):
    """
    Drops WHERE "parent_id" != 0
    :param df:
    :return:
    """
    df.drop(index=df[df['parent_id__NON_STANDARD__'].isna()].index, inplace=True)
    df.drop(index=df[df['parent_id__NON_STANDARD__'] != 0].index, inplace=True)


# todo remove
def remove_non_common_records(df, pk_column_names):
    """
    Sorts by pk columns, parent id, remove duplicate pk, keep min parent id
    :param df:
    :param pk_column_names:
    :return:
    """
    order_columns = [*pk_column_names, 'parent_id__NON_STANDARD__']
    df.sort_values(by=order_columns, ascending=True, inplace=True)
    df.drop_duplicates(subset=pk_column_names, keep='first', inplace=True)


# todo remove
def apply_standard_value(df, pk_column_names):
    standard_pk_column_names = [f'{col}__STANDARD__' for col in pk_column_names]
    cond = True
    for col in standard_pk_column_names:
        cond = cond & ~df[col].isna()  # "have standard value" means "pk columns not none"
    idx = df[cond].index  # records have standard value
    for pk_col, standard_pk_col in zip(pk_column_names, standard_pk_column_names):
        df.loc[idx, pk_col] = df.loc[idx, standard_pk_col]


# todo remove
def merge_df(df_master_data, df_mapping, pk_column_names, bridge_cols):
    """
    Merges raw data, non standard data (__NON_STANDARD__), standard data (__STANDARD__) into one df.

    :param df_master_data:
    :param df_mapping:
    :param pk_column_names:
    :param bridge_cols:
    :return:
    """

    # "suffixes" occur when merge same column but diff content
    df = df_master_data.merge(df_mapping, on=pk_column_names, how='left', suffixes=[None, '__NON_STANDARD__'])

    # no business for __NON_STANDARD__, just for debug and avoid mis-reading
    dic_col_rename = {
        col: f'{col}__NON_STANDARD__' for col in df.columns if col not in bridge_cols and '__STANDARD__' not in col
    }
    return df.rename(columns=dic_col_rename)


# todo remove
def append_standard_columns(master_cls, df_mapping):
    if not df_mapping.empty:
        standard_cols = [
            'id',  # join column
            *master_cls.get_pk_column_names(),  # standard value (will be inserted value)
            'master_data_id',
        ]  # not use now. maybe use later ? remove is ok,

        # "suffixes" occur when merge same column but diff value.
        df_mapping = df_mapping.merge(
            df_mapping[standard_cols],
            left_on='parent_id',
            right_on='id',
            how='left',
            suffixes=[None, '__STANDARD__'],
        )

        # update_standard_value_for_parent(df_mapping, standard_cols)
        df_mapping.drop(columns=['id__STANDARD__'], inplace=True)  # float id column -> risk + unnecessary -> remove
        df_mapping = df_mapping.convert_dtypes()
    return df_mapping


# todo remove
def update_standard_value_for_parent(df_mapping, standard_cols):
    """

    :param df_mapping:
    :param standard_cols:
    :return:
    """
    __STANDARD__cols = [f'{col}__STANDARD__' for col in standard_cols]
    df_parent = df_mapping[df_mapping['parent_id'] == 0]
    for _1, _2 in zip(__STANDARD__cols, standard_cols):
        # không viết được df_mapping[__STANDARD__cols] = df_mapping[standard_cols] nên viết kiểu này đỡ
        df_mapping.loc[df_parent.index, _1] = df_mapping.loc[df_parent.index, _2]


def get_model_class(bridge_table) -> Tuple[MasterModel, MasterModelMapping]:
    """
    Returns pair of class master class, mapping class.
    Ex: MLine, MLineMapping

    Mapping class may None

    :param bridge_table:
    :return:
    """
    # TODO: create common cache method and put here, the same table name will be input many time
    mapping_classes: Set[MasterModelMapping] = MasterModelMapping.get_all_subclasses()

    mapping_cls = next(
        (m_cls for m_cls in mapping_classes if bridge_table == m_cls.master_model.get_table_name()),
        None,
    )
    if mapping_cls:  # None case may occur. (this is use case, not just simple None check)
        master_cls = mapping_cls.master_model
    else:
        master_classes = MasterModel.get_all_subclasses()
        master_cls = next((m_cls for m_cls in master_classes if bridge_table == m_cls.get_table_name()), None)
    return master_cls, mapping_cls


def get_mapping_df(mapping_cls: MasterModelMapping, db_source_id):
    # TODO: put cache
    if mapping_cls:
        with BridgeStationModel.get_db_proxy() as db_instance:
            mapping_cols, mapping_rows = mapping_cls.get_by_data_source_id(db_instance, db_source_id)
    else:
        mapping_cols, mapping_rows = [], []
    df_master_mapping = pd.DataFrame(columns=mapping_cols, data=mapping_rows)
    return df_master_mapping


# todo remove
def import_master_data(db_instance: PostgreSQL, model_cls: MasterModel, cols, data):
    """
    import master data into master table
    :param db_instance:
    :param model_cls:
    :param cols:
    :param data:
    :return:
    """
    sql, params = gen_check_exist_sql(model_cls)
    _, has_rows = db_instance.run_sql(sql, row_is_dict=False, params=params)
    if not has_rows:
        # bulk insert
        parameter_marker = model_cls.get_parameter_marker()
        db_instance.bulk_insert(model_cls.get_table_name(), cols, data, parameter_marker)
        return True

    # insert or update
    for rows in data:
        dic_values = dict(zip(cols, rows))
        sql, params = gen_insert_sql(model_cls, dic_values, on_conflict_update=True)
        db_instance.execute_sql(sql, params=params)

    return True
