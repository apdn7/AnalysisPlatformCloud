from datetime import datetime

import pandas as pd

from ap import log_execution_time
from ap.common.common_utils import convert_nan_to_none, convert_type_base_df
from ap.common.constants import (
    EMPTY_STRING,
    NA_STR,
    DataGroupType,
    FilterFunc,
    RelationShip,
    Suffixes,
)
from ap.common.memoize import memoize
from ap.common.pydn.dblib.postgresql import PostgreSQL
from ap.setting_module.models import (
    CfgFilter,
    CfgFilterDetail,
    CfgProcess,
    CfgProcessColumn,
    MEquip,
    MEquipGroup,
    MLine,
    MLineGroup,
    MPart,
    RProdPart,
    crud_config,
    insert_or_update_config,
    make_session,
)
from ap.setting_module.services.process_config import get_all_process
from bridge.common.bridge_station_config_utils import PostgresSequence
from bridge.models.bridge_station import BridgeStationModel
from bridge.models.mapping_factory_machine import MappingFactoryMachine
from bridge.models.r_factory_machine import RFactoryMachine
from bridge.models.transaction_model import TransactionData


def get_filter_request_data(params):
    process_id = params.get('processId')
    filter_id = ''.join(params.get('filterId') or [])
    filter_type = params.get('filterType')
    filter_name = params.get('filterName')
    column_id = params.get('columnName') or None
    filter_parent_detail_ids = params.get('filterDetailParentIds') or []
    filter_detail_ids = params.get('fitlerDetailIds') or []
    filter_detail_conds = params.get('filterConditions') or []
    filter_detail_names = params.get('filterDetailNames') or []
    filter_detail_functions = params.get('filterFunctions') or []
    filter_detail_start_froms = params.get('filterStartFroms') or []

    if not filter_parent_detail_ids:
        filter_parent_detail_ids = [None] * len(filter_detail_ids)
    if not filter_detail_functions:
        filter_detail_functions = [None] * len(filter_detail_ids)
    if not filter_detail_start_froms:
        filter_detail_start_froms = [None] * len(filter_detail_ids)

    return [
        process_id,
        filter_id,
        filter_type,
        column_id,
        filter_detail_ids,
        filter_detail_conds,
        filter_detail_names,
        filter_parent_detail_ids,
        filter_detail_functions,
        filter_detail_start_froms,
        filter_name,
    ]


def save_filter_config(params):
    [
        process_id,
        filter_id,
        filter_type,
        column_id,
        filter_detail_ids,
        filter_detail_conds,
        filter_detail_names,
        filter_parent_detail_ids,
        filter_detail_functions,
        filter_detail_start_froms,
        filter_name,
    ] = get_filter_request_data(params)

    with make_session() as meta_session:
        cfg_filter = CfgFilter(
            **{
                'id': int(filter_id) if filter_id else None,
                'process_id': process_id,
                'name': filter_name,
                'column_id': column_id,
                'filter_type': filter_type,
            },
        )
        cfg_filter = insert_or_update_config(meta_session, cfg_filter)
        meta_session.commit()

        filter_id = cfg_filter.id  # to return to frontend (must)

        # crud filter details
        num_details = len(filter_detail_conds)
        filter_details = []
        for idx in range(num_details):
            filter_detail = CfgFilterDetail(
                **{
                    'id': int(filter_detail_ids[idx]) if filter_detail_ids[idx] else None,
                    'filter_id': cfg_filter.id,
                    'name': filter_detail_names[idx],
                    'parent_detail_id': filter_parent_detail_ids[idx] or None,
                    'filter_condition': filter_detail_conds[idx],
                    'filter_function': filter_detail_functions[idx] or None,
                    'filter_from_pos': filter_detail_start_froms[idx] or None,
                },
            )
            filter_details.append(filter_detail)

        crud_config(
            meta_session=meta_session,
            data=filter_details,
            model=CfgFilterDetail,
            key_names=CfgFilterDetail.id.key,
            parent_key_names=CfgFilterDetail.filter_id.key,
            parent_obj=cfg_filter,
            parent_relation_key=CfgFilter.filter_details.key,
            parent_relation_type=RelationShip.MANY,
        )
    return filter_id


def delete_cfg_filter_from_db(filter_id):
    with make_session() as mss:
        CfgFilter.delete_by_id(mss, filter_id)


@memoize()
def get_filter_config_values(cfg_col_id):
    cfg_col: CfgProcessColumn = CfgProcessColumn.get_by_id(cfg_col_id)
    with BridgeStationModel.get_db_proxy() as db_instance:
        transaction_data_obj = TransactionData(cfg_col.process_id)
        list_tables = db_instance.list_tables()
        if transaction_data_obj.table_name not in list_tables:
            return []

        data_values = transaction_data_obj.get_transaction_distinct_values(db_instance, cfg_col)
        all_data_values = [EMPTY_STRING if value is None else str(value) for value in data_values]
        unique_data_vals = sorted(all_data_values[:1000])
        return unique_data_vals


def insert_default_filter_config():
    filter_function = 'MATCHES'
    with make_session() as meta_session:
        # 1. Get data to summarize
        # 1.1. get process & column data
        cfg_processes: list[CfgProcess] = get_all_process(session=meta_session)
        process_ids = [cfg_process.id for cfg_process in cfg_processes]
        cfg_process_columns: list[CfgProcessColumn] = CfgProcessColumn.get_by_column_types(
            [
                DataGroupType.LINE_ID.value,
                DataGroupType.PART_NO.value,
                DataGroupType.EQUIP_ID.value,
            ],
            session=meta_session,
        )
        process_column_dict = {}
        for cfg_process_column in cfg_process_columns:
            if not process_column_dict.get(cfg_process_column.process_id):
                process_column_dict[cfg_process_column.process_id] = {}
            column_type = DataGroupType(cfg_process_column.column_type).name
            process_column_dict[cfg_process_column.process_id][column_type] = cfg_process_column

        # 1.2. get filter data
        column_ids = [cfg_process_column.id for cfg_process_column in cfg_process_columns]
        cfg_filters: list[CfgFilter] = CfgFilter.get_by_proc_n_col_ids(
            process_ids,
            column_ids,
            filter_types=[
                DataGroupType.LINE_ID.name,
                DataGroupType.PART_NO.name,
                DataGroupType.EQUIP_ID.name,
            ],
            session=meta_session,
        )
        process_filter_dict = {}
        for cfg_filter in cfg_filters:
            if not process_filter_dict.get(cfg_filter.process_id):
                process_filter_dict[cfg_filter.process_id] = {}
            if not process_filter_dict[cfg_filter.process_id].get(cfg_filter.filter_type):
                process_filter_dict[cfg_filter.process_id][cfg_filter.filter_type] = {}
            process_filter_dict[cfg_filter.process_id][cfg_filter.filter_type][cfg_filter.column_id] = cfg_filter

        # 1.3. get filter detail data
        cfg_filter_details: list[CfgFilterDetail] = CfgFilterDetail.get_all(meta_session)
        filter_detail_dict = {}
        for cfg_filter_detail in cfg_filter_details:
            if not filter_detail_dict.get(cfg_filter_detail.filter_id):
                filter_detail_dict[cfg_filter_detail.filter_id] = {}
            if not filter_detail_dict[cfg_filter_detail.filter_id].get(cfg_filter_detail.filter_function):
                filter_detail_dict[cfg_filter_detail.filter_id][cfg_filter_detail.filter_function] = {}
            filter_detail_dict[cfg_filter_detail.filter_id][cfg_filter_detail.filter_function][
                cfg_filter_detail.filter_condition
            ] = cfg_filter_detail

        # 1.4. get master data such as LINE, MACHINE, PART
        m_line_group_df = MLineGroup.get_all_as_df(session=meta_session)
        m_equip_group_df = MEquipGroup.get_all_as_df(session=meta_session)
        m_part_df = MPart.get_all_as_df(session=meta_session)
        filter_type_dict = {
            DataGroupType.LINE_ID.name: m_line_group_df[MLineGroup.line_name_jp.key]
            .dropna()
            .drop_duplicates()
            .tolist(),
            DataGroupType.EQUIP_ID.name: m_equip_group_df[MEquipGroup.equip_name_jp.key]
            .dropna()
            .drop_duplicates()
            .tolist(),
            DataGroupType.PART_NO.name: m_part_df[MPart.part_no.key].dropna().drop_duplicates().tolist(),
        }

        # 2. Insert or update filter & filter detail data
        for cfg_process in cfg_processes:
            for filter_type, filter_conditions in filter_type_dict.items():
                # 2.1. Insert cfg_filter table
                process_col = process_column_dict.get(cfg_process.id, {}).get(filter_type, None)
                if not process_col:
                    continue

                exist_filter = (
                    process_filter_dict.get(cfg_process.id, {}).get(filter_type, {}).get(process_col.id, None)
                )
                cfg_filter = CfgFilter(
                    **{
                        'id': exist_filter.id if exist_filter else None,
                        'process_id': cfg_process.id,
                        'name': exist_filter.name if exist_filter else None,
                        'column_id': process_col.id,
                        'filter_type': filter_type,
                    },
                )
                if not exist_filter:
                    cfg_filter = insert_or_update_config(meta_session, cfg_filter)
                    meta_session.flush()

                # 2.2. Insert cfg_filter_detail table
                for filter_condition in filter_conditions:
                    exist_filter_detail = (
                        filter_detail_dict.get(cfg_filter.id, {}).get(filter_function, {}).get(filter_condition, None)
                    )
                    cfg_filter_detail = CfgFilterDetail(
                        **{
                            'id': exist_filter_detail.id if exist_filter_detail else None,
                            'filter_id': cfg_filter.id,
                            'name': exist_filter_detail.name if exist_filter_detail else filter_condition,
                            'parent_detail_id': exist_filter_detail.parent_detail_id if exist_filter_detail else None,
                            'filter_condition': filter_condition,
                            'filter_function': filter_function,
                            'filter_from_pos': None,
                        },
                    )
                    if not exist_filter_detail:
                        insert_or_update_config(meta_session, cfg_filter_detail)


@log_execution_time()
def insert_default_filter_config_raw_sql(db_instance: PostgreSQL, data_table_id: int):
    from bridge.models.cfg_filter import CfgFilter as BridgeCfgFilter
    from bridge.models.cfg_process_column import CfgProcessColumn as BridgeCfgProcessColumn
    from bridge.models.m_data import MData
    from bridge.models.m_equip import MEquip
    from bridge.models.m_line import MLine
    from bridge.models.m_part import MPart
    from bridge.models.m_process import MProcess
    from bridge.models.mapping_process_data import MappingProcessData

    # TODO: remove all default filter if exist

    filter_type_dict = {
        DataGroupType.LINE.value: DataGroupType.LINE.name,
        DataGroupType.PART.value: DataGroupType.PART.name,
        DataGroupType.EQUIP.value: DataGroupType.EQUIP.name,
    }

    # Get data to summarize
    rows = MappingFactoryMachine.get_process_id_with_data_table_id(db_instance, [data_table_id])
    r_factory_machine = pd.DataFrame(rows)
    all_m_process_df = MProcess.get_all_as_df(db_instance)
    process_group = r_factory_machine.groupby(RFactoryMachine.Columns.process_id.name)
    m_data_df = MData.get_all_as_df(db_instance)
    all_m_line_df = MLine.get_all_as_df(db_instance)
    all_m_equip_df = MEquip.get_all_as_df(db_instance)
    m_part_df = MPart.get_all_as_df(db_instance)
    m_part_df = m_part_df[m_part_df[RProdPart.part_id.name] != 1]  # not gen record Null
    cfg_filter_df_db = BridgeCfgFilter.get_all_as_df(db_instance)
    for process_id, df in process_group:
        # process_ids = [row.get(CfgProcessColumn.Columns.process_id.name) for row in rows]
        m_process_df = all_m_process_df[all_m_process_df[MData.Columns.process_id.name].isin([process_id])]
        cfg_process_column_df = BridgeCfgProcessColumn.get_all_as_df(db_instance)
        m_data_df = m_data_df[
            m_data_df[MData.Columns.data_group_id.name].isin(
                [DataGroupType.LINE.value, DataGroupType.PART.value, DataGroupType.EQUIP.value],
            )
        ]

        process_column_df = m_process_df.merge(m_data_df, on=MData.Columns.process_id.name)

        # filter to get exist master columns only (NOT DUMMY MASTER COLUMNS)
        process_column_df = process_column_df[process_column_df.data_id.isin(cfg_process_column_df.process_column_id)]
        if not len(process_column_df):
            return

        # Initialize new cfg_filter data
        cfg_filter_df = process_column_df[
            [
                MData.Columns.process_id.name,
                MappingProcessData.Columns.data_id.name,
            ]
        ].rename(columns={MappingProcessData.Columns.data_id.name: CfgFilter.column_id.name})
        cfg_filter_df[CfgFilter.filter_type.name] = process_column_df[MData.Columns.data_group_id.name].replace(
            filter_type_dict,
        )
        cfg_filter_cols = list(cfg_filter_df.columns)
        cfg_filter_df = cfg_filter_df.merge(
            cfg_filter_df_db,
            how='left',
            on=[
                CfgFilter.filter_type.name,
                CfgFilter.process_id.name,
                CfgFilter.column_id.name,
            ],
            indicator=True,
            suffixes=Suffixes.KEEP_LEFT,
        )
        new_cfg_filter_df = cfg_filter_df[cfg_filter_df['_merge'] == 'left_only'][cfg_filter_cols].dropna()
        # Gen cfg filter
        if len(new_cfg_filter_df):
            new_cfg_filter_df[CfgFilter.id.name] = PostgresSequence.get_next_id_by_table(
                db_instance,
                CfgFilter.get_table_name(),
                len(new_cfg_filter_df),
            )
            new_cfg_filter_df[[CfgFilter.created_at.name, CfgFilter.updated_at.name]] = datetime.now()
            filter_value = convert_nan_to_none(new_cfg_filter_df, convert_to_list=True)
            db_instance.bulk_insert(CfgFilter.get_table_name(), new_cfg_filter_df.columns.tolist(), filter_value)
            new_cfg_filter_df = new_cfg_filter_df.rename(
                columns={CfgFilter.id.name: CfgFilterDetail.filter_id.name},
            )
            cfg_filter_df = new_cfg_filter_df

        gen_cfg_filter_details(db_instance, df, cfg_filter_df, all_m_line_df, all_m_equip_df, m_part_df)


def gen_cfg_filter_details(db_instance, df, cfg_filter_df, all_m_line_df, all_m_equip_df, m_part_df):
    from bridge.models.cfg_filter_detail import CfgFilterDetail as BridgeCfgFilterDetail

    detail_dfs = []
    for filter_type, detail_df in cfg_filter_df.groupby(by=[CfgFilter.filter_type.name]):
        df_filter_detail = detail_df[[CfgFilterDetail.filter_id.name]]
        df_filter_detail[CfgFilterDetail.filter_function.name] = FilterFunc.MATCHES.name

        if filter_type == DataGroupType.LINE.name:
            line_ids = df[RFactoryMachine.Columns.line_id.name].values.tolist()
            m_line_df = all_m_line_df[all_m_line_df[RFactoryMachine.Columns.line_id.name].isin(line_ids)]
            m_line_df = m_line_df.replace({pd.NA: EMPTY_STRING})
            m_line_df[CfgFilterDetail.name.name] = m_line_df[MLine.line_sign.name] + m_line_df[
                MLine.line_no.name
            ].astype(str)
            m_line_df[CfgFilterDetail.filter_condition.name] = m_line_df[RFactoryMachine.Columns.line_id.name]
            m_line_df[CfgFilterDetail.name.name] = m_line_df[CfgFilterDetail.name.name].replace(
                {EMPTY_STRING: NA_STR},
            )
            data_condition_df = m_line_df[[CfgFilterDetail.name.name, CfgFilterDetail.filter_condition.name]]

        elif filter_type == DataGroupType.EQUIP.name:
            equip_ids = df[RFactoryMachine.Columns.equip_id.name].values.tolist()
            m_equip_df = all_m_equip_df[all_m_equip_df[RFactoryMachine.Columns.equip_id.name].isin(equip_ids)]
            m_equip_df = m_equip_df.replace({pd.NA: EMPTY_STRING})
            m_equip_df[CfgFilterDetail.name.name] = m_equip_df[MEquip.equip_sign.name] + m_equip_df[
                MEquip.equip_no.name
            ].astype(str)
            m_equip_df[CfgFilterDetail.filter_condition.name] = m_equip_df[RFactoryMachine.Columns.equip_id.name]
            m_equip_df[CfgFilterDetail.name.name] = m_equip_df[CfgFilterDetail.name.name].replace(
                {EMPTY_STRING: NA_STR},
            )
            data_condition_df = m_equip_df[[CfgFilterDetail.name.name, CfgFilterDetail.filter_condition.name]]
        elif filter_type == DataGroupType.PART.name:
            m_part_df[CfgFilterDetail.name.name] = m_part_df[MPart.part_no.name].combine_first(
                m_part_df[MPart.part_factid.name].astype(str),
            )

            m_part_df[CfgFilterDetail.filter_condition.name] = m_part_df[RProdPart.part_id.name]
            data_condition_df = m_part_df[[CfgFilterDetail.name.name, CfgFilterDetail.filter_condition.name]]

        df_filter_detail = df_filter_detail.merge(data_condition_df, how='cross')
        detail_dfs.append(df_filter_detail)

    cfg_filter_detail_df = pd.concat(detail_dfs)
    cfg_filter_detail_df_db = BridgeCfgFilterDetail.get_all_as_df(db_instance)
    cfg_filter_detail_cols = list(cfg_filter_detail_df.columns)
    merge_cols = [
        CfgFilterDetail.filter_id.name,
        CfgFilterDetail.filter_condition.name,
        CfgFilterDetail.filter_function.name,
    ]
    convert_type_base_df(cfg_filter_detail_df, cfg_filter_detail_df_db, merge_cols)
    cfg_filter_detail_df = cfg_filter_detail_df.merge(
        cfg_filter_detail_df_db,
        how='left',
        on=merge_cols,
        indicator=True,
        suffixes=Suffixes.KEEP_LEFT,
    )
    cfg_filter_detail_df = cfg_filter_detail_df[cfg_filter_detail_df['_merge'] == 'left_only']
    cfg_filter_detail_df = cfg_filter_detail_df[cfg_filter_detail_cols].dropna()
    cfg_filter_detail_df[[CfgFilter.created_at.name, CfgFilter.updated_at.name]] = datetime.now()
    cfg_filter_detail_df = cfg_filter_detail_df.sort_values([CfgFilterDetail.name.name])
    db_instance.bulk_insert(
        CfgFilterDetail.get_table_name(),
        cfg_filter_detail_df.columns.tolist(),
        cfg_filter_detail_df.values.tolist(),
    )
