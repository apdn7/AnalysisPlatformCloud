import re
from typing import Dict, List, Optional

import numpy as np
import pandas as pd
from flask_babel import get_locale

from ap.common.common_utils import camel_to_snake, gen_sql_label
from ap.common.constants import (
    __NO_NAME__,
    MAX_NAME_LENGTH,
    PREFIX_TABLE_NAME,
    UNDER_SCORE,
    DataGroupType,
    DataType,
)
from ap.common.datetime_format_utils import DateTimeFormatUtils
from ap.common.logger import log_execution_time, logger
from ap.common.pydn.dblib import mysql
from ap.common.pydn.dblib.db_common import add_double_quote
from ap.common.pydn.dblib.db_proxy_readonly import ReadOnlyDbProxy
from ap.common.pydn.dblib.postgresql import PostgreSQL
from ap.common.services.jp_to_romaji_utils import to_romaji
from bridge.models.bridge_station import AbstractProcess, ConfigModel
from bridge.models.cfg_process_column import CfgProcessColumn
from bridge.models.etl_mapping import MasterType
from bridge.models.m_data import MData
from bridge.models.m_data_group import MDataGroup
from bridge.models.m_process import MProcess
from bridge.models.model_utils import TableColumn


class Temp:  # Vertical holding config
    process_no_col = 'PROCESS_NO'
    sensor_name = 'CHECK_CODE'
    sensor_value = 'CHECK_DATA1'


class CfgProcess(ConfigModel, AbstractProcess):  # AbstractProcess -> unused
    class Columns(TableColumn):
        id = (1, DataType.INTEGER)
        name = (2, DataType.TEXT)
        comment = (5, DataType.TEXT)
        order = (6, DataType.INTEGER)
        table_name = (7, DataType.TEXT)
        name_en = (8, DataType.TEXT)
        name_jp = (9, DataType.TEXT)
        name_local = (10, DataType.TEXT)
        is_show_file_name = (11, DataType.BOOLEAN)
        datetime_format = (12, DataType.TEXT)

        created_at = (98, DataType.DATETIME)
        updated_at = (99, DataType.DATETIME)

    def __init__(self, dict_proc=None, is_cascade=False, db_instance=None):
        dict_proc = dict_proc if dict_proc else {}
        self.id = dict_proc.get(CfgProcess.Columns.id.name)
        self.name = dict_proc.get(CfgProcess.Columns.name.name)
        self.name_en = dict_proc.get(CfgProcess.Columns.name_en.name)
        self.name_jp = dict_proc.get(CfgProcess.Columns.name_jp.name)
        self.name_local = dict_proc.get(CfgProcess.Columns.name.name_local)
        self.comment = dict_proc.get(CfgProcess.Columns.comment.name)
        self.order = dict_proc.get(CfgProcess.Columns.order.name)
        self.is_show_file_name = dict_proc.get(CfgProcess.Columns.is_show_file_name.name)
        self.datetime_format = dict_proc.get(CfgProcess.Columns.datetime_format.name)
        self.table_name = dict_proc.get(CfgProcess.Columns.table_name.name)
        self.created_at = dict_proc.get(CfgProcess.Columns.created_at.name)
        self.updated_at = dict_proc.get(CfgProcess.Columns.updated_at.name)
        self.data_source = None  # type: [CfgDataSourceDB, CfgDataSourceCSV]
        self.columns: List[CfgProcessColumn] = []
        if not is_cascade:
            return

        def _get_relation_data_(_self, _db_instance: PostgreSQL):
            from bridge.models.cfg_process_column import CfgProcessColumn

            _self.columns = CfgProcessColumn.get_by_proc_id(_db_instance, _self.id)

        if db_instance:
            _get_relation_data_(self, db_instance)
        else:
            with self.get_db_proxy() as db_instance:
                _get_relation_data_(self, db_instance)

    def __hash__(self):  # unused ?
        return hash((self.id, self.name, self.data_source, self.comment, self.order))

    _table_name = 'cfg_process'
    primary_keys = [Columns.id]

    def get_shown_name(self):
        try:
            locale = get_locale()
            if not self.name_en:
                self.name_en = to_romaji(self.name)
            if not locale:
                return None
            if locale.language == 'ja':
                return self.name_jp if self.name_jp else self.name_en
            if locale.language == 'en':
                return self.name_en
            else:
                return self.name_local if self.name_local else self.name_en
        except Exception:
            return self.name

    @property
    def shown_name(self):
        return self.get_shown_name()

    def get_name(self):  # see AbstractProcess
        return self.name

    def get_id(self):  # see AbstractProcess
        return self.id

    @classmethod
    def get_by_id(cls, db_instance, id, is_cascade: bool = False):
        id_col = cls.get_pk_column_names()[0]
        _, row = cls.select_records(db_instance, {id_col: int(id)}, limit=1)
        if not row:
            return None
        return CfgProcess(row, is_cascade=is_cascade, db_instance=db_instance)

    @classmethod
    def get_all_proc_ids(cls, db_instance):
        selection = [cls.Columns.id.name]
        _, rows = cls.select_records(db_instance, select_cols=selection, row_is_dict=False)
        return rows

    def get_date_col(self, column_name_only=True):
        """
        get date column
        :param column_name_only:
        :return:
        """
        cols = [col for col in self.columns if col.is_get_date]
        if cols:
            if column_name_only:
                return cols[0].column_name

            return cols[0]

        return None

    def get_auto_increment_col(self, column_name_only=True):
        """
        get auto increment column
        :param column_name_only:
        :return:
        """
        cols = [col for col in self.columns if col.is_auto_increment]
        if cols:
            if column_name_only:
                return cols[0].column_name

            return cols[0]

        return None

    def get_auto_increment_col_else_get_date(self, column_name_only=True):
        """
        get auto increment column
        :param column_name_only:
        :return:
        """
        return self.get_auto_increment_col(column_name_only) or self.get_date_col(column_name_only)

    def get_serials(self, column_name_only=True):
        columns = self.get_columns(False)
        if column_name_only:
            cols = [cfg_col.column_name for cfg_col in columns if cfg_col.is_serial_no]
        else:
            cols = [cfg_col for cfg_col in columns if cfg_col.is_serial_no]

        return cols

    def get_cols_by_data_type(self, data_type: DataType, column_name_only=True):
        """
        get date column
        :param data_type:
        :param column_name_only:
        :return:
        """
        if column_name_only:
            cols = [col.column_name for col in self.columns if col.data_type == data_type.name]
        else:
            cols = [col for col in self.columns if col.data_type == data_type.name]

        return cols

    def get_cols_by_column_type(self, column_type: int, column_name_only=True):
        """
        get date column
        :param data_type:
        :param column_name_only:
        :return:
        """
        if column_name_only:
            cols = [col.column_name for col in self.columns if col.column_type == column_type]
        else:
            cols = [col for col in self.columns if col.column_type == column_type]

        return cols

    def get_col_by_data_group_type(self, db_instance, data_group_type: DataGroupType):
        """
        get date column
        :param data_type:
        :param column_name_only:
        :return:
        """
        dic_conditions = {MDataGroup.Columns.data_group_type.name: data_group_type}
        select_cols = [MDataGroup.Columns.id.name]
        cols, m_data_group = MDataGroup.select_records(db_instance, dic_conditions, select_cols, limit=1)
        if not m_data_group:
            raise Exception(f'{data_group_type} not found in {MDataGroup._table_name}')
        cols = [col for col in self.columns if col.column_type == m_data_group[MDataGroup.Columns.id.name]]
        return cols[0] if cols else None

    @classmethod  # moved to cfg data table todo remove.
    def get_by_process_id(cls, db_instance, process_id: int, is_cascade_column=False):
        dict_process_id = {CfgProcess.Columns.id.name: process_id}
        _col, rows = cls.select_records(db_instance, dic_conditions=dict_process_id)
        if not rows:
            return None
        cols = CfgProcessColumn.get_by_proc_id(db_instance, process_id) if is_cascade_column else []
        cfg_process = CfgProcess(rows[0])
        cfg_process.columns = cols
        if False:
            # parent process's columns has column type, meanwhile generated process column has no column type
            _by_column_type = [col.column_type for col in cfg_process.columns if col.column_type]
            if _by_column_type:
                dict_m_data_groups = MDataGroup.get_in_ids(db_instance, _by_column_type, is_return_dict=True)
                for col in cfg_process.columns:
                    # because GUI created process has no m_data, set m_data_group first.
                    col.m_data_group = (
                        dict_m_data_groups[int(col.column_type)] if col.column_type else None
                    )  # todo remove int()

            m_data_s = MData.get_by_process_id(db_instance, process_id, is_cascade=True)
            for col in cfg_process.columns:
                temp_tuple = (m_data for m_data in m_data_s if m_data.m_data_group.get_sys_name() == col.english_name)
                col.m_data = next(temp_tuple, None)  # get first item or None
                if col.m_data and not col.m_data_group:
                    col.m_data_group = col.m_data.m_data_group
        return cfg_process

    @classmethod
    def get_all(cls, db_instance):
        _, rows = cls.select_records(db_instance)
        return rows

    @classmethod
    def get_procs(cls, ids):
        return cls.query.filter(cls.id.in_(ids))

    @classmethod
    def save(cls, meta_session, form):
        pass
        # if not form.id.data:
        #     row = cls()
        #     meta_session.add(row)
        # else:
        #     row = meta_session.query(cls).get(form.id.data)
        # meta_session.commit()
        # return row

    @classmethod
    def delete(cls, proc_id):
        pass
        # meta_session = Session()
        # proc = meta_session.query(cls).get(proc_id)
        # if proc:
        #     meta_session.delete(proc)
        #
        #     # delete traces manually
        #     meta_session.query(CfgTrace).filter(
        #         or_(CfgTrace.self_process_id == proc_id, CfgTrace.target_process_id == proc_id)
        #     ).delete()
        #
        #     # delete linking prediction manually
        #     meta_session.query(ProcLink).filter(
        #         or_(ProcLink.process_id == proc_id, ProcLink.target_process_id == proc_id)
        #     ).delete()
        #
        #     meta_session.commit()
        #
        #     return True
        # return False

    def get_columns(self, column_name_only=False):
        if not self.columns:
            logger.warning('No any column. Get cfg_process cascade or assign columns to this process')
        cols = [cfg_col.column_name for cfg_col in self.columns] if column_name_only else self.columns
        return cols

    def get_dict_rename_columns(self, db_instance):
        """
        Convert columns name to format '__id__name'.
        {origin column name : converted column name}
        :return:
        """
        predefined_columns = MDataGroup.get_predefined_data_group(db_instance)
        predefined_dict = {str(col.id): col.get_sys_name() for col in predefined_columns}
        dict_source_col_names = {}
        dict_bridge_col_names = {}
        for col in self.get_columns():
            if col.column_type in predefined_dict:
                dict_source_col_names[col.column_name] = gen_sql_label(col.id, predefined_dict[col.column_name])
                predefined_column_name = predefined_dict[col.column_type]
                dict_bridge_col_names[predefined_column_name] = gen_sql_label(col.id, predefined_column_name)
            else:
                dict_source_col_names[col.column_name] = gen_sql_label(col.id, col.column_name)

        return dict_source_col_names, dict_bridge_col_names  # todo: split out to two functions

    def get_master_type_columns(self):
        """
        Ex: {1: 'col_x', 2: 'col_y'}

        :return:
        """
        dict_master_type_column_name: Dict[int, str] = {}

        for cfg_column in self.get_columns():
            if cfg_column.master_type:
                dict_master_type_column_name[cfg_column.master_type] = cfg_column.column_name
        return dict_master_type_column_name

    def get_mapping_master_table_master_type_columns(self):
        """

        :return: (generator)
        """
        dict_cfg_master_columns = self.get_master_type_columns()
        master_pk_columns = MasterType.get_dict_master_type_column_names()
        dict_columns = MasterType.get_mapping_config_master_type(dict_cfg_master_columns)

        master_pk_columns = dict(sorted(master_pk_columns.items(), key=lambda item: -len(item[1])))
        already_selected_columns = set()

        dict_table_column_map = {}
        for master_type, master_columns in master_pk_columns.items():
            # column map with master table
            transaction_columns = [dict_columns.get(col, None) for col in master_columns]

            # column map with master table and not yet map with any other table.
            select_transaction_columns = [col for col in transaction_columns if col not in already_selected_columns]
            if None in transaction_columns or not select_transaction_columns:
                # Daiji. transaction_columns = ['m_plant_no',None] means application checks for m_line,
                #                                                       but config have no m_line column -> skip
                # transaction_columns = ['m_plant_no'] means means application checks for m_plant_no, and it is. -> OK
                # continue
                pass

            # Supports to remove higher level of master table (if import m_line, no need to import m_plant)
            # _ = [already_selected_columns.add(col) for col in select_transaction_columns]
            dict_table_column_map[master_type] = dict(zip(master_columns, select_transaction_columns))

        ignore_master_type = []
        for master_type_1, dict_column_map_1 in list(dict_table_column_map.items()):
            for master_type_2, dict_column_map_2 in list(dict_table_column_map.items()):
                if master_type_1 is master_type_2:
                    continue
                for key, value in list(dict_column_map_2.items())[::-1]:
                    # remove trailing only None value in dict. if it become duplicate, it will be removed later
                    if value is None:
                        dict_column_map_2.pop(key)
                    else:
                        break
                if is_subdict(dict_column_map_2, dict_column_map_1):
                    ignore_master_type.append(master_type_2)

        for master_type, dict_column_map in dict_table_column_map.items():
            if master_type in ignore_master_type:
                continue
            table_name = MasterType.get_dict_master_type_table_name()[master_type]
            yield table_name, dict_column_map.values()

    def get_sensor_columns(self):
        return next(filter(lambda col: col.column_type == 'DATA_VALUE', self.get_columns()), None)

    @log_execution_time()
    def get_factory_process_columns(self, process_no_col):
        with ReadOnlyDbProxy(self.data_source) as factory_db_instance:
            if not isinstance(factory_db_instance, mysql.MySQL):
                table_name = add_double_quote(self.table_name)
                process_no_col = add_double_quote(process_no_col)
            sql = f'select distinct CHECK_CODE, {process_no_col} from {table_name} order by {process_no_col}'
            cols, rows = factory_db_instance.run_sql(sql, row_is_dict=False)
        return cols, rows

    def get_time_format(self) -> Optional[str]:
        """
        Extract time format from datetime_format value
        :return: time format
        """
        datetime_format = DateTimeFormatUtils.get_datetime_format(self.datetime_format)
        return datetime_format.time_format

    def get_date_format(self) -> Optional[str]:
        """
        Extract date format from datetime_format value
        :return: date format
        """
        datetime_format = DateTimeFormatUtils.get_datetime_format(self.datetime_format)
        return datetime_format.date_format


def is_subdict(sub, parent):
    return dict(parent, **sub) == parent


def gen_cfg_process(db_instance: PostgreSQL, df_m_process: pd.DataFrame):
    from bridge.services.data_import import insert_by_df_ignore_duplicate

    name_series = df_m_process[MProcess.get_abbr_name_column()]
    cols = [
        MProcess.get_local_name_column(),
        MProcess.get_jp_name_column(),
        MProcess.get_en_name_column(),
    ]
    if any(name_series.isnull()):
        for col in cols:
            name_series.update(df_m_process[name_series.isnull()][col])
        name_series.fillna(__NO_NAME__, inplace=True)

    df_m_process[CfgProcess.Columns.name.name] = name_series
    df_cfg_process = CfgProcess.get_all_as_df(db_instance)
    merged_df = df_m_process.merge(df_cfg_process, on=CfgProcessColumn.Columns.process_id.name, how='left')
    merged_df[CfgProcess.Columns.name.name] = merged_df['name_y'].fillna(merged_df['name_x'])

    # Drop the '_x' and '_y' columns
    df_m_process = merged_df.drop(['name_x', 'name_y'], axis=1)

    if df_cfg_process.empty:
        df_m_process['is_new_record'] = True
    else:
        df_m_process['is_new_record'] = ~df_m_process[CfgProcessColumn.Columns.process_id.name].isin(
            df_cfg_process[CfgProcessColumn.Columns.process_id.name].to_list(),
        )
    for col in cols:
        df_m_process = add_suffix_for_process_name(df_m_process, col)
    df_m_process[CfgProcess.Columns.table_name.name] = (
        PREFIX_TABLE_NAME
        + df_m_process[CfgProcessColumn.Columns.process_id.name].astype(str)
        + UNDER_SCORE
        + df_m_process[CfgProcess.Columns.name.name]
        .fillna(__NO_NAME__.lower())
        .apply(to_romaji, remote_underline=True)
        .apply(camel_to_snake)
    ).str[:MAX_NAME_LENGTH:]

    df_m_process[CfgProcess.Columns.name_jp.name] = df_m_process[MProcess.get_jp_name_column()]
    df_m_process[CfgProcess.Columns.name_en.name] = df_m_process[MProcess.get_jp_name_column()].apply(
        to_romaji,
        remote_underline=True,
    )
    df_m_process[CfgProcess.Columns.name_local.name] = df_m_process[MProcess.get_local_name_column()]

    insert_by_df_ignore_duplicate(db_instance, df_m_process, CfgProcess)


def add_suffix_for_process_name(df_process, col):
    __NEW_SUFFIX__ = '__NEW_SUFFIX__'
    __ORIGIN_VALUE__ = '__ORIGIN_VALUE__'
    __SUFFIX__ = '__SUFFIX__'

    def _lambda_func(v):
        if pd.isnull(v):
            return np.nan, np.nan
        return (re.search(r'(.*)_(\d{2})$', v) or re.search(r'(.*)()$', v)).groups()

    split_series = df_process[col].map(_lambda_func)
    df_process[[__ORIGIN_VALUE__, __SUFFIX__]] = split_series.apply(pd.Series, index=[__ORIGIN_VALUE__, __SUFFIX__])
    df_process[__SUFFIX__] = pd.to_numeric(df_process[__SUFFIX__], errors='coerce').convert_dtypes()
    grouped_df = df_process.groupby(by=[__ORIGIN_VALUE__])

    for idx, _df in grouped_df:
        if len(_df) == 1:
            continue

        start_num = _df[__SUFFIX__].max()
        if pd.isna(start_num):
            start_num = 0
        _df_new_record = _df[_df['is_new_record']]
        if len(_df) == len(_df_new_record):
            target_duplicate_df = _df_new_record.sort_values([CfgProcessColumn.Columns.process_id.name])[
                [col, __ORIGIN_VALUE__]
            ][1:]
        else:
            target_duplicate_df = _df_new_record.sort_values([CfgProcessColumn.Columns.process_id.name])[
                [col, __ORIGIN_VALUE__]
            ]
        target_duplicate_df[__NEW_SUFFIX__] = [
            f'{n:0>2}' for n in np.arange(start_num + 1, start_num + 1 + len(target_duplicate_df))
        ]
        target_duplicate_df[col] = target_duplicate_df[__ORIGIN_VALUE__] + '_' + target_duplicate_df[__NEW_SUFFIX__]
        df_process.update(target_duplicate_df[[col]])
    return df_process
