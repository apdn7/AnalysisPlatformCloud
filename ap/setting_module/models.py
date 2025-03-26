from __future__ import annotations

import datetime
import json
from builtins import setattr
from contextlib import contextmanager
from typing import Any, Dict, List, Optional, Union

import numpy as np
import pandas as pd
import sqlalchemy
from flask_babel import get_locale
from flask_sqlalchemy import BaseQuery
from pandas import DataFrame
from sqlalchemy import and_, asc, desc, event, func, or_
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.inspection import inspect
from sqlalchemy.orm import RelationshipProperty, Session, load_only, scoped_session
from typing_extensions import Self

from ap import BasicConfigYaml, db, get_basic_yaml_obj, get_file_mode
from ap.common.common_utils import (
    JobBreakException,
    chunks,
    convert_nullable_int64_to_numpy_int64,
    convert_time,
    convert_to_d3_format,
    dict_deep_merge,
    format_df,
    gen_sql_label,
    get_column_order,
    get_current_timestamp,
)
from ap.common.constants import (
    DEFAULT_EQUIP_SIGN,
    DEFAULT_ERROR_DISK_USAGE,
    DEFAULT_LINE_SIGN,
    DEFAULT_NONE_VALUE,
    DEFAULT_ST_SIGN,
    DEFAULT_WARNING_DISK_USAGE,
    EFA_HEADER_FLAG,
    NULL_DEFAULT_STRING,
    SQL_IN_MAX,
    VAR_X,
    VAR_Y,
    BaseMasterColumn,
    CacheType,
    CfgConstantType,
    CRUDType,
    CsvDelimiter,
    CSVExtTypes,
    DataGroupType,
    DataType,
    DBType,
    DiskUsageStatus,
    FunctionCastDataType,
    JobStatus,
    JobType,
    MasterDBType,
    MaxGraphNumber,
    RawDataTypeDB,
    RelationShip,
    max_graph_number,
)
from ap.common.cryptography_utils import decrypt_pwd
from ap.common.datetime_format_utils import DateTimeFormatUtils
from ap.common.logger import logger
from ap.common.memoize import set_all_cache_expired
from ap.common.services import http_content
from ap.common.services.http_content import json_dumps
from ap.common.services.jp_to_romaji_utils import to_romaji
from ap.common.services.normalization import model_normalize
from ap.common.trace_data_log import Location, LogLevel, ReturnCode
from ap.setting_module.forms import DataSourceCsvForm, ProcessCfgForm
from bridge.redis_utils.db_changed import publish_master_config_changed

basic_config_yaml: BasicConfigYaml = get_basic_yaml_obj()
db_timestamp = db.TIMESTAMP


class CommonDBModel(db.Model):
    __abstract__ = True

    @classmethod
    def from_dict(
        cls,
        dict_object: dict[str, Any],
    ) -> Self:
        dict_object_modified = {}
        for attr in sqlalchemy.inspect(cls).attrs:
            if attr.key not in dict_object:
                continue
            value = dict_object[attr.key]
            if isinstance(value, list):
                dict_object_modified[attr.key] = [
                    attr.entity.class_.from_dict(v.__dict__) if hasattr(v, '__dict__') else v for v in value
                ]
            elif isinstance(attr, RelationshipProperty) and hasattr(value, '__dict__'):
                dict_object_modified[attr.key] = attr.entity.class_.from_dict(value.__dict__)
            else:
                dict_object_modified[attr.key] = value

        return cls(**dict_object_modified)  # noqa

    def as_dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}

    @classmethod
    def get_table_name(cls):
        return str(cls.__table__.name)

    def update_by_dict(self, dict_data):
        """
        create new instance from a dict

        :param dict_data:
        :return:
        """
        cols = self.get_column_names()
        for key, val in dict_data.items():
            if key in cols:
                setattr(self, key, val)

    @classmethod
    def update_by_conditions(cls, dic_update_vals, ids=None, dic_conditions=None):
        query = cls.query
        if ids:
            query = query.filter(cls.id.in_(ids))

        if dic_conditions:
            for col, val in dic_conditions.items():
                query = query.filter(col.in_(val)) if isinstance(val, (tuple, list)) else query.filter(col == val)

        query.update(dic_update_vals, synchronize_session='fetch')

    @classmethod
    def get_all(cls):
        return cls.query.all()

    @classmethod
    def select_records(
        cls,
        select_cols=None,
        condition_cols=None,
        condition_vals=None,
        dic_conditions=None,
        order_by_cols=None,
        session=None,
    ):
        if session is None:
            session = db.session

        if condition_cols:
            if not isinstance(condition_cols, (tuple, list)):
                condition_cols = [condition_cols]
                condition_vals = [condition_vals]
            dic_conditions = dict(zip(condition_cols, condition_vals))

        if not dic_conditions:
            dic_conditions = {}

        query = session.query(*select_cols) if select_cols else session.query(cls)

        for col, val in dic_conditions.items():
            query = query.filter(col == val)

        if order_by_cols:
            query = query.order_by(*order_by_cols)

        records = query.all()

        return records

    @classmethod
    def get_column_names(cls):
        """
        use for physical table only
        :return:
        """
        return [col.name for col in list(cls.__table__.columns)]

    @classmethod
    def create_instance(cls, dict_data):
        """
        create new instance from a dict

        :param dict_data:
        :return:
        """
        cols = cls.get_column_names()
        valid_dict = {key: val for key, val in dict_data.items() if key in cols}

        return cls(**valid_dict)

    @classmethod
    def delete_by_ids(cls, ids, session=None):
        key = cls.get_primary_keys()
        for chunk_ids in chunks(ids, SQL_IN_MAX):
            session.query(cls).filter(key.in_(chunk_ids)).delete(synchronize_session='fetch')

        return True

    @classmethod
    def get_primary_keys(cls, first_key_only=True):
        """
        get primary key
        :param cls:
        :param first_key_only:
        :return:
        """
        keys = list(inspect(cls).primary_key)
        if first_key_only:
            return keys[0]

        return keys

    @classmethod
    def get_foreign_id_column_name(cls) -> str:  # only use for cfg_ and m_
        """
        m_line  ->  line_id

        :return:
        """
        elems = cls.__tablename__.split('_')
        return f"{'_'.join(elems[1:])}_id"

    @classmethod
    def get_by_id(cls, id, session=None):
        query = session.query(cls) if session else cls.query
        return query.filter(cls.id == id).first()

    @classmethod
    def get_fmt_by_id(cls, id):
        column = cls.get_by_id(id)
        return convert_to_d3_format(column.format)

    @classmethod
    def get_by_id_with_entities(cls, id, col, session=None):
        query = session.query(cls) if session else cls.query
        return query.filter(cls.id == int(id)).with_entities(col).first()

    @classmethod
    def get_in_ids(cls, ids, session=None):
        query = session.query(cls) if session else cls.query
        return query.filter(cls.id.in_(ids)).order_by(cls.id.desc()).all()

    @classmethod
    def get_in_process_ids(cls, process_ids, session=None):
        query = session.query(cls) if session else cls.query
        return query.filter(cls.process_id.in_(process_ids)).order_by(cls.id.desc()).all()

    @classmethod
    def get_all_ids(cls, ids=None, session=None):
        query = session.query(cls) if session else cls.query
        data = query.options(load_only(cls.id))
        if ids:
            data = data.filter(cls.id.in_(ids))

        return data.all()

    @classmethod
    def get_all_as_df(
        cls,
        select_cols=None,
        dic_conditions=None,
        session=None,
        is_convert_null_string_to_na: bool = True,
    ) -> DataFrame:
        query = session.query(cls) if session else cls.query
        if select_cols:
            query = query.options(load_only(*select_cols))

        query_conds = []
        cls_cols = cls.get_column_names()
        if dic_conditions:
            for col, vals in dic_conditions.items():
                col_name = col if isinstance(col, str) else col.key

                if col_name not in cls_cols:
                    continue

                col_obj = getattr(cls, col_name)

                if isinstance(vals, (list, tuple)):
                    query_conds.append(col_obj.in_(vals))
                else:
                    query_conds.append(col_obj == vals)

            query = query.filter(*query_conds)

        cols = query.statement.columns.keys()
        rows = db.session.execute(query).fetchall()
        df = pd.DataFrame(rows, columns=cols, dtype='object')
        if is_convert_null_string_to_na:
            df.replace({NULL_DEFAULT_STRING: DEFAULT_NONE_VALUE}, inplace=True)
        df = format_df(df)

        for col in query.statement.columns:
            if col.type.python_type is int:
                convert_nullable_int64_to_numpy_int64(df, [col.name])
            if col.type.python_type is float:
                df[col.name] = df[col.name].astype(pd.Float64Dtype.name)
            if col.type.python_type is str:
                df[col.name] = df[col.name].astype(pd.StringDtype.name)
            if col.type.python_type is datetime.datetime:
                # df[col.name] = pd.to_datetime(df[col.name], format="%m/%d/%Y, %H:%M:%S")
                df[col.name] = df[col.name].astype(np.datetime64.__name__)
            if col.type.python_type is bool:
                df[col.name] = df[col.name].astype(pd.BooleanDtype.name)

        if 'id' in df.columns and not issubclass(cls, MappingDBModel):  # Ignore mapping tables
            # In Bridge Station system, all models should have same behavior to publish itseft id column
            df.rename(columns={'id': cls.get_foreign_id_column_name()}, inplace=True)

        return df

    @classmethod
    def get_all_records(cls, session=None):  # todo use BaseModel
        query = session.query(cls) if session else cls.query
        cols = cls.__table__.columns.keys()
        rows = db.session.execute(query).fetchall()
        return cols, rows

    @classmethod
    def insert_records(cls, columns, rows, session, is_all_insert=False):
        """
        insert records
        :return:
        :param columns:
        :param rows: [{id:1,name:abc},{id:2,name:def]
        :param session:
        :param is_all_insert:
        :return:
        """
        if not rows:
            return []

        cols = cls.get_column_names()
        all_ids = []
        edited_rows = []
        date_times = []
        for dic_data_original in rows:
            if isinstance(dic_data_original, dict):
                dic_data = {key: val for key, val in dic_data_original.items() if key in cols}
            elif isinstance(dic_data_original, (list, tuple)):
                dic_data = {key: val for key, val in zip(columns, dic_data_original) if key in cols}
            else:
                dic_data = {col: getattr(dic_data_original, col) for col in cols if hasattr(dic_data_original, col)}

            edited_rows.append(dic_data)
            all_ids.append(dic_data.get('id'))
            date_times.append(dic_data.get('updated_at'))

        # insert to db
        dic_db_rows = {}
        if not is_all_insert:
            for ids in chunks(all_ids):
                db_rows = cls.get_in_ids(ids)
                dic_db_rows.update({_row.id: _row for _row in db_rows})
        insert_rows = []
        for id, dic_data, updated_at in zip(all_ids, edited_rows, date_times):
            row = dic_db_rows.get(id, None)
            if row:
                if updated_at is None or row.updated_at is None or str(row.updated_at) <= str(updated_at):
                    for key, val in dic_data.items():
                        setattr(row, key, val)
            else:
                insert_rows.append(dic_data)
        if insert_rows:
            session.execute(cls.__table__.insert(), insert_rows)

        return date_times

    @classmethod
    def is_row_exist(cls, row: Dict):
        query_conds = [getattr(cls, col) == row[col] for col in cls.get_column_names() if col in row]
        data = cls.query.filter(*query_conds).first()
        return bool(data), data

    @classmethod
    def get_original_table_name(cls):
        return cls.__tablename__

    @classmethod
    def get_all_count_record(cls, session=None):
        query = session.query(cls) if session else cls.query
        rows = db.session.execute(query).fetchall()
        return len(rows)


class ConfigDBModel(CommonDBModel):  # todo use BaseModel (inherit)
    __abstract__ = True

    # todo use BaseModel
    @classmethod
    def get_foreign_id_column_name(cls) -> str:  # only use for cfg_ and m_
        """
        m_line  ->  line_id

        :return:
        """
        elems = cls.__tablename__.split('_')
        return f"{'_'.join(elems[1:])}_id"

    @classmethod
    def get_all_as_df(cls, session=None):  # todo use BaseModel
        query = session.query(cls) if session else cls.query
        cols = query.statement.columns.keys()
        rows = db.session.execute(query).fetchall()
        df = pd.DataFrame(rows, columns=cols)

        df = df.convert_dtypes()
        for col in cls.__table__.columns:
            if col.type in (db.Integer, db.BigInteger):
                df[col.name] = df[col.name].astype('Int64')  # all NULL  column
            if col.type in (DataType.REAL,):
                df[col.name] = df[col.name].astype('Float64')  # all NULL  column

        if 'id' in df.columns:
            # In Bridge Station system, all models should have same behavior to publish itseft id column
            df.rename(columns={'id': cls.get_foreign_id_column_name()}, inplace=True)

        return df


class MasterDBModel(CommonDBModel):
    __abstract__ = True

    @classmethod
    def get_all_master(cls):
        return cls.query.all()

    @classmethod
    def get_column_by_name_like(cls, like_compare_str):
        return [column for column in cls.__table__.columns if like_compare_str in column.name]

    @classmethod
    def get_default_name_column(cls) -> str:
        """
        m_line  ->  line_name

        :return:
        """
        elems = cls.__tablename__.split('_')
        return f"{'_'.join(elems[1:])}_name"

    @classmethod
    def get_factid_name_columns(cls) -> list:
        columns = cls.get_column_by_name_like('_factid')
        return [col.name for col in columns] if columns else []

    @classmethod
    def get_jp_name_column(cls) -> str:
        columns = cls.get_column_by_name_like('_name_jp')
        return columns[0].name if columns else None

    @classmethod
    def get_jp_abbr_column(cls) -> str:
        columns = cls.get_column_by_name_like('_abbr_jp')
        return columns[0].name if columns else None

    def get_jp_name(self) -> str:
        jp_name_column = self.get_jp_name_column()
        return getattr(self, jp_name_column) if hasattr(self, jp_name_column) else None

    @classmethod
    def get_en_name_column(cls) -> str:
        columns = cls.get_column_by_name_like('_name_en')
        return columns[0].name if columns else None

    @classmethod
    def get_en_abbr_column(cls) -> str:
        columns = cls.get_column_by_name_like('_abbr_en')
        return columns[0].name if columns else None

    def get_en_name(self) -> str:
        en_name_column = self.get_en_name_column()
        return getattr(self, en_name_column) if hasattr(self, en_name_column) else None

    @classmethod
    def get_sys_name_column(cls) -> str:
        columns = cls.get_column_by_name_like('_name_sys')
        return columns[0].name if columns else None

    @classmethod
    def get_sys_abbr_column(cls) -> str:
        columns = cls.get_column_by_name_like('_abbr_sys')
        return columns[0].name if columns else None

    def get_sys_name(self) -> str:
        """
        Return name that contains ascii character only.
        If model class has no name_sys column, return None

        :return:
        """
        sys_name_column = self.get_sys_name_column()
        return getattr(self, sys_name_column) if hasattr(self, sys_name_column) else None

    @classmethod
    def get_name_all_column(cls) -> str:
        return f"{cls.get_default_name_column().replace('_group', '').replace('_type', '')}_all"

    @classmethod
    def get_abbr_name_column(cls) -> str:
        columns = cls.get_column_by_name_like('_abbr')
        return columns[0].name if columns else None

    @classmethod
    def get_sign_n_no_column(cls) -> list[Any]:
        # m_xxx get xxx only
        short_table_name = cls.get_table_name()[2:]
        columns = cls.get_column_by_name_like(f'{short_table_name}_sign') + cls.get_column_by_name_like(
            f'{short_table_name}_no',
        )
        if not columns:
            return cls.get_abbr_columns()

        return [col.name for col in columns]

    # def get_abbr_name(self) -> str:
    #     abbr_name_column = self.get_abbr_name_column()
    #     return getattr(self, abbr_name_column) if hasattr(self, abbr_name_column) else None

    @classmethod
    def get_local_name_column(cls) -> str:
        columns = cls.get_column_by_name_like('_name_local')
        return columns[0].name if columns else None

    def get_local_name(self) -> str:
        local_name_column = self.get_local_name_column()
        return getattr(self, local_name_column) if hasattr(self, local_name_column) else None

    @classmethod
    def get_local_abbr_column(cls) -> str:
        columns = cls.get_column_by_name_like('_abbr_local')
        return columns[0].name if columns else None

    def get_local_abbr(self) -> str:
        local_abbr_column = self.get_local_abbr_column()
        return getattr(self, local_abbr_column) if hasattr(self, local_abbr_column) else None

    @classmethod
    def get_name_columns(cls):
        columns = [
            cls.get_jp_name_column(),
            cls.get_en_name_column(),
            cls.get_local_name_column(),
        ]
        origin_col = cls.get_column_names()
        return [col for col in columns if col and col in origin_col]

    @classmethod
    def get_group_column_names(cls) -> list:
        columns = cls.get_column_by_name_like('_group')
        return [columns[0].name] if columns else []

    @classmethod
    def get_all_name_columns(cls):
        # columns = [cls.get_abbr_name_column(), cls.get_jp_name_column(), cls.get_en_name_column(),
        columns = [
            cls.get_jp_name_column(),
            cls.get_en_name_column(),
            cls.get_local_name_column(),
            cls.get_sys_name_column(),
        ]  # order is daiji

        # don't use intersection method. This will shuffle order
        # intersection(columns, cls.__table__.columns.get_column_names())
        origin_col = cls.get_column_names()
        return [col for col in columns if col and col in origin_col]

    @classmethod
    def get_abbr_columns(cls):
        columns = [
            cls.get_jp_abbr_column(),
            cls.get_en_abbr_column(),
            cls.get_local_abbr_column(),
        ]
        origin_col = cls.get_column_names()
        return [col for col in columns if col and col in origin_col]

    @classmethod
    def pick_column_by_language(cls, lang, mode=None):
        """

        :param lang: 'en' 'ja' 'vi' ...
        :param lang: 'I' if input (insert to db), 'O' if output (get from db)
        :return:
        """
        dict_lang_and_column = {
            'ja': cls.get_jp_abbr_column(),
            'jp': cls.get_jp_abbr_column(),
            'en': cls.get_en_abbr_column(),
        }
        # not_found_case_column = cls.get_local_name_column() if mode == 'I' else cls.get_sys_name_column()
        not_found_case_column = cls.get_sys_name_column()  # temp
        col = dict_lang_and_column.get(lang)
        return col if col else not_found_case_column

    def get_name(self):  # NOT A CLASS METHOD, support object created by constructor
        all_name_columns = self.get_all_name_columns()
        dict_val = self.__dict__
        for col in all_name_columns:
            val = dict_val.get(col)
            if val:
                return val
        return None

    @classmethod
    def get_sign_column(cls):
        raise Exception

    @classmethod
    def get_default_sign_value(cls):
        raise Exception


class SemiMasterDBModel(CommonDBModel):
    __abstract__ = True

    @classmethod
    def insert_records(cls, columns, rows, session):
        return super().insert_records(columns, rows, session, is_all_insert=True)


class MappingDBModel(CommonDBModel):
    __abstract__ = True
    __is_mapping_table__ = True


class OthersDBModel(CommonDBModel):
    __abstract__ = True


@contextmanager
def make_session(session: Union[scoped_session, Session] = None, is_new_session: bool = False):
    if session is None:
        session = Session(bind=db.engine) if is_new_session else db.session
    try:
        yield session
        session.commit()
    except Exception as e:
        session.rollback()
        raise e


class JobManagement(OthersDBModel):  # TODO change to new modal and edit job
    # __bind_key__ = 'app_metadata'
    __tablename__ = 't_job_management'
    __table_args__ = {'sqlite_autoincrement': True}

    id = db.Column(db.Integer(), primary_key=True, autoincrement=True)

    job_type = db.Column(db.Text())
    data_source_id = db.Column(db.Integer())
    data_table_id = db.Column(db.Integer())
    # TODO: remove
    db_code = db.Column(db.Text())
    # TODO: remove
    db_name = db.Column(db.Text())
    # TODO: change data_table_id
    process_id = db.Column(db.Integer())
    # TODO: remove
    process_name = db.Column(db.Text())

    start_tm = db.Column(db_timestamp(), default=get_current_timestamp)
    end_tm = db.Column(db_timestamp())
    status = db.Column(db.Text())
    done_percent = db.Column(db.Float(), default=0)
    duration = db.Column(db.Float(), default=0)
    error_msg = db.Column(db.Text())

    created_at = db.Column(db_timestamp(), default=get_current_timestamp)
    updated_at = db.Column(db_timestamp(), default=get_current_timestamp, onupdate=get_current_timestamp)

    @classmethod
    def delete(cls, session, id):
        session.query.filter(cls.id == id).delete()

    @classmethod
    def get_processing_jobs(cls, session):
        # rows = session.query(cls.job_type, cls.status).filter(
        #     cls.status == JobStatus.PROCESSING.name).with_for_update().all()
        rows = session.query(cls.job_type, cls.status).filter(cls.status == JobStatus.PROCESSING.name).all()
        return rows

    @classmethod
    def update_processing_to_failed(cls):
        with make_session() as session:
            recs = session.query(cls).filter(cls.status == JobStatus.PROCESSING.name).all()
            for rec in recs:
                rec.status = JobStatus.FATAL.name

    @classmethod
    def check_new_jobs(cls, from_job_id, target_job_types):
        out = cls.query.options(load_only(cls.id))
        return out.filter(cls.id > from_job_id).filter(cls.job_type.in_(target_job_types)).first()

    @classmethod
    def get_last_job_id_by_job_type(cls, job_type, data_table_id=None):
        out = cls.query.options(load_only(cls.id))
        out = out.filter(cls.job_type == job_type)
        if data_table_id:
            out = out.filter(cls.data_table_id == data_table_id)
        out = out.order_by(cls.id.desc()).first()
        return out

    @classmethod
    def get_last_job_of_process(cls, proc_id, job_type):
        out = cls.query.options(load_only(cls.id))
        return out.filter(cls.process_id == proc_id).filter(cls.job_type == job_type).order_by(cls.id.desc()).first()

    @classmethod
    def job_sorts(cls, order_method=''):
        sort = desc
        if order_method == 'asc':
            sort = asc
        return {
            'job_id': sort(cls.id),
            'job_name': sort(cls.job_type),
            'db_master_name': sort(cls.db_name),
            'process_master_name': sort(cls.process_name),
            'start_tm': sort(cls.start_tm),
            'duration': sort(cls.duration),
            'progress': sort(cls.done_percent),
            'status': sort(cls.status),
            'detail': sort(cls.error_msg),
        }

    @classmethod
    def get_all_done_job_of_process(cls, proc_id, job_type):
        return (
            cls.query.filter(cls.process_id == proc_id)
            .filter(and_(cls.job_type == job_type, cls.status == str(JobStatus.DONE)))
            .order_by(cls.id.desc())
            .all()
        )

    @classmethod
    def get_job_of_data_table(cls, data_table_id, job_type):
        out = (
            cls.query.filter(
                cls.data_table_id == data_table_id,
                cls.job_type == job_type,
                cls.status.in_(JobStatus.DONE.name, JobStatus.FAILED.name),
            )
            .order_by(cls.id.desc())
            .all()
        )

        return out

    @classmethod
    def get_error_jobs(cls, job_id):
        infos = cls.query.filter(cls.id == job_id).filter(cls.status != str(JobStatus.DONE))
        return infos.all()

    def as_dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}

    @classmethod
    def get_by_id(cls, job_id):
        return cls.query.filter(cls.id == job_id).first()

    @classmethod
    def update_interrupt_jobs(cls):
        with make_session() as meta_session:
            meta_session.query(cls).filter(cls.status == JobStatus.PROCESSING.name).update(
                {cls.status: JobStatus.KILLED.name},
            )


# Index('ix_test', JobManagement.status, postgresql_where=(JobManagement.status == 'PROCESSING'))


class CsvImport(OthersDBModel):
    # __bind_key__ = 'app_metadata'
    __tablename__ = 't_csv_import'
    __table_args__ = {'sqlite_autoincrement': True}

    id = db.Column(db.Integer(), primary_key=True, autoincrement=True)
    # job_id = db.Column(db.Integer(), db.ForeignKey('t_job_management.id'), index=True)
    job_id = db.Column(db.Integer(), index=True)

    data_table_id = db.Column(db.Integer())
    # TODO: remove
    process_id = db.Column(db.Integer())
    file_name = db.Column(db.Text())

    start_tm = db.Column(db_timestamp(), default=get_current_timestamp)
    end_tm = db.Column(db_timestamp())
    imported_row = db.Column(db.Integer(), default=0)
    status = db.Column(db.Text())
    error_msg = db.Column(db.Text())

    created_at = db.Column(db_timestamp(), default=get_current_timestamp)
    updated_at = db.Column(db_timestamp(), default=get_current_timestamp, onupdate=get_current_timestamp)

    @classmethod
    def get_last_job_id(cls, data_table_id):
        max_job = cls.query.filter(cls.data_table_id == str(data_table_id)).with_entities(
            func.max(cls.job_id).label('job_id'),
        )
        return max_job

    @classmethod
    def get_last_fatal_import(cls, data_table_id):
        status_list = [JobStatus.FATAL.name, JobStatus.PROCESSING.name]
        max_job = cls.get_last_job_id(data_table_id).subquery()
        csv_imports = cls.query.filter(cls.job_id == max_job.c.job_id, cls.status.in_(status_list))
        csv_imports = csv_imports.order_by(cls.id).all()

        return csv_imports

    @classmethod
    def get_by_job_id(cls, job_id):
        csv_imports = cls.query.filter(cls.job_id == job_id)
        return csv_imports.all()

    @classmethod
    def get_error_jobs(cls, job_id):
        csv_imports = cls.query.filter(cls.job_id == job_id).filter(cls.status != str(JobStatus.DONE))
        return csv_imports.all()

    @classmethod
    def get_latest_done_files(cls, data_table_id):
        status_list = [JobStatus.DONE.name, JobStatus.FAILED.name]
        csv_files = cls.query.filter(cls.data_table_id == data_table_id, cls.status.in_(status_list))

        csv_files = csv_files.with_entities(
            cls.file_name,
            func.max(cls.start_tm).label(cls.start_tm.key),
            func.max(cls.imported_row).label(cls.imported_row.key),
        )

        csv_files = csv_files.group_by(cls.file_name).all()

        return csv_files


class ProcLinkCount(db.Model):
    # __bind_key__ = 'app_metadata'
    __tablename__ = 't_proc_link_count'

    # id = db.Column(db.Integer(), primary_key=True, autoincrement=True)
    process_id = db.Column(db.Integer(), primary_key=True)
    target_process_id = db.Column(db.Integer(), primary_key=True)
    matched_count = db.Column(db.Integer(), default=0)
    job_id = db.Column(db.Integer(), db.ForeignKey('t_job_management.id'), index=True)
    created_at = db.Column(db_timestamp(), default=get_current_timestamp)
    updated_at = db.Column(db_timestamp(), default=get_current_timestamp, onupdate=get_current_timestamp)
    # TODO: add migration
    # process = db.relationship('CfgProcess', foreign_keys=[process_id], lazy='joined')
    # target_process = db.relationship('CfgProcess', foreign_keys=[target_process_id], lazy='joined')

    @classmethod
    def get_all(cls):
        return cls.query.all()

    @classmethod
    def delete_all(cls, meta_session=None):
        """delete all records"""
        # cls.query.delete()
        if meta_session is not None:
            meta_session.query(cls).delete()
        else:
            with make_session() as meta_session:
                meta_session.query(cls).delete()

    @classmethod
    def calc_proc_link(cls):
        with make_session() as meta_session:
            output = meta_session.query(
                cls.process_id,
                cls.target_process_id,
                func.sum(cls.matched_count).label(cls.matched_count.key),
            )
            output = output.group_by(cls.process_id, cls.target_process_id).all()

        return output

    @classmethod
    def get_job_ids_by_proc(cls, self_proc_id, target_proc_id):
        output = db.session.query(func.max(cls.job_id))
        output = output.filter(cls.process_id == self_proc_id, cls.target_process_id == target_proc_id).scalar()
        return output

    @classmethod
    def delete_by_process_id(cls, self_process_id, target_process_id, session):
        """

        :param self_process_id:
        :param target_process_id:
        :param session:
        :return:
        """
        output = session.query(cls).filter(cls.process_id == self_process_id)
        output = output.filter(cls.target_process_id == target_process_id)
        output.delete(synchronize_session='fetch')

        return True

    @classmethod
    def insert_records(cls, cols, rows, session):
        """
        insert records
        :param cols:
        :param rows:
        :param session:
        :return:
        """
        if not rows:
            return

        valid_cols = cls.get_column_names()
        insert_rows = [cls.gen_valid_dict(dict(zip(cols, row)), valid_cols) for row in rows]
        session.execute(cls.__table__.insert(), insert_rows)

        return True


class CfgConstant(db.Model):
    # __bind_key__ = 'app_metadata'
    __tablename__ = 'cfg_constant'
    __table_args__ = {'sqlite_autoincrement': True}

    id = db.Column(db.Integer(), primary_key=True, autoincrement=True)
    type = db.Column(db.Text())
    name = db.Column(db.Text())
    value = db.Column(db.Text())

    created_at = db.Column(db_timestamp(), default=get_current_timestamp)
    updated_at = db.Column(db_timestamp(), default=get_current_timestamp, onupdate=get_current_timestamp)

    @classmethod
    def get_value_by_type_first(cls, type, parse_val=None):
        output = cls.query.options(load_only(cls.value)).filter(cls.type == type).first()
        if not output:
            return None

        if parse_val:
            try:
                return parse_val(output.value)
            except Exception:
                return None
        else:
            return output.value

    @classmethod
    def get_value_by_type_name(cls, const_type, name, parse_val=None):
        output = (
            cls.query.options(load_only(cls.value)).filter(cls.type == str(const_type), cls.name == str(name)).first()
        )
        if not output:
            return None

        if parse_val:
            try:
                return parse_val(output.value)
            except Exception:
                return None
        else:
            return output.value

    @classmethod
    def get_value_by_type_names(cls, const_type, names, parse_val=None):
        output = (
            cls.query.options(load_only(cls.name, cls.value)).filter(cls.type == const_type, cls.name.in_(names)).all()
        )
        return output

    @classmethod
    def get_names_values_by_type(cls, const_type):
        output = cls.query.options(load_only(cls.name, cls.value)).filter(cls.type == const_type).all()
        return output

    @classmethod
    def force_running_job(cls):
        output = (
            cls.get_value_by_type_name(
                CfgConstantType.BREAK_JOB.name,
                CfgConstantType.BREAK_JOB.name,
                lambda x: bool(eval(x)),
            )
            or False
        )
        if output:
            logger.debug('[JobBreakException] BREAK JOB')
            raise JobBreakException

    @classmethod
    def create_or_update_by_type(cls, session, const_type=None, const_value=None, const_name=None):
        is_insert = False
        const_name = str(const_name)  # weak ref, back to origin when go out stack
        const_type = str(const_type)  # weak ref, back to origin when go out stack
        constant = None
        if const_type:
            constant = session.query(cls).filter(cls.type == const_type)

            if const_name:
                constant = constant.filter(cls.name == const_name)

            constant = constant.first()

        if not constant:
            constant = cls(type=const_type, value=const_value, name=const_name)
            session.add(constant)
            is_insert = True
        else:
            constant.value = const_value

        return constant, is_insert

    @classmethod
    def create_or_merge_by_type(cls, const_type=None, const_name=None, const_value=0):
        with make_session() as meta_session:
            constant = meta_session.query(cls).filter(cls.type == const_type, cls.name == const_name).first()
            if not constant:
                constant = cls(type=const_type, name=const_name, value=json_dumps(const_value))
                meta_session.add(constant)
            else:
                # merge new order to old orders
                dic_value = json.loads(constant.value)
                dic_latest_orders = dict_deep_merge(const_value, dic_value)
                constant.value = json_dumps(dic_latest_orders)

    @classmethod
    def get_efa_header_flag(cls, data_source_id):
        efa_header_flag = cls.query.filter(
            cls.type == CfgConstantType.EFA_HEADER_EXISTS.name,
            cls.name == data_source_id,
        ).first()

        if efa_header_flag and efa_header_flag.value and efa_header_flag.value == EFA_HEADER_FLAG:
            return True
        return False

    @classmethod
    def get_warning_disk_usage(cls) -> int:
        return cls.get_value_by_type_name(
            CfgConstantType.DISK_USAGE_CONFIG.name,
            DiskUsageStatus.Warning.name,
            parse_val=int,
        )

    @classmethod
    def get_error_disk_usage(cls) -> int:
        return cls.get_value_by_type_name(
            CfgConstantType.DISK_USAGE_CONFIG.name,
            DiskUsageStatus.Full.name,
            parse_val=int,
        )

    @classmethod
    def initialize_disk_usage_limit(cls):
        """
        Sets default disk usage limit constants.
            - Warning: 80% (No terminate jobs)
            - Error: 90% (Terminate jobs)

        :return:
        """
        constants_type = CfgConstantType.DISK_USAGE_CONFIG.name
        warning_percent = cls.get_warning_disk_usage()
        if not warning_percent:  # insert of not existing
            warning_percent = DEFAULT_WARNING_DISK_USAGE
            with make_session() as session:
                cls.create_or_update_by_type(
                    session,
                    constants_type,
                    warning_percent,
                    const_name=DiskUsageStatus.Warning.name,
                )

        error_percent = cls.get_error_disk_usage()
        if not error_percent:  # insert of not existing
            error_percent = DEFAULT_ERROR_DISK_USAGE

            with make_session() as session:
                cls.create_or_update_by_type(
                    session,
                    constants_type,
                    error_percent,
                    const_name=DiskUsageStatus.Full.name,
                )

    @classmethod
    def initialize_max_graph_constants(cls):
        with make_session() as session:
            for constant in MaxGraphNumber:
                db_constant = CfgConstant.get_value_by_type_first(constant.name, int)
                if not db_constant:
                    cls.create_or_update_by_type(
                        session,
                        constant.name,
                        const_value=max_graph_number[constant.name],
                    )


class CfgDataSource(ConfigDBModel):
    # __bind_key__ = 'app_metadata'
    __tablename__ = 'cfg_data_source'
    __table_args__ = {'sqlite_autoincrement': True}
    id = db.Column(db.Integer(), primary_key=True, autoincrement=True)
    name = db.Column(db.Text())
    type = db.Column(db.Text())
    comment = db.Column(db.Text())
    order = db.Column(db.Integer())
    master_type = db.Column(db.Text())
    is_direct_import = db.Column(db.Boolean(), default=False)
    created_at = db.Column(db_timestamp(), default=get_current_timestamp)
    updated_at = db.Column(db_timestamp(), default=get_current_timestamp, onupdate=get_current_timestamp)

    db_detail = db.relationship(
        'CfgDataSourceDB',
        lazy='subquery',
        backref='cfg_data_source',
        uselist=False,
        cascade='all',
    )
    csv_detail = db.relationship(
        'CfgDataSourceCSV',
        lazy='subquery',
        backref='cfg_data_source',
        uselist=False,
        cascade='all',
    )

    # processes = db.relationship('CfgProcess', lazy="dynamic", cascade='all')
    data_tables = db.relationship('CfgDataTable', lazy='subquery', cascade='all')

    @classmethod
    def delete(cls, meta_session, id):
        meta_session.query.filter(cls.id == id).delete()

    @classmethod
    def get_all(cls):
        # TODO(khanhdq): 206 merge source
        all_ds = cls.query.order_by(cls.order, cls.id.asc()).all()
        return all_ds

    @classmethod
    def get_all_db_source(cls):
        all_ds = cls.query.filter(cls.type != str(CSVExtTypes.CSV.value).upper()).order_by(cls.order).all()
        return all_ds

    @classmethod
    def get_ds(cls, ds_id):
        ds = cls.query.get(ds_id)
        # db_detail: CfgDataSourceDB = ds.db_detail
        # if db_detail and db_detail.hashed:
        #     db_detail.password = decrypt_pwd(db_detail.password)
        return ds

    @classmethod
    def update_order(cls, meta_session, data_source_id, order):
        meta_session.query(cls).filter(cls.id == data_source_id).update({cls.order: order})

    @classmethod
    def get_by_name_and_master_type(cls, meta_session, name, master_type):
        return meta_session.query(cls).filter(cls.master_type == master_type, cls.name == name).first()

    @classmethod
    def get_data_source_efa_and_v2(cls):
        return cls.query.filter(cls.master_type != MasterDBType.OTHERS.name).all()

    @classmethod
    def check_duplicated_name(cls, dbs_name):
        dbs = cls.query.filter(cls.name == dbs_name).all()
        return len(dbs) != 0

    def is_csv_or_v2(self):
        return self.type in [DBType.CSV.name, DBType.V2.name]


class CfgDataSourceDB(ConfigDBModel):
    # __bind_key__ = 'app_metadata'
    __tablename__ = 'cfg_data_source_db'
    __table_args__ = {'sqlite_autoincrement': True}
    id = db.Column(db.Integer(), db.ForeignKey('cfg_data_source.id', ondelete='CASCADE'), primary_key=True)
    host = db.Column(db.Text())
    port = db.Column(db.Integer())
    dbname = db.Column(db.Text())
    schema = db.Column(db.Text())
    username = db.Column(db.Text())
    password = db.Column(db.Text())
    hashed = db.Column(db.Boolean(), default=False)
    use_os_timezone = db.Column(db.Boolean(), default=False)
    master_type = db.Column(db.Text())
    created_at = db.Column(db_timestamp(), default=get_current_timestamp)
    updated_at = db.Column(db_timestamp(), default=get_current_timestamp, onupdate=get_current_timestamp)

    def get_password(self, is_get_decrypted):
        if is_get_decrypted and self.hashed and self.password not in ('', None):
            return decrypt_pwd(self.password)
        return self.password

    # @classmethod
    # def save(cls, form: DataSourceDbForm):
    #     if form.id:
    #         row = cls()
    #     else:
    #         row = cls.query.filter(cls.id == form.id)
    #
    #     # create dataSource ins
    #     # TODO pwd encrypt
    #     form.populate_obj(row)
    #
    #     return row

    @classmethod
    def delete(cls, id):
        cls.query.filter(cls.id == id).delete()


class CfgDataSourceCSV(ConfigDBModel):
    # __bind_key__ = 'app_metadata'
    __tablename__ = 'cfg_data_source_csv'
    __table_args__ = {'sqlite_autoincrement': True}
    id = db.Column(db.Integer(), db.ForeignKey('cfg_data_source.id', ondelete='CASCADE'), primary_key=True)
    directory = db.Column(db.Text())
    second_directory = db.Column(db.Text())
    skip_head = db.Column(db.Integer(), nullable=True)
    skip_tail = db.Column(db.Integer(), nullable=True)
    n_rows = db.Column(db.Integer(), nullable=True)
    is_transpose = db.Column(db.Boolean(), nullable=True)
    delimiter = db.Column(db.Text(), default=CsvDelimiter.CSV.name)
    etl_func = db.Column(db.Text())
    dummy_header = db.Column(db.Boolean(), default=False)  # ap.v4.6.0.222
    is_file_path = db.Column(db.Boolean(), nullable=True, default=False)
    created_at = db.Column(db_timestamp(), default=get_current_timestamp)
    updated_at = db.Column(db_timestamp(), default=get_current_timestamp, onupdate=get_current_timestamp)

    # TODO check fetch all
    csv_columns = db.relationship('CfgCsvColumn', backref='cfg_data_source_csv', lazy='subquery', cascade='all')

    def get_column_names_with_sorted(self, key='id'):
        """
        get column names that sorted by key
        :param key:
        :return:
        """
        self.csv_columns.sort(key=lambda csv_column: getattr(csv_column, key))
        return [col.column_name for col in self.csv_columns]

    @classmethod
    def save(cls, form: DataSourceCsvForm):
        row = cls() if form.id else cls.query.filter(cls.id == form.id)

        # create dataSource ins
        form.populate_obj(row)

        return row

    @classmethod
    def delete(cls, id):
        cls.query.filter(cls.id == id).delete()


class CfgProcessUnusedColumn(db.Model):
    __bind_key__ = 'app_metadata'
    __tablename__ = 'cfg_process_unused_column'
    __table_args__ = {'sqlite_autoincrement': True}
    id = db.Column(db.Integer(), primary_key=True, autoincrement=True)
    process_id = db.Column(db.Integer(), db.ForeignKey('cfg_process.id', ondelete='CASCADE'))
    column_name = db.Column(db.Text())

    created_at = db.Column(db.Text(), default=get_current_timestamp)
    updated_at = db.Column(db.Text(), default=get_current_timestamp, onupdate=get_current_timestamp)

    @classmethod
    def get_all_unused_columns_by_process_id(cls, process_id):
        return [col.column_name for col in cls.query.filter(cls.process_id == process_id).all()]

    @classmethod
    def delete_all_columns_by_proc_id(cls, proc_id):
        with make_session() as meta_session:
            meta_session.query(cls).filter(cls.process_id == proc_id).delete()
            meta_session.commit()


class CfgDataTable(ConfigDBModel):
    """
    Since v4.0, this table replaces CfgProcess.
    CfgDataTable use for supports query data from data source.
    CfgProcess supports show data on GUI.

    """

    __tablename__ = 'cfg_data_table'
    __table_args__ = {'sqlite_autoincrement': True}

    id = db.Column(db.Integer(), primary_key=True, autoincrement=True)

    name = db.Column(db.Text())
    data_source_id = db.Column(db.Integer(), db.ForeignKey('cfg_data_source.id', ondelete='CASCADE'))
    partition_from = db.Column(db.Text())
    partition_to = db.Column(db.Text())
    table_name = db.Column(db.Text())
    detail_master_type = db.Column(db.Text())
    comment = db.Column(db.Text())

    order = db.Column(db.Integer())
    skip_merge = db.Column(db.Boolean(), default=False)

    data_source: CfgDataSource = db.relationship('CfgDataSource', lazy='select')

    created_at = db.Column(db_timestamp(), default=get_current_timestamp)
    updated_at = db.Column(db_timestamp(), default=get_current_timestamp, onupdate=get_current_timestamp)

    columns = db.relationship('CfgDataTableColumn', lazy='joined', backref='cfg_data_table', cascade='all')
    partition_tables = db.relationship('CfgPartitionTable', lazy='joined', backref='cfg_data_table', cascade='all')

    def as_dict(self, is_add_data_source=False):
        dic = {c.name: getattr(self, c.name) for c in self.__table__.columns}
        if is_add_data_source:
            dic['data_source'] = getattr(self, 'data_source').as_dict()
        return dic

    @classmethod
    def get_basic_config(cls, data_table_id):
        cfg_data_table: CfgDataTable = cls.get_by_id(data_table_id)
        # sort column : serials first, auto_increment second, then others
        cfg_data_table.get_sorted_columns()
        return cfg_data_table

    @classmethod
    def get_by_process_id(cls, process_id: int, session=None) -> list[dict]:
        session = session if session else db.session
        sql = f'''
SELECT DISTINCT
    cds.{CfgDataTable.id.name}
    , cds.{CfgDataTable.name.name}
    , cds.{CfgDataTable.data_source_id.name}
    , cds.{CfgDataTable.partition_from.name}
    , cds.{CfgDataTable.partition_to.name}
    , cds.{CfgDataTable.table_name.name}
    , cds.{CfgDataTable.comment.name}
    , cds.{CfgDataTable.order.name}
    , cds.{CfgDataTable.created_at.name}
    , cds.{CfgDataTable.updated_at.name}
FROM {CfgDataTable.get_table_name()} cds
INNER JOIN {MappingFactoryMachine.get_table_name()} mdm ON
    mdm.{MappingFactoryMachine.data_table_id.name} = cds.{CfgDataTable.id.name}
INNER JOIN {RFactoryMachine.get_table_name()} rfm ON
    rfm.{RFactoryMachine.id.name} = mdm.{MappingFactoryMachine.factory_machine_id.name}
WHERE
    rfm.{RFactoryMachine.process_id.name} = :1
        '''
        params = {'1': process_id}
        rows = session.execute(sql, params=params)
        cols = [
            CfgDataTable.id.name,
            CfgDataTable.name.name,
            CfgDataTable.data_source_id.name,
            CfgDataTable.partition_from.name,
            CfgDataTable.partition_to.name,
            CfgDataTable.table_name.name,
            CfgDataTable.comment.name,
            CfgDataTable.order.name,
            CfgDataTable.created_at.name,
            CfgDataTable.updated_at.name,
        ]
        return [dict(zip(cols, row)) for row in rows]

    def get_min_max_time(
        self,
    ) -> Union[tuple[datetime.datetime, datetime.datetime, str, str], tuple[None, None, None, None]]:
        times = []
        origin_times = []
        for cfg_partition in self.partition_tables:
            if cfg_partition.min_time:
                times.append(convert_time(cfg_partition.min_time, return_string=False, without_timezone=True))
                origin_times.append(cfg_partition.min_time)
            if cfg_partition.max_time:
                times.append(convert_time(cfg_partition.max_time, return_string=False, without_timezone=True))
                origin_times.append(cfg_partition.max_time)

        if times:
            min_time, max_time = min(times), max(times)
            origin_min_time, origin_max_time = (
                origin_times[times.index(min_time)],
                origin_times[times.index(max_time)],
            )
            return min_time, max_time, origin_min_time, origin_max_time

        return None, None, None, None

    # def get_partitions_by_job_done_status(self, job_type: JobType=None):
    #     return sorted([partition for partition in self.partition_tables if partition.job_done == job_type.name],
    #                   key=lambda x: x.partition_time)

    def get_auto_increment_col(self, column_name_only=True):
        """
        get auto increment column
        :param column_name_only:
        :return:
        """
        cols = [col for col in self.columns if col.data_group_type == DataGroupType.AUTO_INCREMENTAL.value]
        if cols:
            if column_name_only:
                return cols[0].column_name

            return cols[0]

        return None

    def get_date_col(self, column_name_only=True):
        """
        get date column
        :param column_name_only:
        :return:
        """
        cols = [col for col in self.columns if col.data_group_type == DataGroupType.DATA_TIME.value]
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
        return self.get_auto_increment_col(column_name_only) or self.get_date_col(column_name_only) or None

    def is_has_auto_increment_col(self):
        return bool(self.get_auto_increment_col_else_get_date())

    def get_partition_for_job(self, job_type: JobType, many=None, session=None):
        job_type = str(job_type)
        done_type = None
        if job_type in CfgPartitionTable.PROGRESS_ORDER:
            idx = CfgPartitionTable.PROGRESS_ORDER.index(job_type)
            done_type = CfgPartitionTable.PROGRESS_ORDER[idx - 1] if idx else None  # nếu idx = 0 thì cũng None
        cfg_partition_table = CfgPartitionTable.get_most_recent_by_type(self.id, done_type, many, session=session)
        return cfg_partition_table

    def get_master_type(self):
        from bridge.services.utils import get_master_type

        return get_master_type(
            self.data_source.master_type,
            table_name=self.table_name,
            column_names=[col.column_name for col in self.columns],
        )

    def is_has_serial_col(self):
        cols = self.columns
        return any(col.data_group_type == DataGroupType.DATA_SERIAL.value for col in cols)

    @classmethod
    def get_all(cls):
        return cls.query.order_by(cls.order).all()

    @classmethod
    def get_by_id(cls, data_table_id, session=None):
        query = session.query(cls) if session else cls.query
        cfg_data_table = query.get(data_table_id)
        return cfg_data_table

    @classmethod
    def get_by_data_source_id(cls, data_source_id):
        return cls.query.filter(cls.data_source_id == data_source_id).first()

    @classmethod
    def get_by_name(cls, name, session=None):
        query = session.query(cls) if session else cls.query
        return query.filter(cls.name == name).first()

    def get_cols_by_data_type(self, data_type: DataType, column_name_only=True):
        """
        get date column
        :param data_type:
        :param column_name_only:
        :return:
        """
        # TODO: use name not value, fix convert timezone
        default_column_names = BaseMasterColumn.get_dummy_column_name()
        if column_name_only:
            cols = [
                col.column_name
                for col in self.columns
                if col.data_type == str(data_type.name) and col.column_name not in default_column_names
            ]
        else:
            cols = [
                col
                for col in self.columns
                if col.data_type == str(data_type.name) and col.column_name not in default_column_names
            ]

        return cols

    def get_cols_by_data_group_type(self, data_group_type: DataGroupType, column_name_only=True):
        """
        get date column
        :param data_group_type:
        :param column_name_only:
        :return:
        """
        if column_name_only:
            cols = [col.column_name for col in self.columns if col.data_group_type == data_group_type.value]
        else:
            cols = [col for col in self.columns if col.data_group_type == data_group_type.value]

        return cols

    def get_sorted_columns(self):
        self.columns.sort(key=lambda c: (str(c.data_type), str(c.column_name)))
        self.columns.sort(key=lambda c: get_column_order(c.data_group_type))
        return self.columns

    @classmethod
    def delete(cls, data_table_id):
        #  just copy from process TODO refactor ?
        with make_session() as meta_session:
            proc = meta_session.query(cls).get(data_table_id)
            if not proc:
                return False

            meta_session.delete(proc)

        return True

    @classmethod
    def update_order(cls, meta_session, data_table_id, order):
        meta_session.query(cls).filter(cls.id == data_table_id).update({cls.order: order})

    def is_export_file(self):
        file_mode = get_file_mode()
        is_direct_import = self.data_source.is_direct_import
        return file_mode and not is_direct_import

    @hybrid_property
    def scan_done(self):
        job = JobManagement.get_last_job_id_by_job_type(job_type=JobType.SCAN_DATA_TYPE.name, data_table_id=self.id)
        return job and job.status == JobStatus.DONE.name

    @hybrid_property
    def mapping_page_enabled(self):
        return self.is_export_file()

    @hybrid_property
    def has_new_master(self):
        from bridge.services.nayose_handler import has_new_master

        return has_new_master(self.id)


class CfgDataTableColumn(ConfigDBModel):
    __tablename__ = 'cfg_data_table_column'
    __table_args__ = {'sqlite_autoincrement': True}

    id = db.Column(db.Integer(), primary_key=True, autoincrement=True)
    data_table_id = db.Column(db.Integer(), db.ForeignKey('cfg_data_table.id', ondelete='CASCADE'))

    # tunghh note: no need column like english_name.
    column_name = db.Column(db.Text())
    english_name = db.Column(db.Text())
    name = db.Column(db.Text())
    data_type = db.Column(db.Text())
    data_group_type = db.Column(db.SmallInteger())  # ref to m_data_group.id, allow null.
    order = db.Column(db.Integer())
    created_at = db.Column(db_timestamp(), default=get_current_timestamp)
    updated_at = db.Column(db_timestamp(), default=get_current_timestamp, onupdate=get_current_timestamp)

    @classmethod
    def get_column_names_by_data_group_types(cls, data_table_id, data_group_types: List[DataGroupType]):
        """
        get date column
        :param data_table_id:
        :param data_group_types:
        :return:
        """
        data_group_type_ids = [key.value for key in data_group_types]
        recs = cls.query.filter(cls.data_table_id == data_table_id, cls.data_group_type.in_(data_group_type_ids)).all()
        dic_recs = {rec.data_group_type: rec.column_name for rec in recs}
        cols = [dic_recs.get(id) for id in data_group_type_ids if id in dic_recs]

        return cols

    @classmethod
    def get_data_group_types_by_column_names(
        cls,
        data_table_id: int,
        column_names: List[str],
        session=None,
    ) -> Dict[str, DataGroupType]:
        """
        Get dictionary of column name & data group type
        :param data_table_id: data table id
        :param column_names: list of column name
        :param session: Meta session
        :return:
        """
        query = session.query(cls) if session else cls.query
        recs = query.filter(cls.data_table_id == data_table_id, cls.column_name.in_(column_names)).all()
        dic_recs: Dict[str, DataGroupType] = {rec.column_name: DataGroupType(rec.data_group_type) for rec in recs}

        return dic_recs

    def as_dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}

    @classmethod
    def get_by_data_table_id(cls, data_table_id):
        return cls.query.filter(cls.data_table_id == data_table_id).all()

    @classmethod
    def get_by_column_names(cls, data_table_id, column_name):
        return cls.query.filter(cls.data_table_id == data_table_id, cls.column_name == column_name).first()

    @classmethod
    def get_by_id(cls, col_id: int):
        return cls.query.get(col_id)

    @classmethod
    def gen_label_from_col_id(cls, col_id: int):
        col = cls.get_by_id(col_id)
        if not col:
            return None
        col_label = gen_sql_label(col.id, col.column_name)
        return col_label

    @classmethod
    def get_order_column(cls, data_table_id):
        return cls.query.filter(
            cls.data_table_id == data_table_id,
            cls.data_group_type == DataGroupType.AUTO_INCREMENTAL.value,
        ).first()

    @classmethod
    def get_split_columns(cls, data_table_id):
        order_col = cls.get_order_column(data_table_id)
        if order_col:
            return [
                DataGroupType.AUTO_INCREMENTAL,
                DataGroupType.PROCESS_NAME,
                DataGroupType.LINE_NAME,
            ]
        else:
            return [DataGroupType.DATA_TIME, DataGroupType.PROCESS_NAME, DataGroupType.LINE_NAME]


class CfgCsvColumn(ConfigDBModel):
    # __bind_key__ = 'app_metadata'
    __tablename__ = 'cfg_csv_column'
    __table_args__ = {'sqlite_autoincrement': True}
    id = db.Column(db.Integer(), primary_key=True, autoincrement=True)
    data_source_id = db.Column(db.Integer(), db.ForeignKey('cfg_data_source_csv.id', ondelete='CASCADE'))
    column_name = db.Column(db.Text())
    data_type = db.Column(db.Text())
    order = db.Column(db.Integer())
    directory_no = db.Column(db.Integer())  # 1: V2, 2: V2 HISTORY
    created_at = db.Column(db_timestamp(), default=get_current_timestamp)
    updated_at = db.Column(db_timestamp(), default=get_current_timestamp, onupdate=get_current_timestamp)


class CfgProcess(ConfigDBModel):
    """
    Since v4.0, this table is only generated, not GUI edit.
    """

    # __bind_key__ = 'app_metadata'
    __tablename__ = 'cfg_process'
    __table_args__ = {'sqlite_autoincrement': True}

    id = db.Column(db.Integer(), primary_key=True, autoincrement=True)
    name = db.Column(db.Text())  # system_name
    name_jp = db.Column(db.Text())
    name_en = db.Column(db.Text())
    name_local = db.Column(db.Text())
    table_name = db.Column(db.Text())
    comment = db.Column(db.Text())
    # is_show_file_name = null => process not imported
    # is_show_file_name = false => imported but unchecked
    # is_show_file_name = true => imported and checked
    is_show_file_name = db.Column(db.Boolean(), default=None)
    datetime_format = db.Column(db.Text(), default=None)
    order = db.Column(db.Integer())
    created_at = db.Column(db_timestamp(), default=get_current_timestamp)
    updated_at = db.Column(db_timestamp(), default=get_current_timestamp, onupdate=get_current_timestamp)

    # TODO check fetch all
    columns: list['CfgProcessColumn'] = db.relationship(
        'CfgProcessColumn',
        lazy='joined',
        backref='cfg_process',
        cascade='all',
    )
    traces = db.relationship(
        'CfgTrace',
        lazy='dynamic',
        foreign_keys='CfgTrace.self_process_id',
        backref='cfg_process',
        cascade='all',
    )
    filters = db.relationship('CfgFilter', lazy='dynamic', backref='cfg_process', cascade='all')
    visualizations = db.relationship('CfgVisualization', lazy='dynamic', backref='cfg_process', cascade='all')

    # data_source = db.relationship('CfgDataSource', lazy='select')

    def as_dict(self, is_add_extend_attributes: bool = False) -> dict:
        dic = super().as_dict()
        if is_add_extend_attributes:
            for extend_attribute_name in ['data_source_name', 'data_table_name']:
                if hasattr(self, extend_attribute_name):
                    dic[extend_attribute_name] = getattr(self, extend_attribute_name)
        return dic

    def get_shown_name(self):
        try:
            locale = get_locale()
            if not self.name_en:
                self.name_en = to_romaji(self.name)
            if not locale:
                return None
            if locale.language == 'ja':
                return self.name_jp if self.name_jp else self.name_en
            else:
                return self.name_local if self.name_local else self.name_en
        except Exception:
            return self.name

    @hybrid_property
    def shown_name(self):
        return self.get_shown_name()

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

    def get_serials(self, column_name_only=True):
        if column_name_only:
            cols = [col.column_name for col in self.columns if col.is_serial_no]
        else:
            cols = [col for col in self.columns if col.is_serial_no]

        return cols

    def get_order_cols(self, column_name_only=True, column_id_only=False):
        cols = [
            col
            for col in self.columns
            if col.is_serial_no
            or col.data_type
            in [
                DataType.DATETIME.name,
                DataType.TEXT.name,
                DataType.INTEGER.name,
                DataType.INTEGER_SEP.name,
                DataType.EU_INTEGER_SEP.name,
                DataType.BIG_INT.name,
            ]
        ]
        if column_name_only:
            cols = [col.column_name for col in cols]

        if column_id_only:
            cols = [col.id for col in cols]

        return cols

    def get_cols(self, col_ids=()):
        cols = [col for col in self.columns if col.id in col_ids]
        return cols

    def get_col(self, col_id):
        cols = [col for col in self.columns if col.id == col_id]
        if cols:
            return cols[0]
        else:
            return None

    def get_cols_by_data_type(self, data_type: DataType, column_name_only=True):
        """
        get date column
        :param data_type:
        :param column_name_only:
        :return:
        """
        if column_name_only:
            cols = [col.column_name for col in self.columns if col.data_type == data_type.value]
        else:
            cols = [col for col in self.columns if col.data_type == data_type.value]

        return cols

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

    @classmethod
    def get_by_name(cls, name):
        return cls.query.filter(cls.name == name).first()

    @classmethod
    def get_all(cls, session=None):
        query = session.query(cls) if session else cls.query
        return query.order_by(cls.order).all()

    @classmethod
    def get_all_ids(cls):
        return cls.query.options(load_only(cls.id)).all()

    @classmethod
    def get_all_order_by_id(cls):
        return cls.query.order_by(cls.id).all()

    @classmethod
    def get_procs(cls, ids, session=None):
        """
        Get cfg_process by in ids
        """
        query = session.query(cls) if session else cls.query
        return query.filter(cls.id.in_(ids)).all()

    @classmethod
    def get_proc_by_id(cls, proc_id, session=None):
        """
        Get cfg_process by in ids
        """
        query = session.query(cls) if session else cls.query
        return query.filter(cls.id == proc_id).first()

    @classmethod
    def save(cls, meta_session, form: ProcessCfgForm):
        if not form.id.data:
            row = cls()
            meta_session.add(row)
        else:
            row = meta_session.query(cls).get(form.id.data)
            # row.columns = [ProcessColumnsForm]
            # for column in columns:
            #     columObj = CfgProcessColumn(**column)
            #     meta_session.add(columObj)
        # form.populate_obj(row)
        meta_session.commit()
        return row

    @classmethod
    def delete(cls, proc_id):
        # TODO refactor
        with make_session() as meta_session:
            proc = meta_session.query(cls).get(proc_id)
            if not proc:
                return False

            meta_session.delete(proc)

            # delete traces manually
            meta_session.query(CfgTrace).filter(
                or_(CfgTrace.self_process_id == proc_id, CfgTrace.target_process_id == proc_id),
            ).delete()

            # delete linking prediction manually
            meta_session.query(ProcLinkCount).filter(
                or_(ProcLinkCount.process_id == proc_id, ProcLinkCount.target_process_id == proc_id),
            ).delete()

            meta_session.commit()

        return True

    @classmethod
    def update_order(cls, meta_session, process_id, order):
        meta_session.query(cls).filter(cls.id == process_id).update({cls.order: order})

    @classmethod
    def get_list_of_process(cls):
        processes = cls.query.order_by(cls.id).all()
        return [{cls.id.name: proc.id, cls.name.name: proc.shown_name} for proc in processes]

    @classmethod
    def check_duplicated_name(cls, name_en, name_jp, name_local):
        check_name_en = len(cls.query.filter(cls.name_en == name_en).all()) != 0 if name_en else False
        check_name_jp = len(cls.query.filter(cls.name_jp == name_jp).all()) != 0 if name_jp else False
        check_name_local = len(cls.query.filter(cls.name_local == name_local).all()) != 0 if name_local else False
        return check_name_en, check_name_jp, check_name_local

    @hybrid_property
    def is_imported(self) -> bool:
        # TODO: might refactor this logic later ...
        # is_show_file_name = null => process not imported
        # is_show_file_name = false => imported but unchecked
        # is_show_file_name = true => imported and checked
        return self.is_show_file_name is not None


class CfgProcessColumn(ConfigDBModel):
    # __bind_key__ = 'app_metadata'
    __tablename__ = 'cfg_process_column'
    __table_args__ = {'sqlite_autoincrement': True}

    id = db.Column(db.Integer(), primary_key=True, autoincrement=True)

    process_id = db.Column(db.Integer(), db.ForeignKey('cfg_process.id', ondelete='CASCADE'))
    column_name = db.Column(db.Text())  # sys_name
    name_en = db.Column(db.Text())
    name_jp = db.Column(db.Text())
    name_local = db.Column(db.Text())
    bridge_column_name = db.Column(db.Text())  # use in Bridge Station
    column_raw_name = db.Column(db.Text(), nullable=False)  # column name in data source
    data_type = db.Column(db.Text())
    raw_data_type = db.Column(db.Text())
    operator = db.Column(db.Text())
    coef = db.Column(db.Text())
    column_type = db.Column(db.Integer())
    is_serial_no = db.Column(db.Boolean(), default=False)
    is_get_date = db.Column(db.Boolean(), default=False)
    is_dummy_datetime = db.Column(db.Boolean(), default=False)
    is_auto_increment = db.Column(db.Boolean(), default=False)
    order = db.Column(db.Integer())
    format = db.Column(db.Text())
    unit = db.Column(db.Text())
    function_details: list['CfgProcessFunctionColumn'] = db.relationship(
        'CfgProcessFunctionColumn',
        lazy='joined',
        backref='cfg_process_column',
        cascade='all',
        order_by='CfgProcessFunctionColumn.order.asc()',
    )

    created_at = db.Column(db_timestamp(), default=get_current_timestamp)
    updated_at = db.Column(db_timestamp(), default=get_current_timestamp, onupdate=get_current_timestamp)

    # This field to sever store function config of this column, it's NOT REAL COLUMN IN TABLE.
    function_config: Optional[dict] = None

    # TODO trace key, cfg_filter: may not needed
    # visualizations = db.relationship('CfgVisualization', lazy='dynamic', backref="cfg_process_column", cascade="all")

    def get_shown_name(self):
        try:
            locale = get_locale()
            if not locale:
                return None
            if locale.language == 'ja':
                return self.name_jp if self.name_jp else self.name_en
            else:
                return self.name_local if self.name_local else self.name_en
        except Exception:
            return self.name_jp

    @hybrid_property
    def shown_name(self):
        return self.get_shown_name()

    @hybrid_property
    def is_linking_column(self):
        return (
            self.raw_data_type
            not in [RawDataTypeDB.REAL.value, RawDataTypeDB.BOOLEAN.value, RawDataTypeDB.CATEGORY.value]
            and self.column_type in DataGroupType.get_column_type_show_graph()
        )

    @classmethod
    def get_by_col_name(cls, proc_id, col_name):
        return cls.query.filter(cls.process_id == proc_id, cls.column_name == col_name).first()

    @classmethod
    def get_by_data_type(cls, proc_id, data_type: DataType):
        return cls.query.filter(cls.process_id == proc_id, cls.data_type == data_type.value).all()

    @classmethod
    def get_by_raw_data_type(cls, proc_id, raw_data_type: RawDataTypeDB):
        return cls.query.filter(cls.process_id == proc_id, cls.raw_data_type == raw_data_type.value).all()

    @classmethod
    def get_by_ids(cls, ids):
        return cls.query.filter(cls.id.in_(ids)).all()

    @classmethod
    def get_by_process_id(cls, process_id: Union[int, str], return_df: bool = False):
        objects = cls.query.filter(cls.process_id == process_id).all()
        if return_df:
            list_dic = [obj.as_dict() for obj in objects]
            df = pd.DataFrame(list_dic, dtype='object').replace({None: DEFAULT_NONE_VALUE}).convert_dtypes()
            return df

        return objects

    @classmethod
    def get_serials(cls, proc_id):
        return cls.query.filter(cls.process_id == proc_id, cls.is_serial_no.is_(True)).all()

    @classmethod
    def get_data_time(cls, proc_id):
        return cls.query.filter(cls.process_id == proc_id, cls.is_get_date.is_(True)).first()

    @classmethod
    def get_dummy_datetime_column(cls, proc_id):
        return cls.query.filter(
            cls.process_id == proc_id,
            cls.is_get_date.is_(True),
            cls.is_dummy_datetime.is_(True),
        ).first()

    @classmethod
    def get_all_columns(cls, proc_id):
        return cls.query.filter(cls.process_id == proc_id).all()

    @classmethod
    def get_by_column_types(cls, column_types: list[int], proc_ids: list[int] = None, session=None) -> list:
        query = session.query(cls) if session else cls.query
        if proc_ids:
            return query.filter(cls.column_type.in_(column_types), cls.process_id.in_(proc_ids)).all()
        return query.filter(cls.column_type.in_(column_types)).all()

    @classmethod
    def gen_label_from_col_id(cls, col_id: int):
        col = cls.get_by_id(col_id)
        if not col:
            return None
        col_label = gen_sql_label(col.id, col.column_name)
        return col_label

    def gen_sql_label(self, is_bridge: bool = False) -> str:
        col_name = self.bridge_column_name if is_bridge else self.column_name
        col_label = gen_sql_label(self.id, col_name)
        return col_label

    def is_master_data_column(self) -> bool:
        return DataGroupType.is_master_data_column(self.column_type) and not len(self.function_details)

    def is_data_source_name_column(self) -> bool:
        return DataGroupType.is_data_source_name(self.column_type)

    def master_data_column_id_label(self, is_bridge: bool = False) -> str:
        sql_label = self.gen_sql_label(is_bridge)
        return f'{sql_label}_MASTER_ID'

    @classmethod
    def get_columns_by_process_id(cls, proc_id):
        columns = cls.query.filter(cls.process_id == proc_id).all()
        return [{cls.id.name: col.id, 'name': col.shown_name, cls.data_type.name: col.data_type} for col in columns]

    @hybrid_property
    def is_function_column(self):
        return len(self.function_details) > 0

    @hybrid_property
    def is_me_function_column(self):
        return self.is_function_column and any(col.is_me_function for col in self.function_details)

    @hybrid_property
    def is_chain_of_me_functions(self):
        return self.is_function_column and all(col.is_me_function for col in self.function_details)

    def existed_in_transaction_table(self):
        # this if master column, hence it does not exist
        if self.is_master_data_column():
            return False

        # this if not function column, hence it exists
        if not self.is_function_column:
            return True

        # this is function column, but it created by a chain of mes
        if self.is_chain_of_me_functions:
            return True

        return False

    @hybrid_property
    def is_category(self):
        return self.data_type == DataType.TEXT.value or self.is_int_category

    @hybrid_property
    def is_int_category(self):
        return self.data_type == DataType.INTEGER.value and self.is_serial_no

    @classmethod
    def get_function_col_ids(cls, process_id):
        recs = (
            cls.query.options(load_only(cls.id))
            .filter(cls.process_id == process_id)
            .filter(cls.column_type == DataGroupType.GENERATED_EQUATION.value)
            .all()
        )
        ids = [rec.id for rec in recs]
        return ids

    @classmethod
    def remove_by_col_ids(cls, ids, session=None):
        query = session.query(cls) if session else cls.query
        query.filter(cls.id.in_(ids)).delete(synchronize_session='fetch')

        return True


class CfgProcessFunctionColumn(ConfigDBModel):
    __tablename__ = 'cfg_process_function_column'
    __table_args__ = {'sqlite_autoincrement': True}

    id = db.Column(db.Integer(), primary_key=True, autoincrement=True)
    process_column_id = db.Column(db.Integer(), db.ForeignKey('cfg_process_column.id', ondelete='CASCADE'))
    function_id = db.Column(db.Integer())
    var_x = db.Column(db.Integer())
    var_y = db.Column(db.Integer())
    a = db.Column(db.Text())
    b = db.Column(db.Text())
    c = db.Column(db.Text())
    n = db.Column(db.Text())
    k = db.Column(db.Text())
    s = db.Column(db.Text())
    t = db.Column(db.Text())
    return_type = db.Column(db.Text())
    note = db.Column(db.Text())
    order = db.Column(db.Integer(), nullable=False)
    created_at = db.Column(db_timestamp(), default=get_current_timestamp)
    updated_at = db.Column(db_timestamp(), default=get_current_timestamp, onupdate=get_current_timestamp)

    @hybrid_property
    def is_me_function(self) -> bool:
        return self.process_column_id in [self.var_x, self.var_y]

    @classmethod
    def get_by_process_id(cls, process_id: int, session=None):
        session = session if session else cls.query.session
        cols = [
            CfgProcessFunctionColumn.id.name,
            CfgProcessFunctionColumn.process_column_id.name,
            CfgProcessFunctionColumn.function_id.name,
            CfgProcessFunctionColumn.var_x.name,
            CfgProcessFunctionColumn.var_y.name,
            CfgProcessFunctionColumn.a.name,
            CfgProcessFunctionColumn.b.name,
            CfgProcessFunctionColumn.c.name,
            CfgProcessFunctionColumn.n.name,
            CfgProcessFunctionColumn.k.name,
            CfgProcessFunctionColumn.s.name,
            CfgProcessFunctionColumn.t.name,
            CfgProcessFunctionColumn.return_type.name,
            CfgProcessFunctionColumn.note.name,
            CfgProcessFunctionColumn.order.name,
            CfgProcessFunctionColumn.created_at.name,
            CfgProcessFunctionColumn.updated_at.name,
        ]
        sql = f'''
SELECT
    pfc.{CfgProcessFunctionColumn.id.name}
    , pfc.{CfgProcessFunctionColumn.process_column_id.name}
    , pfc.{CfgProcessFunctionColumn.function_id.name}
    , pfc.{CfgProcessFunctionColumn.var_x.name}
    , pfc.{CfgProcessFunctionColumn.var_y.name}
    , pfc.{CfgProcessFunctionColumn.a.name}
    , pfc.{CfgProcessFunctionColumn.b.name}
    , pfc.{CfgProcessFunctionColumn.c.name}
    , pfc.{CfgProcessFunctionColumn.n.name}
    , pfc.{CfgProcessFunctionColumn.k.name}
    , pfc.{CfgProcessFunctionColumn.s.name}
    , pfc.{CfgProcessFunctionColumn.t.name}
    , pfc.{CfgProcessFunctionColumn.return_type.name}
    , pfc.{CfgProcessFunctionColumn.note.name}
    , pfc.{CfgProcessFunctionColumn.order.name}
    , pfc.{CfgProcessFunctionColumn.created_at.name}
    , pfc.{CfgProcessFunctionColumn.updated_at.name}
FROM {CfgProcessFunctionColumn.get_table_name()} pfc
INNER JOIN {CfgProcessColumn.get_table_name()} pc ON
    pc.{CfgProcessColumn.id.name} = pfc.{CfgProcessFunctionColumn.process_column_id.name}
WHERE
    pc.{CfgProcessColumn.process_id.name} = :1'''
        params = {'1': process_id}
        rows = session.execute(sql, params=params)
        df = pd.DataFrame(rows, columns=cols)

        df = df.convert_dtypes()
        for col in cls.__table__.columns:
            if col.type in (db.Integer, db.BigInteger):
                df[col.name] = df[col.name].astype('Int64')  # all NULL  column
            if col.type in (DataType.REAL,):
                df[col.name] = df[col.name].astype('Float64')  # all NULL  column

        if 'id' in df.columns:
            # In Bridge Station system, all models should have same behavior to publish itseft id column
            df.rename(columns={'id': cls.get_foreign_id_column_name()}, inplace=True)

        return df

    @classmethod
    def get_all_cfg_col_ids(cls):
        data = cls.query.all()
        cfg_col_ids = []
        for row in data:
            if row.var_x:
                cfg_col_ids.append(row.var_x)

            if row.var_y:
                cfg_col_ids.append(row.var_y)

        return cfg_col_ids

    @classmethod
    def remove_by_col_ids(cls, ids, session=None):
        query = session.query(cls) if session else cls.query
        query.filter(cls.id.in_(ids)).delete(synchronize_session='fetch')

        return True


class CfgPartitionTable(ConfigDBModel):
    PROGRESS_ORDER = [
        JobType.SCAN_MASTER.name,
        JobType.SCAN_DATA_TYPE.name,
        JobType.USER_APPROVED_MASTER.name,
        JobType.PULL_DB_DATA.name,
    ]  # no need TRANSACTION_CSV_IMPORT here

    __tablename__ = 'cfg_partition_table'
    __table_args__ = {'sqlite_autoincrement': True}

    id = db.Column(db.Integer(), primary_key=True, autoincrement=True)
    data_table_id = db.Column(db.Integer(), db.ForeignKey('cfg_data_table.id', ondelete='CASCADE'))
    table_name = db.Column(db.Text())
    partition_time = db.Column(db.Text())
    min_time = db.Column(db.Text())
    max_time = db.Column(db.Text())
    job_done = db.Column(db.Text)  # None -> Scan Master -> Scan data type -> Transaction Import

    created_at = db.Column(db_timestamp(), default=get_current_timestamp)
    updated_at = db.Column(db_timestamp(), default=get_current_timestamp)

    def as_dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}

    def is_existing_by_data_table(self, cfg_data_table_id):
        _query = self.query.options(load_only(self.id)).filter(self.data_table_id == cfg_data_table_id).first()
        return bool(_query) or False

    def is_no_min_max_date_time(self):
        # In case this table is empty (no have any records)
        return self.min_time is None and self.max_time is None

    @classmethod
    def get_max_time_by_data_table(cls, cfg_data_table_id):
        res = db.session.query(func.max(cls.max_time)).filter(cls.data_table_id == cfg_data_table_id).first()
        if res and res[0]:
            return res[0]

        return None

    @classmethod
    def get_min_time_by_data_table(cls, cfg_data_table_id):
        res = db.session.query(func.min(cls.min_time)).filter(cls.data_table_id == cfg_data_table_id).first()
        if res and res[0]:
            return res[0]

        return None

    @classmethod
    def get_by_data_table_id(cls, cfg_data_table_id):
        return (
            cls.query.options(load_only(cls.id))
            .filter(cls.data_table_id == cfg_data_table_id)
            .order_by(cls.max_time.desc())
            .all()
        )

    @classmethod
    def get_most_recent_by_type(cls, cfg_data_table_id, job_type: JobType = None, many=None, session=None):
        job_type = str(job_type) if job_type else sqlalchemy.null()
        query = session.query(cls) if session else cls.query
        output = query.filter(and_(cls.data_table_id == cfg_data_table_id, cls.job_done == job_type))
        output = output.order_by(cls.partition_time.desc())

        if many:
            return output.all()

        return output.first()

    @classmethod
    def delete_not_in_partition_times(cls, data_table_id, partition_times):
        with make_session() as meta_session:
            meta_session.query(cls).filter(
                cls.data_table_id == data_table_id,
                cls.partition_time.notin_(partition_times),
            ).delete(synchronize_session=False)


class CfgTrace(ConfigDBModel):
    # __bind_key__ = 'app_metadata'
    __tablename__ = 'cfg_trace'
    __table_args__ = {'sqlite_autoincrement': True}

    id = db.Column(db.Integer(), primary_key=True, autoincrement=True)
    self_process_id = db.Column(db.Integer(), db.ForeignKey('cfg_process.id', ondelete='CASCADE'))
    target_process_id = db.Column(db.Integer(), db.ForeignKey('cfg_process.id', ondelete='CASCADE'))
    is_trace_backward = db.Column(db.Boolean(), default=False)

    created_at = db.Column(db_timestamp(), default=get_current_timestamp)
    updated_at = db.Column(db_timestamp(), default=get_current_timestamp, onupdate=get_current_timestamp)

    trace_keys: list['CfgTraceKey'] = db.relationship(
        'CfgTraceKey',
        lazy='joined',
        backref='cfg_trace',
        cascade='all',
        order_by='asc(CfgTraceKey.self_column_id)',
    )

    self_process = db.relationship('CfgProcess', foreign_keys=[self_process_id], lazy='joined')
    target_process = db.relationship('CfgProcess', foreign_keys=[target_process_id], lazy='joined')

    @classmethod
    def get_all(cls):
        return cls.query.all()

    def as_dict(self):
        dict_trace = {c.name: getattr(self, c.name) for c in self.__table__.columns}
        dict_trace['trace_keys'] = [trace_key.as_dict() for trace_key in self.trace_keys]
        return dict_trace

    @classmethod
    def get_cols_between_two(cls, proc_id1, proc_id2):
        trace = cls.query.filter(
            or_(
                and_(cls.self_process_id == proc_id1, cls.target_process_id == proc_id2),
                and_(cls.self_process_id == proc_id2, cls.target_process_id == proc_id1),
            ),
        ).first()

        cols = set()
        if trace:
            [cols.update([key.self_column_id, key.target_column_id]) for key in trace.trace_keys]
        return cols

    @classmethod
    def get_traces_of_proc(cls, proc_ids):
        traces = cls.query.filter(or_(cls.self_process_id.in_(proc_ids), cls.target_process_id.in_(proc_ids))).all()
        return traces

    def is_same_tracing(self, other):
        """
        True if same self process id, target process id, self column id list, target column id list.
        :param other:
        :return:
        """
        if not isinstance(other, CfgTrace):
            return False
        if (self.self_process_id, self.target_process_id) != (
            other.self_process_id,
            other.target_process_id,
        ):
            return False
        if len(self.trace_keys) != len(other.trace_keys):
            return False

        keys = [
            [
                key.self_column_id,
                key.self_column_substr_from,
                key.self_column_substr_to,
                key.target_column_id,
                key.target_column_substr_from,
                key.target_column_substr_to,
            ]
            for key in self.trace_keys
        ]
        other_keys = [
            [
                key.self_column_id,
                key.self_column_substr_from,
                key.self_column_substr_to,
                key.target_column_id,
                key.target_column_substr_from,
                key.target_column_substr_to,
            ]
            for key in other.trace_keys
        ]
        cols = [
            'self_column_id',
            'self_column_substr_from',
            'self_column_substr_to',
            'target_column_id',
            'target_column_substr_from',
            'target_column_substr_to',
        ]

        self_trace_key_df = pd.DataFrame(keys, columns=cols)
        other_trace_key_df = pd.DataFrame(other_keys, columns=cols)
        if not self_trace_key_df.equals(other_trace_key_df):
            return False

        return True


class CfgTraceKey(ConfigDBModel):
    # __bind_key__ = 'app_metadata'
    __tablename__ = 'cfg_trace_key'
    __table_args__ = {'sqlite_autoincrement': True}

    id = db.Column(db.Integer(), primary_key=True, autoincrement=True)

    trace_id = db.Column(db.Integer(), db.ForeignKey('cfg_trace.id', ondelete='CASCADE'))
    # TODO confirm PO delete
    self_column_id = db.Column(db.Integer(), db.ForeignKey('cfg_process_column.id', ondelete='CASCADE'))
    self_column_substr_from = db.Column(db.Integer())
    self_column_substr_to = db.Column(db.Integer())

    target_column_id = db.Column(db.Integer(), db.ForeignKey('cfg_process_column.id', ondelete='CASCADE'))
    target_column_substr_from = db.Column(db.Integer())
    target_column_substr_to = db.Column(db.Integer())

    delta_time = db.Column(db.Float())
    cut_off = db.Column(db.Float())

    order = db.Column(db.Integer())

    self_column = db.relationship('CfgProcessColumn', foreign_keys=[self_column_id], lazy='joined')
    target_column = db.relationship('CfgProcessColumn', foreign_keys=[target_column_id], lazy='joined')

    created_at = db.Column(db_timestamp(), default=get_current_timestamp)
    updated_at = db.Column(db_timestamp(), default=get_current_timestamp, onupdate=get_current_timestamp)


class CfgFilter(ConfigDBModel):
    # __bind_key__ = 'app_metadata'
    __tablename__ = 'cfg_filter'
    __table_args__ = {'sqlite_autoincrement': True}

    id = db.Column(db.Integer(), primary_key=True, autoincrement=True)
    process_id = db.Column(db.Integer(), db.ForeignKey('cfg_process.id', ondelete='CASCADE'))
    name = db.Column(db.Text())
    column_id = db.Column(db.Integer(), db.ForeignKey('cfg_process_column.id', ondelete='CASCADE'))  # TODO confirm PO
    filter_type = db.Column(db.Text())
    parent_id = db.Column(
        db.Integer(),
        db.ForeignKey(id, ondelete='CASCADE'),
        nullable=True,
    )  # TODO check if needed to self ref

    created_at = db.Column(db_timestamp(), default=get_current_timestamp)
    updated_at = db.Column(db_timestamp(), default=get_current_timestamp, onupdate=get_current_timestamp)

    parent = db.relationship('CfgFilter', lazy='joined', backref='cfg_children', remote_side=[id], uselist=False)
    column = db.relationship('CfgProcessColumn', lazy='joined', backref='cfg_filters', uselist=False)
    filter_details = db.relationship('CfgFilterDetail', lazy='joined', backref='cfg_filter', cascade='all')

    @classmethod
    def delete_by_id(cls, meta_session, filter_id):
        cfg_filter = meta_session.query(cls).get(filter_id)
        if cfg_filter:
            meta_session.delete(cfg_filter)

    @classmethod
    def get_filters(cls, ids):
        return cls.query.filter(cls.id.in_(ids))

    @classmethod
    def get_filter_by_col_id(cls, column_id):
        return cls.query.filter(cls.column_id == column_id).first()

    @classmethod
    def get_by_proc_n_col_ids(cls, proc_ids, column_ids, filter_types=None, session=None):
        query = session.query(cls) if session else cls.query
        query = query.filter(cls.process_id.in_(proc_ids), cls.column_id.in_(column_ids))
        if filter_types:
            query.filter(cls.filter_type.in_(filter_types))

        return query.all()

    @classmethod
    def get_by_col_name_and_column_id(cls, proc_id, filter_type, column_id):
        return cls.query.filter(
            cls.process_id == proc_id,
            cls.filter_type == filter_type,
            cls.column_id == column_id,
        ).first()


class CfgFilterDetail(ConfigDBModel):
    # __bind_key__ = 'app_metadata'
    __tablename__ = 'cfg_filter_detail'
    __table_args__ = {'sqlite_autoincrement': True}

    id = db.Column(db.Integer(), primary_key=True, autoincrement=True)
    filter_id = db.Column(db.Integer(), db.ForeignKey('cfg_filter.id', ondelete='CASCADE'))
    parent_detail_id = db.Column(db.Integer(), db.ForeignKey(id, ondelete='CASCADE'))
    name = db.Column(db.Text())
    filter_condition = db.Column(db.Text())
    filter_function = db.Column(db.Text())
    filter_from_pos = db.Column(db.Integer())
    order = db.Column(db.Integer())
    parent = db.relationship('CfgFilterDetail', lazy='joined', backref='cfg_children', remote_side=[id], uselist=False)
    created_at = db.Column(db_timestamp(), default=get_current_timestamp)
    updated_at = db.Column(db_timestamp(), default=get_current_timestamp, onupdate=get_current_timestamp)

    def update_by_dict(self, **kwargs):
        for key, value in kwargs.items():
            if hasattr(self, key):
                setattr(self, key, value)

    @classmethod
    def get_all(cls, session=None):
        query = session.query(cls) if session else cls.query
        return query.all()

    @classmethod
    def get_filters(cls, ids):
        return cls.query.filter(cls.id.in_(ids))

    @classmethod
    def update_order(cls, meta_session, process_id, order):
        meta_session.query(cls).filter(cls.id == process_id).update({cls.order: order})

    @classmethod
    def get_by_condition_and_function(cls, filter_id, filter_condition, filter_function):
        return cls.query.filter(
            cls.filter_id == filter_id,
            cls.filter_condition == filter_condition,
            cls.filter_function == filter_function,
        ).first()

    def __eq__(self, other):
        return (
            isinstance(other, self.__class__)
            and getattr(other, self.id.key, None)
            and self.id
            and getattr(other, self.id.key, None) == self.id
        )

    def __hash__(self):
        return hash(str(self.id))


class CfgVisualization(ConfigDBModel):
    # __bind_key__ = 'app_metadata'
    __tablename__ = 'cfg_visualization'
    __table_args__ = {'sqlite_autoincrement': True}

    id = db.Column(db.Integer(), primary_key=True, autoincrement=True)
    process_id = db.Column(db.Integer(), db.ForeignKey('cfg_process.id', ondelete='CASCADE'))
    # TODO confirm PO
    control_column_id = db.Column(db.Integer(), db.ForeignKey('cfg_process_column.id', ondelete='CASCADE'))
    filter_column_id = db.Column(
        db.Integer(),
        db.ForeignKey('cfg_process_column.id', ondelete='CASCADE'),
        nullable=True,
    )
    # filter_column_id = db.Column(db.Integer(), nullable=True)
    filter_value = db.Column(db.Text())
    is_from_data = db.Column(db.Boolean(), default=False)
    filter_detail_id = db.Column(db.Integer(), db.ForeignKey('cfg_filter_detail.id', ondelete='CASCADE'), nullable=True)

    ucl = db.Column(db.Float())
    lcl = db.Column(db.Float())
    upcl = db.Column(db.Float())
    lpcl = db.Column(db.Float())
    ymax = db.Column(db.Float())
    ymin = db.Column(db.Float())

    # TODO check default value, null is OK
    act_from = db.Column(db.Text())
    act_to = db.Column(db.Text())

    order = db.Column(db.Integer())

    control_column = db.relationship('CfgProcessColumn', foreign_keys=[control_column_id], lazy='joined')
    filter_column = db.relationship('CfgProcessColumn', foreign_keys=[filter_column_id], lazy='joined')
    filter_detail = db.relationship('CfgFilterDetail', lazy='joined')

    created_at = db.Column(db_timestamp(), default=get_current_timestamp)
    updated_at = db.Column(db_timestamp(), default=get_current_timestamp, onupdate=get_current_timestamp)

    @classmethod
    def get_filter_ids(cls):
        return db.session.query(cls.filter_detail_id).filter(cls.filter_detail_id > 0)

    @classmethod
    def get_by_control_n_filter_detail_ids(cls, col_ids, filter_detail_ids, start_tm, end_tm):
        return (
            cls.query.filter(
                and_(
                    cls.control_column_id.in_(col_ids),
                    cls.filter_detail_id.in_(filter_detail_ids),
                ),
            )
            .filter(or_(cls.act_from.is_(None), cls.act_from < end_tm, cls.act_from == ''))
            .filter(or_(cls.act_to.is_(None), cls.act_to > start_tm, cls.act_to == ''))
            .order_by(cls.act_from.desc())
            .all()
        )

    @classmethod
    def get_by_control_ids(cls, col_ids, start_tm, end_tm):
        return (
            cls.query.filter(cls.control_column_id.in_(col_ids))
            .filter(or_(cls.act_from.is_(None), cls.act_from < end_tm, cls.act_from == ''))
            .filter(or_(cls.act_to.is_(None), cls.act_to > start_tm, cls.act_to == ''))
            .order_by(cls.act_from.desc())
            .all()
        )

    @classmethod
    def get_sensor_default_chart_info(cls, col_ids, start_tm, end_tm):
        # TODO: not deleted, ...
        return (
            cls.query.filter(cls.control_column_id.in_(col_ids))
            .filter(and_(cls.filter_detail_id.is_(None)))
            .filter(or_(cls.act_from.is_(None), cls.act_from < end_tm, cls.act_from == ''))
            .filter(or_(cls.act_to.is_(None), cls.act_to > start_tm, cls.act_to == ''))
            .order_by(cls.act_from.desc())
            .all()
        )

    @classmethod
    def get_by_control_n_filter_col_id(cls, col_id, filter_col_id, start_tm, end_tm):
        # TODO: not deleted, ...
        return (
            cls.query.filter(
                and_(
                    cls.control_column_id == col_id,
                    cls.filter_column_id == filter_col_id,
                    cls.filter_value.is_(None),
                    cls.filter_detail_id.is_(None),
                ),
            )
            .filter(or_(cls.act_from.is_(None), cls.act_from < end_tm, cls.act_from == ''))
            .filter(or_(cls.act_to.is_(None), cls.act_to > start_tm, cls.act_to == ''))
            .order_by(cls.act_from.desc())
            .all()
        )

    @classmethod
    def get_all_by_control_n_filter_col_id(cls, col_id, filter_col_id, start_tm, end_tm):
        # TODO: not deleted, ...
        return (
            cls.query.filter(
                and_(
                    cls.control_column_id == col_id,
                    cls.filter_column_id == filter_col_id,
                ),
            )
            .filter(or_(cls.act_from.is_(None), cls.act_from < end_tm, cls.act_from == ''))
            .filter(or_(cls.act_to.is_(None), cls.act_to > start_tm, cls.act_to == ''))
            .order_by(cls.act_from.desc())
            .all()
        )

    @classmethod
    def get_by_filter_detail_id(cls, col_id, filter_detail_id, start_tm, end_tm):
        # TODO: not deleted, ...
        return (
            cls.query.filter(and_(cls.control_column_id == col_id, cls.filter_detail_id == filter_detail_id))
            .filter(or_(cls.act_from.is_(None), cls.act_from < end_tm, cls.act_from == ''))
            .filter(or_(cls.act_to.is_(None), cls.act_to > start_tm, cls.act_to == ''))
            .order_by(cls.act_from.desc())
            .all()
        )


class DataTraceLog(OthersDBModel):
    # __bind_key__ = 'app_metadata'
    __tablename__ = 't_data_trace_log'

    id = db.Column(db.Integer(), primary_key=True, autoincrement=True)
    date_time = db.Column(db_timestamp(), default=get_current_timestamp)
    dataset_id = db.Column(db.Integer())
    event_type = db.Column(db.Text())
    event_action = db.Column(db.Text())
    target = db.Column(db.Text())
    exe_time = db.Column(db.Integer())
    data_size = db.Column(db.BigInteger())
    rows = db.Column(db.Integer())
    cols = db.Column(db.Integer())
    dumpfile = db.Column(db.Text())
    created_at = db.Column(db_timestamp(), default=get_current_timestamp)
    updated_at = db.Column(db_timestamp(), default=get_current_timestamp, onupdate=get_current_timestamp)

    @classmethod
    def get_max_id(cls):
        out = cls.query.options(load_only(cls.id)).order_by(cls.id.desc()).first()
        if out:
            return out.id
        else:
            return 0

    @classmethod
    def get_dataset_id(cls, dataset_id, action):
        return cls.query.filter(cls.dataset_id == dataset_id, cls.event_action == action).all()


class AbnormalTraceLog(OthersDBModel):
    # __bind_key__ = 'app_metadata'
    __tablename__ = 't_abnormal_trace_log'

    id = db.Column(db.Integer(), primary_key=True, autoincrement=True)
    date_time = db.Column(db_timestamp(), default=get_current_timestamp)
    dataset_id = db.Column(db.Integer(), autoincrement=True)
    log_level = db.Column(db.Text(), default=LogLevel.ERROR.value)
    event_type = db.Column(db.Text())
    event_action = db.Column(db.Text())
    location = db.Column(db.Text(), default=Location.PYTHON.value)
    return_code = db.Column(db.Text(), default=ReturnCode.UNKNOWN_ERR.value)
    message = db.Column(db.Text())
    dumpfile = db.Column(db.Text())
    created_at = db.Column(db_timestamp(), default=get_current_timestamp)
    updated_at = db.Column(db_timestamp(), default=get_current_timestamp, onupdate=get_current_timestamp)


# TODO: rename : Import_history import_log
class FactoryImport(OthersDBModel):
    # __bind_key__ = 'app_metadata'
    __tablename__ = 't_factory_import'
    __table_args__ = {'sqlite_autoincrement': True}

    id = db.Column(db.Integer(), primary_key=True, autoincrement=True)
    # job_id = db.Column(db.Integer(), db.ForeignKey('t_job_management.id'), index=True)
    job_id = db.Column(db.Integer(), index=True)
    data_table_id = db.Column(db.Integer())
    process_id = db.Column(db.Integer())
    import_type = db.Column(db.Text())
    import_from = db.Column(db.Text())
    import_to = db.Column(db.Text())
    cycle_start_tm = db.Column(db_timestamp())
    cycle_end_tm = db.Column(db_timestamp())

    start_tm = db.Column(db_timestamp(), default=get_current_timestamp)
    end_tm = db.Column(db_timestamp())
    imported_row = db.Column(db.Integer(), default=0)
    imported_cycle_id = db.Column(db.Integer(), default=0)
    # TODO: check if Boolean work well in Sqlite3
    is_duplicate_checked = db.Column(db.Boolean(), default=0)
    synced = db.Column(db.Boolean(), default=0)
    status = db.Column(db.Text())
    error_msg = db.Column(db.Text())

    created_at = db.Column(db_timestamp(), default=get_current_timestamp)
    updated_at = db.Column(db_timestamp(), default=get_current_timestamp, onupdate=get_current_timestamp)

    @classmethod
    def is_range_time_imported(cls, process_id, start_dt, end_dt):
        # start_dt = convert_time(start_dt, format_str=DATE_FORMAT_STR_ONLY_DIGIT_SHORT)
        # end_dt = convert_time(end_dt, format_str=DATE_FORMAT_STR_ONLY_DIGIT_SHORT)

        data = cls.query.filter(cls.process_id == process_id)
        data = data.filter(cls.status.in_([str(JobStatus.FAILED), str(JobStatus.DONE)]))
        data = data.filter(cls.import_type.in_([str(JobType.TRANSACTION_IMPORT), str(JobType.TRANSACTION_PAST_IMPORT)]))
        data = data.filter(cls.synced.is_(False))

        data1 = data.filter(cls.cycle_start_tm <= start_dt, cls.cycle_end_tm >= start_dt)
        data1 = data1.first()
        if data1:
            return False

        data2 = data.filter(cls.cycle_start_tm <= end_dt, cls.cycle_end_tm >= end_dt)
        data2 = data2.first()
        if data2:
            return False

        return True

    @classmethod
    def get_last_import_transaction(cls, data_table_id, import_type):
        data = cls.query.filter(cls.data_table_id == data_table_id, cls.import_type == import_type)
        data = data.filter(cls.status.in_([str(JobStatus.FAILED), str(JobStatus.DONE)]))
        data = data.order_by(cls.id.desc())
        data = data.first()

        return data

    @classmethod
    def get_last_import(cls, process_id, import_type, only_synced=False, is_first_id=False):
        data = cls.query.filter(cls.process_id == process_id)
        data = data.filter(cls.import_type == import_type)
        if only_synced:
            data = data.filter(cls.synced.is_(True))
        data = data.filter(cls.status.in_([str(JobStatus.FAILED), str(JobStatus.DONE)]))
        data = data.order_by(cls.job_id.desc())
        data = data.order_by(cls.id) if is_first_id else data.order_by(cls.id.desc())

        data = data.first()

        return data

    @classmethod
    def get_last_pull_by_data_table(cls, data_table_id, import_type):
        data = cls.query.filter(cls.data_table_id == data_table_id)
        data = data.filter(cls.import_type == import_type, cls.import_from.isnot(None), cls.import_to.isnot(None))
        data = data.filter(cls.status.in_([str(JobStatus.FAILED), str(JobStatus.DONE)]))
        data = data.order_by(cls.id.desc())
        data = data.first()

        return data

    @classmethod
    def get_by_import_type(cls, process_id, import_type):
        data = cls.query.filter(cls.process_id == process_id)
        data = data.filter(cls.import_type == import_type)
        data = data.filter(cls.status.in_([str(JobStatus.FAILED), str(JobStatus.DONE)])).first()

        return data

    @classmethod
    def get_first_import(cls, process_id, import_type):
        data = cls.query.filter(cls.process_id == process_id)
        data = data.filter(cls.import_type == import_type)
        data = data.filter(cls.status.in_([str(JobStatus.FAILED), str(JobStatus.DONE)]))
        data = data.order_by(cls.id).first()

        return data

    @classmethod
    def get_first_pull_by_data_table(cls, data_table_id, import_type):
        data = cls.query.filter(cls.data_table_id == data_table_id)
        data = data.filter(cls.import_type == import_type, cls.import_from.isnot(None), cls.import_to.isnot(None))
        data = data.filter(cls.status.in_([str(JobStatus.FAILED), str(JobStatus.DONE)]))
        data = data.order_by(cls.id).first()

        return data

    @classmethod
    def get_error_jobs(cls, job_id):
        infos = cls.query.filter(cls.job_id == job_id).filter(cls.status != str(JobStatus.DONE))
        return infos.all()

    @classmethod
    def insert_history_record(
        cls,
        job_id,
        process_id,
        job_type,
        import_from,
        import_to,
        imported_row,
        import_status,
        start_tm=None,
        end_tm=None,
        error_msg=None,
        cycle_start_tm=None,
        cycle_end_tm=None,
        is_duplicate_checked=None,
    ):
        with make_session() as meta_session:
            factory_import = cls()
            factory_import.job_id = job_id
            factory_import.process_id = process_id
            factory_import.import_type = job_type
            factory_import.import_from = import_from
            factory_import.import_to = import_to
            factory_import.status = import_status
            factory_import.imported_row = imported_row
            factory_import.error_msg = error_msg
            if start_tm:
                factory_import.start_tm = start_tm
            else:
                factory_import.start_tm = get_current_timestamp()

            if end_tm:
                factory_import.end_tm = end_tm
            else:
                factory_import.end_tm = get_current_timestamp()

            factory_import.cycle_start_tm = cycle_start_tm
            factory_import.cycle_end_tm = cycle_end_tm
            factory_import.is_duplicate_checked = is_duplicate_checked

            meta_session.add(factory_import)

    @classmethod
    def get_latest_import_by_type(cls, job_type):
        latest_ids = db.session.query(func.max(cls.id)).filter(cls.import_type == job_type).group_by(cls.process_id)
        data = cls.query.filter(cls.id.in_(latest_ids)).all()

        return data

    @classmethod
    def get_sum_imported_record(cls, data_table_id):
        return db.session.query(func.sum(cls.imported_row)).filter(cls.data_table_id == data_table_id).first()


class AppLog(OthersDBModel):
    # __bind_key__ = 'app_metadata'
    __tablename__ = 't_app_log'
    __table_args__ = {'sqlite_autoincrement': True}

    id = db.Column(db.Integer(), primary_key=True, autoincrement=True)
    ip = db.Column(db.Text())
    action = db.Column(db.Text())
    description = db.Column(db.Text())
    created_at = db.Column(db_timestamp(), default=get_current_timestamp)


def insert_or_update_config(
    meta_session,
    data: Union[Dict, db.Model],
    key_names: Union[List, str] = None,
    model: db.Model = None,
    parent_obj: db.Model = None,
    parent_relation_key=None,
    parent_relation_type=None,
    exclude_columns=None,
):
    """

    :param exclude_columns:
    :param meta_session:
    :param data:
    :param key_names:
    :param model:
    :param parent_obj:
    :param parent_relation_key:
    :param parent_relation_type:
    :return:
    """
    excludes = ['created_at', 'updated_at']
    if exclude_columns:
        excludes += exclude_columns

    rec = None

    # get model
    if not model:
        model = data.__class__

    # default primary key is id
    # get primary keys
    primary_keys = [key.name for key in inspect(model).primary_key]

    if not key_names:
        key_names = primary_keys

    # convert to list
    if isinstance(key_names, str):
        key_names = [key_names]

    # query condition by keys
    if isinstance(data, db.Model):
        dict_key = {key: getattr(data, key) for key in key_names}
    else:
        dict_key = {key: data[key] for key in key_names}

    # query by key_names
    if dict_key:
        rec = meta_session.query(model).filter_by(**dict_key).first()

    # create new record
    if not rec:
        rec = model()
        if not parent_obj:
            meta_session.add(rec)
        elif parent_relation_type is RelationShip.MANY:
            objs = getattr(parent_obj, parent_relation_key)
            if objs is None:
                setattr(parent_obj, parent_relation_key, [rec])
            else:
                objs.append(rec)
        else:
            setattr(parent_obj, parent_relation_key, rec)

    dict_data = (
        {key: getattr(data, key) for key in data.__table__.columns.keys()} if isinstance(data, db.Model) else data
    )

    for key, val in dict_data.items():
        # primary keys
        if key in primary_keys and not val:
            continue

        # ignore non-data fields
        if key in excludes:
            continue

        # check if valid columns
        if key not in model.__table__.columns.keys():
            continue

        # avoid update None to primary key_names
        if key in key_names and not val:
            continue

        setattr(rec, key, val)

    meta_session.commit()

    return rec


def crud_config(
    meta_session,
    data: List[Union[Dict, db.Model]],
    parent_key_names: Union[List, str] = None,
    key_names: Union[List, str] = None,
    model: type[db.Model] = None,
    parent_obj: type[db.Model] = None,
    parent_relation_key=None,
    parent_relation_type=RelationShip.MANY,
):
    """

    :param meta_session:
    :param data:
    :param parent_key_names:
    :param key_names:
    :param model:
    :param parent_obj:
    :param parent_relation_key:
    :param parent_relation_type:
    :return:
    """
    # get model
    if not model:
        model = data[0].__class__

    # convert to list
    if isinstance(parent_key_names, str):
        parent_key_names = [parent_key_names]

    # get primary keys
    if not key_names:
        key_names = [key.name for key in inspect(model).primary_key]

    if isinstance(key_names, str):
        key_names = [key_names]

    key_names = parent_key_names + key_names

    # query condition by keys
    if parent_key_names:
        # query by key_names
        if not data:
            current_recs = []
            if parent_relation_key:  # assume that relation key was set in model
                current_recs = getattr(parent_obj, parent_relation_key)
        else:
            if isinstance(data[0], db.Model):
                dict_key = {key: getattr(data[0], key) for key in parent_key_names}
            else:
                dict_key = {key: data[0][key] for key in parent_key_names}

            current_recs = meta_session.query(model).filter_by(**dict_key).all()
    else:
        # query all
        current_recs = meta_session.query(model).all()

    # insert or update data
    set_active_keys = set()

    # # container
    # if parent_obj and parent_relation_key:
    #     setattr(parent_obj, parent_relation_key, [] if parent_relation_type else None)

    for row in data:
        if parent_obj and parent_relation_key:
            rec = insert_or_update_config(
                meta_session,
                row,
                key_names,
                model=model,
                parent_obj=parent_obj,
                parent_relation_key=parent_relation_key,
                parent_relation_type=parent_relation_type,
            )
        else:
            rec = insert_or_update_config(meta_session, row, key_names, model=model)

        key = tuple(getattr(rec, key) for key in key_names)
        set_active_keys.add(key)

    # delete data
    for current_rec in current_recs:
        key = tuple(getattr(current_rec, key) for key in key_names)
        if key in set_active_keys:
            continue

        meta_session.delete(current_rec)

    meta_session.commit()

    return True


class CfgUserSetting(ConfigDBModel):
    # __bind_key__ = 'app_metadata'
    __tablename__ = 'cfg_user_setting'
    __table_args__ = {'sqlite_autoincrement': True}

    id = db.Column(db.Integer(), primary_key=True, autoincrement=True)
    key = db.Column(db.Text())  # TODO use page_name/page_url + title for now
    title = db.Column(db.Text())
    page = db.Column(db.Text())
    created_by = db.Column(db.Text())
    priority = db.Column(db.Integer())
    use_current_time = db.Column(db.Boolean())
    description = db.Column(db.Text())
    share_info = db.Column(db.Boolean())
    save_graph_settings = db.Column(db.Boolean())
    settings = db.Column(db.Text())
    synced = db.Column(db.Boolean(), default=False)
    created_at = db.Column(db_timestamp(), default=get_current_timestamp)
    updated_at = db.Column(db_timestamp(), default=get_current_timestamp, onupdate=get_current_timestamp)

    @classmethod
    def get_all(cls):
        data = cls.query.options(
            load_only(
                cls.id,
                cls.key,
                cls.title,
                cls.page,
                cls.created_by,
                cls.priority,
                cls.use_current_time,
                cls.description,
                cls.share_info,
                cls.synced,
                cls.created_at,
                cls.updated_at,
            ),
        )
        data = data.order_by(cls.priority.desc(), cls.updated_at.desc()).all()
        return data

    @classmethod
    def delete_by_id(cls, meta_session, setting_id):
        user_setting = meta_session.query(cls).get(setting_id)
        if user_setting:
            meta_session.delete(user_setting)

    @classmethod
    def get_by_id(cls, setting_id):
        return cls.query.get(setting_id)

    @classmethod
    def get_top(cls, page):
        return cls.query.filter(cls.page == page).order_by(cls.priority.desc(), cls.updated_at.desc()).first()

    @classmethod
    def get_by_title(cls, title):
        return cls.query.filter(cls.title == title).order_by(cls.priority.desc(), cls.created_at.desc()).all()

    @classmethod
    def get_not_synced(cls):
        return cls.query.filter(cls.synced.is_(False)).all()

    @classmethod
    def get_bookmarks(cls):
        return cls.query.with_entities(
            cls.id,
            cls.priority,
            cls.page.label('function'),
            cls.title,
            cls.created_by,
            cls.description,
            cls.updated_at,
        ).all()

    @classmethod
    def get_page_by_bookmark(cls, bookmark_id):
        return cls.query.filter(cls.id == bookmark_id).first().page


class CfgRequest(db.Model):
    # __bind_key__ = 'app_metadata'
    __tablename__ = 'cfg_request'

    id = db.Column(db.Integer(), primary_key=True)
    params = db.Column(db.Text())
    odf = db.Column(db.Text())

    created_at = db.Column(db_timestamp(), default=get_current_timestamp)
    updated_at = db.Column(db_timestamp(), default=get_current_timestamp, onupdate=get_current_timestamp)

    options = db.relationship('CfgOption', cascade='all, delete', backref='parent')

    @classmethod
    def save_odf_and_params_by_req_id(cls, session, req_id, odf, params):
        req = cls.query.filter(cls.id == req_id).first()
        if not req:
            req = CfgRequest(id=req_id, odf=odf, params=params)
            session.add(req)
            session.commit()

    @classmethod
    def get_by_req_id(cls, req_id):
        return cls.query.get(req_id)

    @classmethod
    def get_odf_by_req_id(cls, req_id):
        req = cls.query.get(req_id)
        if req:
            return req.odf
        return None

    @classmethod
    def find_all_expired_reqs(cls, time):
        res = cls.query.filter(cls.created_at < time).all()
        return res or []


class CfgOption(db.Model):
    # __bind_key__ = 'app_metadata'
    __tablename__ = 'cfg_option'

    id = db.Column(db.Integer(), primary_key=True, autoincrement=True)
    option = db.Column(db.Text())
    req_id = db.Column(db.Integer(), db.ForeignKey('cfg_request.id', ondelete='CASCADE'))

    created_at = db.Column(db_timestamp(), default=get_current_timestamp)
    updated_at = db.Column(db_timestamp(), default=get_current_timestamp, onupdate=get_current_timestamp)

    @classmethod
    def get_option(cls, option_id):
        return cls.query.filter(cls.id == option_id).first()

    @classmethod
    def get_options(cls, req_id):
        return cls.query.filter(cls.req_id == req_id).all()


# class TBridgeStationRequestHistory(OthersDBModel):
#     # __bind_key__ = 'app_metadata'
#     __tablename__ = 't_bridge_station_request_history'
#     __table_args__ = {'sqlite_autoincrement': True}
#     id = db.Column(db.Integer(), primary_key=True)
#     service = db.Column(db.Text())
#     method = db.Column(db.Text())
#     transaction_group = db.Column(db.Integer())
#     message = db.Column(db.Text())
#     tried = db.Column(db.Integer(), default=0)
#     status = db.Column(db.Text())
#     sync_order = db.Column(db.Integer())
#     request_time = db.Column(db_timestamp())
#     response_time = db.Column(db_timestamp())
#     created_at = db.Column(db_timestamp(), default=get_current_timestamp)
#     updated_at = db.Column(db_timestamp(), default=get_current_timestamp, onupdate=get_current_timestamp)
#
#     @classmethod
#     def get_by_status(cls, service_name=None, method_name=None, status=None, exclude_msg=None):
#         recs = cls.query
#         if exclude_msg:
#             recs = recs.option(db.defer(cls.message))
#
#         if service_name:
#             recs = recs.filter(cls.service == service_name)
#
#         if method_name:
#             recs = recs.filter(cls.method == method_name)
#
#         if status:
#             recs = recs.filter(cls.status == status)
#
#         recs = recs.order_by(cls.id).all()
#         return recs
#
#     @classmethod
#     def update_status(cls, meta_session, rec_id, status, tried, request_msg=None):
#         dict_update_value = {cls.status: status, cls.tried: tried}
#         if request_msg:
#             dict_update_value.update({cls.message: pickle.dumps(request_msg)})
#         meta_session.query(cls).filter(cls.id == rec_id).update(dict_update_value)
#
#     @classmethod
#     def get_by_status_and_tried(cls, statuses, tried_less_than):
#         recs = cls.query.options(load_only(cls.id))
#         recs = recs.filter(cls.status.in_(statuses))
#         recs = recs.filter(cls.tried < tried_less_than)
#         recs = recs.order_by(cls.id).all()
#         return recs


# class TBridgeStationNayoseHistory(OthersDBModel):
#     # __bind_key__ = 'app_metadata'
#     __tablename__ = 't_bridge_station_nayose_history'
#     __table_args__ = {'sqlite_autoincrement': True}
#     id = db.Column(id_data_type_class(), primary_key=True)
#     job_id = db.Column(db.Integer())
#     path = db.Column(db.Text())
#     status = db.Column(db.Text())
#     using_by = db.Column(db.Text())
#     generated_by = db.Column(db.Text())
#     created_at = db.Column(date_time_data_type_class(), default=get_current_timestamp)
#     updated_at = db.Column(date_time_data_type_class(), default=get_current_timestamp, onupdate=get_current_timestamp)


# ###################### TABLE MASTER ###########################


class MUnit(MasterDBModel):
    __tablename__ = 'm_unit'
    __table_args__ = {'sqlite_autoincrement': True}
    id = db.Column(db.Integer(), primary_key=True)
    quantity_jp = db.Column(db.Text())
    quantity_en = db.Column(db.Text())
    unit = db.Column(db.Text())
    type = db.Column(db.Text())
    base = db.Column(db.Integer())
    conversion = db.Column(db.Float())
    denominator = db.Column(db.Float())
    offset = db.Column(db.Float())
    created_at = db.Column(db_timestamp(), default=get_current_timestamp)
    updated_at = db.Column(db_timestamp(), default=get_current_timestamp, onupdate=get_current_timestamp)

    @classmethod
    def get_empty_unit_id(cls):
        non_unit = cls.query.filter(cls.unit.is_(None)).first()
        return non_unit.id if non_unit else 1


class MLineGroup(MasterDBModel):
    __tablename__ = 'm_line_group'
    __table_args__ = {'sqlite_autoincrement': True}
    id = db.Column(db.Integer(), primary_key=True)
    line_name_jp = db.Column(db.Text())
    line_name_en = db.Column(db.Text())
    line_name_sys = db.Column(db.Text())
    line_name_local = db.Column(db.Text())
    created_at = db.Column(db_timestamp(), default=get_current_timestamp)
    updated_at = db.Column(db_timestamp(), default=get_current_timestamp, onupdate=get_current_timestamp)


class MLocation(MasterDBModel):
    __tablename__ = 'm_location'
    __table_args__ = {'sqlite_autoincrement': True}
    id = db.Column(db.Integer(), primary_key=True)
    location_name_jp = db.Column(db.Text())
    location_name_en = db.Column(db.Text())
    location_name_sys = db.Column(db.Text())
    location_abbr = db.Column(db.Text())
    created_at = db.Column(db_timestamp(), default=get_current_timestamp)
    updated_at = db.Column(db_timestamp(), default=get_current_timestamp, onupdate=get_current_timestamp)

    @classmethod
    def get_abbr_columns(cls):
        return [cls.location_abbr.key]


class MProdFamily(MasterDBModel):
    __tablename__ = 'm_prod_family'
    __table_args__ = {'sqlite_autoincrement': True}
    id = db.Column(db.Integer(), primary_key=True)
    prod_family_factid = db.Column(db.Text())  # "FACT": real-life code
    prod_family_name_jp = db.Column(db.Text())
    prod_family_name_en = db.Column(db.Text())
    prod_family_name_sys = db.Column(db.Text())
    prod_family_name_local = db.Column(db.Text())
    prod_family_abbr_jp = db.Column(db.Text())
    prod_family_abbr_en = db.Column(db.Text())
    prod_family_abbr_local = db.Column(db.Text())
    created_at = db.Column(db_timestamp(), default=get_current_timestamp)
    updated_at = db.Column(db_timestamp(), default=get_current_timestamp, onupdate=get_current_timestamp)


class MEquip(MasterDBModel):
    __tablename__ = 'm_equip'
    __table_args__ = {'sqlite_autoincrement': True}
    id = db.Column(db.Integer(), primary_key=True)
    equip_group_id = db.Column(db.Integer())
    equip_no = db.Column(db.SmallInteger())
    equip_sign = db.Column(db.Text())
    equip_factid = db.Column(db.Text())  # "FACT": real-life code
    equip_product_no = db.Column(db.Text())
    equip_product_date = db.Column(db_timestamp())
    created_at = db.Column(db_timestamp(), default=get_current_timestamp)
    updated_at = db.Column(db_timestamp(), default=get_current_timestamp, onupdate=get_current_timestamp)

    @classmethod
    def get_sign_column(cls):
        return cls.equip_sign.name

    @classmethod
    def get_default_sign_value(cls):
        return DEFAULT_EQUIP_SIGN


class MSt(MasterDBModel):
    __tablename__ = 'm_st'
    __table_args__ = {'sqlite_autoincrement': True}
    id = db.Column(db.Integer(), primary_key=True)
    equip_id = db.Column(db.Integer())
    st_no = db.Column(db.SmallInteger())
    st_sign = db.Column(db.Text())
    created_at = db.Column(db_timestamp(), default=get_current_timestamp)
    updated_at = db.Column(db_timestamp(), default=get_current_timestamp, onupdate=get_current_timestamp)

    @classmethod
    def get_sign_column(cls):
        return cls.st_sign.name

    @classmethod
    def get_default_sign_value(cls):
        return DEFAULT_ST_SIGN


class MProd(MasterDBModel):
    __tablename__ = 'm_prod'
    __table_args__ = {'sqlite_autoincrement': True}
    id = db.Column(db.Integer(), primary_key=True)
    prod_family_id = db.Column(db.Integer())
    prod_factid = db.Column(db.Text())
    prod_name_jp = db.Column(db.Text())
    prod_name_en = db.Column(db.Text())
    prod_name_sys = db.Column(db.Text())
    prod_name_local = db.Column(db.Text())
    prod_abbr_jp = db.Column(db.Text())
    prod_abbr_en = db.Column(db.Text())
    prod_abbr_local = db.Column(db.Text())
    created_at = db.Column(db_timestamp(), default=get_current_timestamp)
    updated_at = db.Column(db_timestamp(), default=get_current_timestamp, onupdate=get_current_timestamp)


class MFunction(MasterDBModel):
    __tablename__ = 'm_function'
    __table_args__ = {'sqlite_autoincrement': True}
    id = db.Column(db.Integer(), primary_key=True)
    function_type = db.Column(db.Text())
    function_name_en = db.Column(db.Text())
    function_name_jp = db.Column(db.Text())
    description_en = db.Column(db.Text())
    description_jp = db.Column(db.Text())
    x_type = db.Column(db.Text())  # r,i,t is real, int, text
    y_type = db.Column(db.Text())  #
    return_type = db.Column(db.Text())  # r,i,t is real, int, text. 'x' is same as x, 'y' is same as y
    show_serial = db.Column(db.Boolean())
    a = db.Column(db.Text())
    b = db.Column(db.Text())
    c = db.Column(db.Text())
    n = db.Column(db.Text())
    k = db.Column(db.Text())
    s = db.Column(db.Text())
    t = db.Column(db.Text())
    created_at = db.Column(db_timestamp(), default=get_current_timestamp)
    updated_at = db.Column(db_timestamp(), default=get_current_timestamp, onupdate=get_current_timestamp)

    @classmethod
    def get_by_function_type(cls, function_type):
        return cls.query.filter(cls.function_type == function_type).first()

    def has_x(self):
        return self.x_type is not None

    def has_y(self):
        return self.y_type is not None

    def get_variables(self) -> list[str]:
        variables = []
        if self.has_x():
            variables.append(VAR_X)
        if self.has_y():
            variables.append(VAR_Y)
        return variables

    def get_string_x_types(self) -> list[str]:
        return self.x_type.split(',') if self.has_x() else []

    def get_string_y_types(self) -> list[str]:
        return self.y_type.split(',') if self.has_y() else []

    def get_string_return_types(self):
        return self.return_type.split(',') if self.return_type else []

    def get_possible_x_types(self) -> list[RawDataTypeDB]:
        if not self.has_x():
            return []
        x_types = [RawDataTypeDB.get_by_enum_value(dtype) for dtype in self.get_string_x_types()]
        # extend integer types
        if RawDataTypeDB.INTEGER in x_types:
            x_types.extend([RawDataTypeDB.SMALL_INT, RawDataTypeDB.BIG_INT])
        return [dtype for dtype in x_types if dtype is not None]

    def get_possible_y_types(self) -> list[RawDataTypeDB]:
        if not self.has_y():
            return []
        y_types = [RawDataTypeDB.get_by_enum_value(dtype) for dtype in self.get_string_y_types()]
        # extend integer types
        if RawDataTypeDB.INTEGER in y_types:
            y_types.extend([RawDataTypeDB.SMALL_INT, RawDataTypeDB.BIG_INT])
        return [dtype for dtype in y_types if dtype is not None]

    def get_possible_return_type(self) -> RawDataTypeDB | None:
        if not self.return_type:
            return None

        return_types = [RawDataTypeDB.get_by_enum_value(dtype) for dtype in self.get_string_return_types()]
        return_types = [dtype for dtype in return_types if dtype is not None]

        if len(return_types) == 0:
            return None
        if len(return_types) > 1:
            raise ValueError(f'Return type for {self.return_type} is not valid')
        return return_types[0]

    def get_possible_cast_return_types(self) -> list[FunctionCastDataType]:
        return_types = [FunctionCastDataType.get_by_enum_value(dtype) for dtype in self.get_string_return_types()]
        return [dtype for dtype in return_types if dtype is not None]

    def get_x_data_type(self, x_data_type: str | None) -> RawDataTypeDB | None:
        return RawDataTypeDB.get_data_type_for_function(x_data_type, self.get_string_x_types())

    def get_y_data_type(self, y_data_type: str | None) -> RawDataTypeDB | None:
        return RawDataTypeDB.get_data_type_for_function(y_data_type, self.get_string_y_types())

    def get_output_data_type(
        self,
        x_data_type: str | None = None,
        _y_data_type: str | None = None,
        cast_data_type: str | None = None,
    ) -> RawDataTypeDB | None:
        # return type is specified
        possible_return_type = self.get_possible_return_type()
        if possible_return_type is not None:
            return possible_return_type

        # should cast to new type
        possible_cast_return_types = self.get_possible_cast_return_types()
        if FunctionCastDataType.CAST in possible_cast_return_types and cast_data_type:
            return RawDataTypeDB.get_by_enum_value(cast_data_type)

        # same as X
        if FunctionCastDataType.SAME_AS_X in possible_cast_return_types:
            x_data_type = self.get_x_data_type(x_data_type)
            return RawDataTypeDB.get_by_enum_value(x_data_type)

        return None


class MSect(MasterDBModel):
    __tablename__ = 'm_sect'
    __table_args__ = {'sqlite_autoincrement': True}
    id = db.Column(db.Integer(), primary_key=True)
    dept_id = db.Column(db.Integer())
    sect_factid = db.Column(db.Text())  # "FACT": real-life code
    sect_name_jp = db.Column(db.Text())
    sect_name_en = db.Column(db.Text())
    sect_name_sys = db.Column(db.Text())
    sect_name_local = db.Column(db.Text())
    sect_abbr_jp = db.Column(db.Text())
    sect_abbr_en = db.Column(db.Text())
    sect_abbr_local = db.Column(db.Text())
    created_at = db.Column(db_timestamp(), default=get_current_timestamp)
    updated_at = db.Column(db_timestamp(), default=get_current_timestamp, onupdate=get_current_timestamp)


class MProcess(MasterDBModel):
    __tablename__ = 'm_process'
    __table_args__ = {'sqlite_autoincrement': True}
    id = db.Column(db.Integer(), primary_key=True)
    prod_family_id = db.Column(db.Integer())
    process_factid = db.Column(db.Text())  # "FACT": real-life code
    process_name_jp = db.Column(db.Text())
    process_name_en = db.Column(db.Text())
    process_name_sys = db.Column(db.Text())
    process_name_local = db.Column(db.Text())
    process_abbr_jp = db.Column(db.Text())
    process_abbr_en = db.Column(db.Text())
    process_abbr_local = db.Column(db.Text())
    created_at = db.Column(db_timestamp(), default=get_current_timestamp)
    updated_at = db.Column(db_timestamp(), default=get_current_timestamp, onupdate=get_current_timestamp)
    deleted_at = db.Column(db_timestamp())

    @classmethod
    def get_existed_process_ids_query(cls, process_ids: list[int] | None) -> BaseQuery:
        query = cls.query.options(load_only(cls.id))
        query = query.filter(cls.deleted_at.is_(None))
        if process_ids:
            query = query.filter(cls.id.in_(process_ids))
        return query

    @classmethod
    def get_existed_process_ids(cls, process_ids=None):
        query = cls.get_existed_process_ids_query(process_ids=process_ids)
        m_processes = query.all()
        output_ids = [m_proc.id for m_proc in m_processes]
        return output_ids


class MPlant(MasterDBModel):
    __tablename__ = 'm_plant'
    __table_args__ = {'sqlite_autoincrement': True}
    id = db.Column(db.Integer(), primary_key=True)
    factory_id = db.Column(db.Integer())
    plant_factid = db.Column(db.Text())  # "FACT": real-life code
    plant_name_jp = db.Column(db.Text())
    plant_name_en = db.Column(db.Text())
    plant_name_sys = db.Column(db.Text())
    plant_name_local = db.Column(db.Text())
    plant_abbr_jp = db.Column(db.Text())
    plant_abbr_en = db.Column(db.Text())
    plant_abbr_local = db.Column(db.Text())
    created_at = db.Column(db_timestamp(), default=get_current_timestamp)
    updated_at = db.Column(db_timestamp(), default=get_current_timestamp, onupdate=get_current_timestamp)


class MDept(MasterDBModel):
    __tablename__ = 'm_dept'
    __table_args__ = {'sqlite_autoincrement': True}
    id = db.Column(db.Integer(), primary_key=True)
    dept_factid = db.Column(db.Text())  # "FACT": real-life code
    dept_name_jp = db.Column(db.Text())
    dept_name_en = db.Column(db.Text())
    dept_name_sys = db.Column(db.Text())
    dept_name_local = db.Column(db.Text())
    dept_abbr_jp = db.Column(db.Text())
    dept_abbr_en = db.Column(db.Text())
    dept_abbr_local = db.Column(db.Text())
    created_at = db.Column(db_timestamp(), default=get_current_timestamp)
    updated_at = db.Column(db_timestamp(), default=get_current_timestamp, onupdate=get_current_timestamp)


class MPart(MasterDBModel):
    __tablename__ = 'm_part'
    __table_args__ = {'sqlite_autoincrement': True}
    id = db.Column(db.Integer(), primary_key=True)
    part_type_id = db.Column(db.Integer())
    part_factid = db.Column(db.Text())  # "FACT": real-life code
    part_no = db.Column(db.Text())
    part_use = db.Column(db.Boolean())
    location_id = db.Column(db.Integer())
    created_at = db.Column(db_timestamp(), default=get_current_timestamp)
    updated_at = db.Column(db_timestamp(), default=get_current_timestamp, onupdate=get_current_timestamp)


class MEquipGroup(MasterDBModel):
    __tablename__ = 'm_equip_group'
    __table_args__ = {'sqlite_autoincrement': True}
    id = db.Column(db.Integer(), primary_key=True)
    equip_name_jp = db.Column(db.Text())
    equip_name_en = db.Column(db.Text())
    equip_name_sys = db.Column(db.Text())
    equip_name_local = db.Column(db.Text())
    created_at = db.Column(db_timestamp(), default=get_current_timestamp)
    updated_at = db.Column(db_timestamp(), default=get_current_timestamp, onupdate=get_current_timestamp)


class MDataGroup(MasterDBModel):
    __tablename__ = 'm_data_group'
    __table_args__ = {'sqlite_autoincrement': True}
    id = db.Column(db.Integer(), primary_key=True)
    data_name_jp = db.Column(db.Text())
    data_name_en = db.Column(db.Text())
    data_name_sys = db.Column(db.Text())
    data_name_local = db.Column(db.Text())
    data_abbr_jp = db.Column(db.Text())
    data_abbr_en = db.Column(db.Text())
    data_abbr_local = db.Column(db.Text())
    data_group_type = db.Column(db.SmallInteger())
    created_at = db.Column(db_timestamp(), default=get_current_timestamp)
    updated_at = db.Column(db_timestamp(), default=get_current_timestamp, onupdate=get_current_timestamp)

    @classmethod
    def get_data_group_in_group_types(cls, group_types: List):
        return cls.query.filter(cls.data_group_type.in_(group_types)).order_by(cls.id.desc()).all()

    @classmethod
    def get_in_ids(cls, ids):
        return cls.query.filter(cls.id.in_(ids)).order_by(cls.id.desc()).all()

    @classmethod
    def get_by_data_name_sys(cls, data_name_sys):
        return cls.query.filter(cls.data_name_sys == data_name_sys).first()


class MData(MasterDBModel):
    __tablename__ = 'm_data'
    __table_args__ = {'sqlite_autoincrement': True}
    id = db.Column(db.Integer(), primary_key=True)
    process_id = db.Column(db.Integer())
    data_group_id = db.Column(db.Integer())
    data_type = db.Column(db.Text())
    unit_id = db.Column(db.Integer())
    config_equation_id = db.Column(db.Integer())
    data_factid = db.Column(db.Text())  # "FACT": real-life code
    is_hide = db.Column(db.Boolean(), default=False)  # "FACT": real-life code
    created_at = db.Column(db_timestamp(), default=get_current_timestamp)
    updated_at = db.Column(db_timestamp(), default=get_current_timestamp, onupdate=get_current_timestamp)

    @classmethod
    def update_by_type(cls, ids, data_type, session=None):
        query_obj = session.query(cls) if session else cls.query
        query_obj.filter(cls.id.in_(ids)).update({cls.data_type: data_type}, synchronize_session='fetch')

    @classmethod
    def get_by_process_id(cls, process_id, session=None, as_df=False):
        if as_df:
            return cls.get_all_as_df(dic_conditions={MData.process_id.key: process_id}, session=session)

        query_obj = session.query(cls) if session else cls.query
        return query_obj.filter(cls.process_id == process_id).all()

    @classmethod
    def get_in_data_group_ids_and_process(cls, process_id, data_group_ids):
        return cls.query.filter(cls.process_id == process_id, cls.data_group_id.in_(data_group_ids)).all()

    @classmethod
    def hide_col_by_ids(cls, process_id=None, data_ids=None, is_hide=True, session=None):
        query_obj = session.query(cls) if session else cls.query
        if process_id:
            query_obj = query_obj.filter(cls.process_id == process_id)

        if data_ids:
            query_obj = query_obj.filter(cls.id.in_(data_ids))

        query_obj.update({cls.is_hide: is_hide}, synchronize_session='fetch')

    @classmethod
    def get_process_id_data_id(cls):
        objs = cls.query.all()
        data_ids, process_ids = [], []
        if objs:
            for m in objs:
                data_ids.append(m.id)
                process_ids.append(m.process_id)
        return data_ids, process_ids

    @classmethod
    def remove_by_col_ids(cls, ids, session=None):
        query = session.query(cls) if session else cls.query
        query.filter(cls.id.in_(ids)).delete(synchronize_session='fetch')

        return True


class MFactory(MasterDBModel):
    __tablename__ = 'm_factory'
    __table_args__ = {'sqlite_autoincrement': True}
    id = db.Column(db.Integer(), primary_key=True)
    factory_factid = db.Column(db.Text())  # "FACT": real-life code
    factory_name_jp = db.Column(db.Text())
    factory_name_en = db.Column(db.Text())
    factory_name_sys = db.Column(db.Text())
    factory_name_local = db.Column(db.Text())
    location_id = db.Column(db.Integer())
    factory_abbr_jp = db.Column(db.Text())
    factory_abbr_en = db.Column(db.Text())
    factory_abbr_local = db.Column(db.Text())
    created_at = db.Column(db_timestamp(), default=get_current_timestamp)
    updated_at = db.Column(db_timestamp(), default=get_current_timestamp, onupdate=get_current_timestamp)


class MPartType(MasterDBModel):
    __tablename__ = 'm_part_type'
    __table_args__ = {'sqlite_autoincrement': True}
    id = db.Column(db.Integer(), primary_key=True)
    part_type_factid = db.Column(db.Text())  # "FACT": real-life code
    part_name_jp = db.Column(db.Text())
    part_name_en = db.Column(db.Text())
    part_name_local = db.Column(db.Text())
    part_abbr_jp = db.Column(db.Text())
    part_abbr_en = db.Column(db.Text())
    part_abbr_local = db.Column(db.Text())
    assy_flag = db.Column(db.Boolean())
    created_at = db.Column(db_timestamp(), default=get_current_timestamp)
    updated_at = db.Column(db_timestamp(), default=get_current_timestamp, onupdate=get_current_timestamp)


class MLine(MasterDBModel):
    __tablename__ = 'm_line'
    __table_args__ = {'sqlite_autoincrement': True}
    id = db.Column(db.Integer(), primary_key=True)
    plant_id = db.Column(db.Integer())
    prod_family_id = db.Column(db.Integer())
    line_group_id = db.Column(db.Integer())
    line_factid = db.Column(db.Text())  # "FACT": real-life code
    line_no = db.Column(db.SmallInteger())
    line_sign = db.Column(db.Text())
    outsourcing_flag = db.Column(db.Boolean())
    outsource = db.Column(db.Text())
    supplier = db.Column(db.Text())
    created_at = db.Column(db_timestamp(), default=get_current_timestamp)
    updated_at = db.Column(db_timestamp(), default=get_current_timestamp, onupdate=get_current_timestamp)

    @classmethod
    def get_sign_column(cls):
        return cls.line_sign.name

    @classmethod
    def get_default_sign_value(cls):
        return DEFAULT_LINE_SIGN


class RProdPart(MasterDBModel):
    __tablename__ = 'r_prod_part'
    __table_args__ = {'sqlite_autoincrement': True}
    id = db.Column(db.Integer(), primary_key=True)
    prod_id = db.Column(db.Integer())
    part_id = db.Column(db.Integer())
    created_at = db.Column(db_timestamp(), default=get_current_timestamp)
    updated_at = db.Column(db_timestamp(), default=get_current_timestamp, onupdate=get_current_timestamp)

    @classmethod
    def mapping_master_columns(cls) -> set[db.Column]:
        cols = set()
        exclude_cols = [cls.id, cls.created_at, cls.updated_at]
        exclude_cols_name = {col.name for col in exclude_cols}
        for col in sqlalchemy.inspect(cls).columns:
            if col.name not in exclude_cols_name:
                cols.add(col)
        return cols


class RFactoryMachine(MasterDBModel):
    __tablename__ = 'r_factory_machine'
    __table_args__ = {'sqlite_autoincrement': True}
    id = db.Column(db.Integer(), primary_key=True)
    line_id = db.Column(db.Integer())
    process_id = db.Column(db.Integer())
    equip_id = db.Column(db.Integer())
    equip_st = db.Column(db.Integer())
    sect_id = db.Column(db.Integer())
    st_id = db.Column(db.Integer())
    created_at = db.Column(db_timestamp(), default=get_current_timestamp)
    updated_at = db.Column(db_timestamp(), default=get_current_timestamp, onupdate=get_current_timestamp)

    @classmethod
    def get_by_process_id(cls, process_id):
        return cls.query.filter(cls.process_id == process_id).all()

    @classmethod
    def get_by_process_ids(cls, process_ids):
        return cls.query.filter(cls.process_id.in_(process_ids)).all()

    @classmethod
    def mapping_master_columns(cls) -> set[db.Column]:
        cols = set()
        exclude_cols = [cls.id, cls.created_at, cls.updated_at]
        exclude_cols_name = {col.name for col in exclude_cols}
        for col in sqlalchemy.inspect(cls).columns:
            if col.name not in exclude_cols_name:
                cols.add(col)
        return cols


class SemiMaster(SemiMasterDBModel):  # Part of transaction table
    __tablename__ = 'semi_master'
    factor = db.Column(db.SmallInteger(), primary_key=True)
    value = db.Column(db.Text())
    group_id = db.Column(db.Integer(), primary_key=True)
    updated_at = db.Column(db_timestamp(), default=get_current_timestamp, onupdate=get_current_timestamp)


class MappingProcessData(MappingDBModel):
    __tablename__ = 'mapping_process_data'
    __table_args__ = {'sqlite_autoincrement': True}
    id = db.Column(db.Integer(), primary_key=True, autoincrement=True)
    t_process_id = db.Column(db.Text())
    t_process_name = db.Column(db.Text())
    t_process_abbr = db.Column(db.Text())
    t_data_id = db.Column(db.Text())
    t_data_name = db.Column(db.Text())
    t_data_abbr = db.Column(db.Text())
    t_prod_family_id = db.Column(db.Text())
    t_prod_family_name = db.Column(db.Text())
    t_prod_family_abbr = db.Column(db.Text())
    t_unit = db.Column(db.Text())
    data_id = db.Column(db.Integer())
    data_table_id = db.Column(db.Integer())

    @classmethod
    def get_process_data_table_id(cls):
        objs = cls.query.all()
        data_ids, data_table_ids = [], []
        if objs:
            for m in objs:
                data_ids.append(m.data_id)
                data_table_ids.append(m.data_table_id)
        return data_ids, data_table_ids


class MappingFactoryMachine(MappingDBModel):
    __tablename__ = 'mapping_factory_machine'
    __table_args__ = {'sqlite_autoincrement': True}
    id = db.Column(db.Integer(), primary_key=True, autoincrement=True)
    t_location_name = db.Column(db.Text())
    t_location_abbr = db.Column(db.Text())
    t_factory_id = db.Column(db.Text())
    t_factory_name = db.Column(db.Text())
    t_factory_abbr = db.Column(db.Text())
    t_plant_id = db.Column(db.Text())
    t_plant_name = db.Column(db.Text())
    t_plant_abbr = db.Column(db.Text())
    t_dept_id = db.Column(db.Text())
    t_dept_name = db.Column(db.Text())
    t_dept_abbr = db.Column(db.Text())
    t_sect_id = db.Column(db.Text())
    t_sect_name = db.Column(db.Text())
    t_sect_abbr = db.Column(db.Text())
    t_line_id = db.Column(db.Text())
    t_line_no = db.Column(db.Text())
    t_line_name = db.Column(db.Text())
    t_outsource = db.Column(db.Text())
    t_equip_id = db.Column(db.Text())
    t_equip_name = db.Column(db.Text())
    t_equip_product_no = db.Column(db.Text())
    t_equip_product_date = db.Column(db.Text())
    t_equip_no = db.Column(db.Text())
    t_station_no = db.Column(db.Text())
    t_prod_family_id = db.Column(db.Text())
    t_prod_family_name = db.Column(db.Text())
    t_prod_family_abbr = db.Column(db.Text())
    t_prod_id = db.Column(db.Text())
    t_prod_name = db.Column(db.Text())
    t_prod_abbr = db.Column(db.Text())
    t_process_id = db.Column(db.Text())
    t_process_name = db.Column(db.Text())
    t_process_abbr = db.Column(db.Text())
    factory_machine_id = db.Column(db.Integer())
    data_table_id = db.Column(db.Integer())

    @classmethod
    def get_by_data_table_id(cls, data_table_id):
        return cls.query.filter(cls.data_table_id == data_table_id).all()

    @classmethod
    def get_by_r_factory_machine_id(cls, r_factory_machine_id):
        return cls.query.filter(cls.factory_machine_id == r_factory_machine_id).all()

    @classmethod
    def get_by_factory_machine_ids(cls, factory_machine_ids):
        return cls.query.filter(cls.factory_machine_id.in_(factory_machine_ids)).all()


class MappingPart(MappingDBModel):
    __tablename__ = 'mapping_part'
    __table_args__ = {'sqlite_autoincrement': True}
    id = db.Column(db.Integer(), primary_key=True, autoincrement=True)
    t_part_type = db.Column(db.Text())
    t_part_name = db.Column(db.Text())
    t_part_abbr = db.Column(db.Text())
    t_part_no_full = db.Column(db.Text())
    t_part_no = db.Column(db.Text())
    part_id = db.Column(db.Integer())
    data_table_id = db.Column(db.Integer())


class MGroup(MasterDBModel):
    __tablename__ = 'm_group'
    __table_args__ = {'sqlite_autoincrement': True}
    id = db.Column(db.Integer(), primary_key=True, autoincrement=True)
    data_group_id = db.Column(db.Integer())
    created_at = db.Column(db_timestamp(), default=get_current_timestamp)
    updated_at = db.Column(db_timestamp(), default=get_current_timestamp, onupdate=get_current_timestamp)
    column_groups = db.relationship('MColumnGroup', lazy='joined', backref='m_column_group', cascade='all')

    @classmethod
    def delete(cls, group_id: int, session: scoped_session = None):
        query = session.query(cls) if session else cls.query
        query.filter(cls.id == group_id).delete()


class MColumnGroup(MasterDBModel):
    __tablename__ = 'm_column_group'
    __table_args__ = {'sqlite_autoincrement': True}
    id = db.Column(db.Integer(), primary_key=True, autoincrement=True)
    data_id = db.Column(db.Integer())
    group_id = db.Column(db.Integer(), db.ForeignKey('m_group.id', ondelete='CASCADE'))
    created_at = db.Column(db_timestamp(), default=get_current_timestamp)
    updated_at = db.Column(db_timestamp(), default=get_current_timestamp, onupdate=get_current_timestamp)
    group = db.relationship('MGroup', lazy='subquery', backref='m_group', uselist=False, cascade='all')

    @classmethod
    def get_by_group_ids(cls, group_ids: list[int], session: scoped_session = None):
        query = session.query(cls) if session else cls.query
        return query.filter(cls.group_id.in_(group_ids)).all()

    @classmethod
    def get_by_data_ids(cls, data_ids: list[int], session: scoped_session = None):
        query = session.query(cls) if session else cls.query
        return query.filter(cls.data_id.in_(data_ids)).all()

    @classmethod
    def delete_by_data_ids(cls, data_ids: list[int], session: scoped_session = None):
        query = session.query(cls) if session else cls.query
        return query.filter(cls.data_id.in_(data_ids)).delete(synchronize_session='fetch')

    @classmethod
    def delete(cls, column_group_id: int, session: scoped_session = None):
        query = session.query(cls) if session else cls.query
        query.filter(cls.id == column_group_id).delete()


class MappingCategoryData(MappingDBModel):
    __tablename__ = 'mapping_category_data'
    __table_args__ = {'sqlite_autoincrement': True}
    id = db.Column(db.Integer(), primary_key=True, autoincrement=True)
    factor = db.Column(db.SmallInteger())
    t_category_data = db.Column(db.Text())
    group_id = db.Column(db.Integer())
    data_id = db.Column(db.Integer())
    data_table_id = db.Column(db.Integer())
    created_at = db.Column(db_timestamp(), default=get_current_timestamp)
    updated_at = db.Column(db_timestamp(), default=get_current_timestamp, onupdate=get_current_timestamp)


class ArchivedConfigMaster(OthersDBModel):
    __tablename__ = 'archived_config_master'
    __table_args__ = {'sqlite_autoincrement': True}

    id = db.Column(db.Integer(), primary_key=True, autoincrement=True)
    table_name = db.Column(db.Text())
    archived_id = db.Column(db.Integer())
    data = db.Column(db.Text())

    created_at = db.Column(db_timestamp(), default=get_current_timestamp)
    updated_at = db.Column(db_timestamp(), default=get_current_timestamp)


class ArchivedCycle(OthersDBModel):
    __tablename__ = 'archived_cycle'
    __table_args__ = {'sqlite_autoincrement': True}

    id = db.Column(db.Integer(), primary_key=True, autoincrement=True)
    job_id = db.Column(db.Integer())
    process_id = db.Column(db.Integer())
    archived_ids = db.Column(db.Text())  # pickle (list)

    created_at = db.Column(db_timestamp(), default=get_current_timestamp)
    updated_at = db.Column(db_timestamp(), default=get_current_timestamp, onupdate=get_current_timestamp)


class ApschedulerJobs(OthersDBModel):
    __abstract__ = True
    __tablename__ = 'apscheduler_jobs'

    from sqlalchemy.dialects import postgresql

    id = (db.Column(db.VARCHAR(length=191), primary_key=True, autoincrement=False, nullable=False),)
    next_run_time = (db.Column(postgresql.DOUBLE_PRECISION(precision=53), autoincrement=False, nullable=True),)
    job_state = db.Column(postgresql.BYTEA(), autoincrement=False, nullable=False)


class CsvManagement(OthersDBModel):
    # __bind_key__ = 'app_metadata'
    __tablename__ = 't_csv_management'
    __table_args__ = {'sqlite_autoincrement': True}

    id = db.Column(db.Integer(), primary_key=True, autoincrement=True)
    file_name = db.Column(db.Text())
    data_table_id = db.Column(db.Integer())
    data_time = db.Column(db.SmallInteger())
    data_process = db.Column(db.Text())
    data_line = db.Column(db.Text())
    data_delimiter = db.Column(db.Text())
    data_encoding = db.Column(db.Text())
    data_size = db.Column(db.Float())
    # clone_status = db.Column(db.Boolean())  # TODO: remove
    scan_status = db.Column(db.Boolean())
    dump_status = db.Column(db.Boolean())
    # import_status = db.Column(db.Boolean())  # TODO: remove

    created_at = db.Column(db_timestamp(), default=get_current_timestamp)
    updated_at = db.Column(db_timestamp(), default=get_current_timestamp, onupdate=get_current_timestamp)

    @classmethod
    def get_min_date_time(cls, data_table_id):
        return cls.query.filter(cls.data_table_id == data_table_id).order_by(cls.data_time).last()


class AutoLink(OthersDBModel):
    __tablename__ = 't_auto_link'

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    process_id = db.Column(db.Integer(), index=True)
    serial = db.Column(db.Text())
    date_time = db.Column(db_timestamp())


# ###################### TABLE MASTER : END ###########################


def get_models(modelType=None):
    if modelType is None:
        db_modules = [ConfigDBModel, MasterDBModel, MappingDBModel, OthersDBModel]

    elif isinstance(modelType, (list, tuple)):
        db_modules = modelType
    else:
        db_modules = [modelType]

    all_sub_classes = []
    for db_module in db_modules:
        all_sub_classes += db_module.__subclasses__()

    return tuple(all_sub_classes)


def trigger_master_config_changed(target, crud_type):
    """
    trigger when there is changed in master or config table
    :param target:
    :param crud_type:
    :return:
    """
    table_name = target.__table__.name

    dict_data = target.as_dict()
    id = getattr(target, 'id', False)  # there not id column in some tables
    if not id:
        return

    if crud_type == CRUDType.DELETE.name:
        with make_session(is_new_session=True) as meta_session:
            archived_rec = ArchivedConfigMaster()
            archived_rec.table_name = table_name
            archived_rec.archived_id = id
            archived_rec.data = json.dumps(dict_data, ensure_ascii=False, default=http_content.json_serial)
            meta_session.add(archived_rec)

    publish_master_config_changed(table_name=table_name, crud_type=crud_type, id=id, dict_data=dict_data)


def make_f(model):
    list_of_target = (ConfigDBModel, MasterDBModel)

    @event.listens_for(model, 'before_insert')
    def before_insert(_mapper, _connection, target):
        model_normalize(target)
        if isinstance(target, list_of_target):
            set_all_cache_expired(CacheType.CONFIG_DATA)

    @event.listens_for(model, 'before_update')
    def before_update(_mapper, _connection, target):
        model_normalize(target)
        if isinstance(target, list_of_target):
            set_all_cache_expired(CacheType.CONFIG_DATA)

    if not basic_config_yaml.is_postgres_db():
        return

    @event.listens_for(model, 'after_insert')
    def after_insert(mapper, connection, target):
        logger.debug(f'insert {target}')
        trigger_master_config_changed(target, CRUDType.INSERT.name)

    @event.listens_for(model, 'after_update')
    def after_update(mapper, connection, target):
        logger.debug(f'update {target}')
        trigger_master_config_changed(target, CRUDType.UPDATE.name)

    @event.listens_for(model, 'after_delete')
    def after_delete(mapper, connection, target):
        logger.debug(f'delete {target}')
        trigger_master_config_changed(target, CRUDType.DELETE.name)


def add_listen_event():
    for model in get_models([ConfigDBModel, MasterDBModel, OthersDBModel]):
        make_f(model)


# GUI normalization
add_listen_event()
