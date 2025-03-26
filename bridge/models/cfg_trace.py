from typing import List

import pandas as pd

from ap.common.constants import DataType
from bridge.models.bridge_station import ConfigModel
from bridge.models.cfg_trace_key import CfgTraceKey
from bridge.models.model_utils import TableColumn


class CfgTrace(ConfigModel):
    def __init__(self, dict_trace=None):
        if not dict_trace:
            dict_trace = {}
        self.id = dict_trace.get(CfgTrace.Columns.id.name)
        if self.id is None:
            del self.id
        self.self_process_id = dict_trace.get(CfgTrace.Columns.self_process_id.name)
        self.target_process_id = dict_trace.get(CfgTrace.Columns.target_process_id.name)
        self.is_trace_backward = dict_trace.get(CfgTrace.Columns.is_trace_backward.name)
        self.created_at = dict_trace.get(CfgTrace.Columns.created_at.name)
        self.updated_at = dict_trace.get(CfgTrace.Columns.updated_at.name)
        trace_keys = dict_trace.get('trace_keys')
        self.trace_keys: List[CfgTraceKey] = [CfgTraceKey(**key) for key in trace_keys] if trace_keys else []

    class Columns(TableColumn):
        id = (1, DataType.INTEGER)
        self_process_id = (2, DataType.INTEGER)
        target_process_id = (3, DataType.INTEGER)
        is_trace_backward = (4, DataType.BOOLEAN)

        created_at = (5, DataType.DATETIME)
        updated_at = (6, DataType.DATETIME)

    _table_name = 'cfg_trace'
    primary_keys = [Columns.id]

    @classmethod
    def _to_objs(cls, db_instance, rows, cascade_trace_key=False):
        rt_list = []
        for record in rows:
            trace = cls._to_obj(db_instance, record, cascade_trace_key)
            rt_list.append(trace)
        return rt_list

    @classmethod
    def _to_obj(cls, db_instance, record, cascade_trace_key=False):
        trace = CfgTrace(record)
        if cascade_trace_key:
            trace.load_trace_keys_cascade(db_instance)

        return trace

    @classmethod
    def get_all(cls):
        return cls.query.all()

    def as_dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}

    @classmethod
    def get_traces_of_proc(cls, db_instance, proc_id=None, cascade_trace_key=False):
        """
        get traces that relates to one proc
        :param db_instance:
        :param proc_id:
        :param cascade_trace_key:
        :return:
        """
        dict_conds = {}
        if proc_id:
            dict_conds = {
                cls.Columns.self_process_id.name: proc_id,
                cls.Columns.target_process_id.name: proc_id,
            }
        _, rows = cls.select_records(db_instance, is_or_operation=True, dic_conditions=dict_conds)
        return cls._to_objs(db_instance, rows, cascade_trace_key)

    @classmethod
    def get_trace(cls, db_instance, trace_id, cascade_trace_key=False):
        """
        get traces that relates to one proc
        :param db_instance:
        :param trace_id:
        :param cascade_trace_key:
        :return:
        """
        _, row = cls.select_records(db_instance, dic_conditions={cls.Columns.id.name: trace_id}, limit=1)
        return cls._to_obj(db_instance, row, cascade_trace_key) if row else None

    def load_trace_keys_cascade(self, db_instance):
        trace_keys = CfgTraceKey.get_trace_keys(db_instance, self.id)
        if trace_keys:
            self.trace_keys.extend(trace_keys)

    @classmethod
    def get_all_traces(cls, db_instance, select_cols=None, cascade_trace_key=False):
        """
        get traces that relates to one proc
        :param db_instance:
        :param proc_id:
        :return:
        """
        _, rows = cls.select_records(db_instance, select_cols=select_cols)
        return cls._to_objs(db_instance, rows, cascade_trace_key)

    @classmethod
    def get_all_traced_processes(cls, db_instance):
        """
        get traces that relates to one proc
        :param db_instance:
        :param proc_id:
        :return:
        """
        return cls.get_all_traces(
            db_instance,
            select_cols=[cls.Columns.self_process_id.name, cls.Columns.target_process_id.name],
            cascade_trace_key=False,
        )

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
