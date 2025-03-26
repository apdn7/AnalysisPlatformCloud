from __future__ import annotations

from dataclasses import dataclass
from functools import cached_property
from typing import TYPE_CHECKING, Any, Optional, Union

import sqlalchemy as sa
from sqlalchemy import cast, func, join, or_, select
from sqlalchemy.orm import aliased
from sqlalchemy.sql import operators, sqltypes
from sqlalchemy.sql.elements import ColumnClause, and_

from ap import log_execution_time
from ap.common.common_utils import gen_proc_time_label, gen_sql_label, gen_sql_like_value
from ap.common.constants import (
    TIME_COL,
    DataGroupType,
    DataType,
    DuplicateSerialShow,
    FilterFunc,
    RawDataTypeDB,
)
from ap.setting_module import models as ap_models
from ap.setting_module.models import CfgProcessColumn, MColumnGroup
from bridge.models.transaction_model import TransactionData
from bridge.services.master_catalog import RelationMasterSingleton
from bridge.services.sql.transaction_query_builder import (
    TransactionDataProcLinkQueryBuilder,
    TransactionDataQueryBuilder,
)

if TYPE_CHECKING:
    from sqlalchemy.orm.util import AliasedClass
    from sqlalchemy.sql.operators import ColumnOperators, Operators
    from sqlalchemy.sql.selectable import CTE, Join, Select

    from bridge.models.cfg_filter_detail import CfgFilterDetail
    from bridge.models.cfg_trace import CfgTrace
    from bridge.services.trace_data_schema import ConditionProc, ConditionProcDetail

ROW_NUMBER_COL_PREFIX = 'cum_count'

CTE_PROCESS_PREFIX = 'cte_p'

PROCESS_ALIAS_PREFIX = 'p'
NORMAL_FILTER_ALIAS_PREFIX = 'pn'
MASTER_FILTER_ALIAS_PREFIX = 'pm'
SHOW_CATEGORY_ALIAS_PREFIX = 'ps'

SEMI_TABLE_ALIAS_PREFIX = 's'
MASTER_MAPPING_PREFIX = 'm'

MAPPING_PART_NO = 'mapping_part'
MAPPING_LINE = 'mapping_line'
MAPPING_MACHINE = 'mapping_equip'
MAPPING_PROCESS = 'mapping_process'

SQL_GENERATOR_PREFIX = 'SQL_GENERATOR'

NOT_MATERIALIZED = 'NOT MATERIALIZED'

FILTER_PREFIX = 'filtered'

TIMEDIFF_PREFIX = 'timediff'
RANK_BY_TIMEDIFF = 'rank_by_timediff'
CTE_TRACING_TIMEDIFF = 'cte_tracing_timediff'
CTE_TRACING_RANK_BY_TIMEDIFF = 'cte_tracing_rank_by_timediff'
CTE_TRACING_MIN_TIMEDIFF = 'cte_tracing_min_timediff'


def gen_alias_col_name(trans_data: TransactionData, column_name: str) -> Optional[str]:
    cfg_column = trans_data.get_cfg_column_by_name(column_name)
    if cfg_column is None:
        return None
    if cfg_column.is_data_source_name_column():
        raise NotImplementedError('Did not implement master data source name')
    elif cfg_column.is_master_data_column():
        relation_master = RelationMasterSingleton.instance(DataGroupType(cfg_column.column_type))
        alias_name = f'{relation_master.r_column_name}_{trans_data.process_id}'
    else:
        alias_name = gen_sql_label(cfg_column.id, cfg_column.column_name)
    return alias_name


def gen_row_number_col_name(proc_id: int) -> str:
    return f'{ROW_NUMBER_COL_PREFIX}_{proc_id}'


def join_table(
    join_conditions: Optional[Join],
    left: Any,
    right: Any,
    onclause: Union[Operators, bool],
    isouter: bool = True,
) -> Join:
    if join_conditions is None:
        return join(left=left, right=right, onclause=onclause, isouter=isouter)
    return join_conditions.join(right=right, onclause=onclause, isouter=isouter)


@dataclass(frozen=True)
class SqlProcLinkKey:
    id: int
    name: str
    substr_from: Optional[int]
    substr_to: Optional[int]
    delta_time: Optional[float]
    cut_off: Optional[float]

    @cached_property
    def good(self) -> bool:
        return self.substr_from != 0 or self.substr_to != 0

    @cached_property
    def bad(self) -> bool:
        return self.substr_from == 0 and self.substr_to == 0

    @cached_property
    def sql_label(self, is_bridge: bool = False) -> str:
        cfg_cols = CfgProcessColumn.get_by_ids([self.id])[0]
        return cfg_cols.gen_sql_label(is_bridge)


class SqlProcLink:
    select_col_ids: list[int]
    select_col_names: list[str]
    start_tm: str
    end_tm: str
    link_keys: list[SqlProcLinkKey]
    next_link_keys: list[SqlProcLinkKey]
    sql: str
    params: str
    temp_table_name: str
    time_ranges: list[tuple[str]]
    trans_data: TransactionData
    condition_procs: list[ConditionProc]
    is_start_proc: bool = False

    def __init__(self, *, transaction_data: TransactionData):
        self.trans_data = transaction_data

    @property
    def process_id(self) -> int:
        return self.trans_data.process_id

    @property
    def table_name(self) -> str:
        return self.trans_data.table_name

    @property
    def time_col(self) -> str:
        return self.trans_data.getdate_column.bridge_column_name

    @cached_property
    def has_link_keys(self) -> bool:
        """Determine if we have link keys"""
        return len(self.link_keys) > 0 or len(self.next_link_keys)

    @cached_property
    def serial_column(self) -> Optional[str]:
        """Get serial columns to in transaction table"""
        serial_col = self.trans_data.serial_column
        if serial_col is not None:
            return serial_col.bridge_column_name
        return None

    @cached_property
    def all_link_key_names(self) -> set[str]:
        """Get link key names, take serial as link key if we don't have any (single process)"""
        if not self.has_link_keys and self.serial_column is None:
            return set()

        if not self.has_link_keys:
            return {self.serial_column}

        return {link.name for link in self.link_keys} | {link.name for link in self.next_link_keys}

    @cached_property
    def all_link_key_ids(self) -> set[int]:
        """Get link key names, take serial as link key if we don't have any (single process)"""
        if not self.has_link_keys and self.trans_data.serial_column is None:
            return set()

        if not self.has_link_keys:
            return {self.trans_data.serial_column.id}

        return {link.id for link in self.link_keys} | {link.id for link in self.next_link_keys}

    @cached_property
    def all_link_keys_labels(self) -> set[str]:
        """Get all link keys sql label
        TODO: This method is wrong, fix it later
        """
        if not self.has_link_keys and self.serial_column is None:
            return set()

        if not self.has_link_keys:
            serial_col_alias = gen_alias_col_name(self.trans_data, self.serial_column)
            return {serial_col_alias}

        return {key.sql_label for key in self.link_keys + self.next_link_keys}

    @cached_property
    def condition_procs_column_name(self) -> set[str]:
        """Get all filter condition columns"""
        cond_procs_column_name = set()
        for cond in self.condition_procs:
            cond_ids = cond.dic_col_id_filters.keys()
            cond_col_names = (self.trans_data.get_column_name(idx) for idx in cond_ids)
            cond_procs_column_name.update(cond_col_names)
        return cond_procs_column_name

    @cached_property
    def all_cfg_columns(self) -> list[CfgProcessColumn]:
        all_column_names = self.all_link_key_names | set(self.select_col_names) | self.condition_procs_column_name
        return [self.trans_data.get_cfg_column_by_name(col_name) for col_name in all_column_names]

    def gen_proc_time_label(self, is_start_proc: bool = False) -> str:
        return TIME_COL if is_start_proc else gen_proc_time_label(self.process_id)

    @cached_property
    def table_model(self) -> sa.Table:
        return self.trans_data.table_model

    @property
    def link_cfg_columns(self) -> list[CfgProcessColumn]:
        return CfgProcessColumn.get_by_ids(self.all_link_key_ids)

    def filter_conditions(self, cte: CTE) -> Optional[Operators]:
        """Filter master after we get cte from single process"""
        conditions = set()

        for cond_proc in self.condition_procs:
            assert cond_proc.proc_id == self.process_id
            for col_id, filters in cond_proc.dic_col_id_filters.items():
                cfg_column = self.trans_data.get_cfg_column_by_id(col_id)
                col_type = cfg_column.column_type
                if cfg_column.is_data_source_name_column():
                    raise NotImplementedError('Did not implement filter for data source name column')
                elif cfg_column.is_master_data_column():
                    col_name_alias = cfg_column.master_data_column_id_label()
                    col = cte.c.get(col_name_alias)
                    assert col is not None, f'{col_name_alias} must exist'
                    # must join with master in order to filter
                    relation_mapping_master = RelationMasterSingleton.instance(
                        DataGroupType(col_type),
                    )
                    assert relation_mapping_master is not None
                    join_col = relation_mapping_master.id
                    conditions.add(col == join_col)

                    filter_col = relation_mapping_master.id
                    # TODO: we assume all master column is text?
                    filter_col_type = RawDataTypeDB.TEXT.value
                else:
                    col_name_alias = gen_alias_col_name(self.trans_data, cfg_column.bridge_column_name)
                    col = cte.c.get(col_name_alias)
                    assert col is not None, f'{col_name_alias} must exist'
                    filter_col = col
                    filter_col_type = cfg_column.raw_data_type
                conditions.add(gen_sql_condition_per_col(filter_col, filter_col_type, filters))

        if not conditions:
            return None
        return and_(*conditions)

    def apply_filter(self, cte: CTE) -> CTE:
        filter_conditions = self.filter_conditions(cte)
        if filter_conditions is None:
            return cte
        name_aliased = f'{FILTER_PREFIX}_{cte.description}'
        stmt = select([cte])
        stmt.append_whereclause(filter_conditions)
        return stmt.cte(name_aliased)

    @log_execution_time(SQL_GENERATOR_PREFIX)
    def gen_cte(
        self,
        idx: int,
        duplicated_serial_show: DuplicateSerialShow,
        is_start_proc: bool = False,
        use_row_number: bool = True,
    ):
        query_builder = TransactionDataQueryBuilder(self.trans_data)

        if is_start_proc:
            query_builder.add_column(column=self.trans_data.id_col_name)

        time_col = self.table_model.c.get(self.trans_data.getdate_column.bridge_column_name)
        query_builder.add_column(column=time_col, label=self.gen_proc_time_label(is_start_proc))
        query_builder.between(start_tm=self.start_tm, end_tm=self.end_tm)

        for cfg_col in self.all_cfg_columns:
            if cfg_col.is_data_source_name_column():
                query_builder.join_data_source_name(label=cfg_col.gen_sql_label())
            elif cfg_col.is_master_data_column():
                query_builder.join_master(
                    data_group_type=DataGroupType(cfg_col.column_type),
                    name_column=cfg_col.gen_sql_label(),
                    master_id_column=cfg_col.master_data_column_id_label(),
                )
            elif cfg_col.existed_in_transaction_table():
                query_builder.add_column(
                    column=cfg_col.bridge_column_name,
                    label=cfg_col.gen_sql_label(),
                )

        link_cols = []
        for cfg_col in self.link_cfg_columns:
            col_name = (
                cfg_col.master_data_column_id_label() if cfg_col.is_master_data_column() else cfg_col.gen_sql_label()
            )
            link_cols.append(query_builder.column(col_name))

        if use_row_number and duplicated_serial_show == DuplicateSerialShow.SHOW_BOTH and self.link_keys:
            query_builder.add_column(
                column=(
                    func.row_number().over(
                        partition_by=link_cols,
                        order_by=time_col.desc(),
                    )
                ),
                label=gen_row_number_col_name(self.process_id),
            )

        if duplicated_serial_show != DuplicateSerialShow.SHOW_BOTH:
            distinct_cols = [col for col in link_cols if col.name != self.time_col]
            if distinct_cols:
                query_builder.distinct(columns=distinct_cols)
                if duplicated_serial_show == DuplicateSerialShow.SHOW_FIRST:
                    query_builder.order_by(columns=distinct_cols + [time_col])
                else:
                    query_builder.order_by(columns=distinct_cols + [time_col.desc()])

        cte = query_builder.build().cte(f'{CTE_PROCESS_PREFIX}{idx}')
        cte: CTE = self.apply_filter(cte)

        return cte


def cast_col_to_text(col: ColumnClause, raw_data_type: str) -> Union[ColumnClause, ColumnOperators]:
    if RawDataTypeDB.is_text_data_type(raw_data_type):
        return col
    return cast(col, sqltypes.Text).label(col.name)


def force_take_substr(
    col: ColumnClause,
    key: SqlProcLinkKey,
    raw_data_type: str,
) -> Union[ColumnClause, ColumnOperators]:
    assert key.good, 'We only use good key here'
    substr_col = cast_col_to_text(col, raw_data_type)
    distance = key.substr_to - key.substr_from + 1
    return func.substr(substr_col, key.substr_from, distance)


def make_comparison_column_with_cast_and_substr(
    from_col: ColumnClause,
    from_key: SqlProcLinkKey,
    from_data_type: str,
    to_col: ColumnClause,
    to_key: SqlProcLinkKey,
    to_data_type: str,
):
    modified_col1 = from_col if from_key.bad else force_take_substr(from_col, from_key, from_data_type)
    modified_col2 = to_col if to_key.bad else force_take_substr(to_col, to_key, to_data_type)

    modified_type1 = from_data_type if from_key.bad else RawDataTypeDB.TEXT.value
    modified_type2 = to_data_type if to_key.bad else RawDataTypeDB.TEXT.value

    is_col1_text = RawDataTypeDB.is_text_data_type(modified_type1)
    is_col2_text = RawDataTypeDB.is_text_data_type(modified_type2)

    if (is_col1_text and is_col2_text) or (not is_col1_text and not is_col2_text):
        return modified_col1 == modified_col2

    modified_col1 = cast_col_to_text(modified_col1, modified_type1)
    modified_col2 = cast_col_to_text(modified_col2, modified_type2)
    return modified_col1 == modified_col2


def make_interval_delta_time_col(col, delta_time: float, cut_off: float):
    # delta_time and cut_off is minute
    seconds = delta_time * 60 + cut_off * 60
    return sa.type_coerce(col, sa.DateTime()) + func.make_interval(0, 0, 0, 0, 0, 0, seconds)


def make_comparisons_column_delta_time_and_cut_off(
    from_col: ColumnClause,
    from_key: SqlProcLinkKey,
    to_col: ColumnClause,
    to_key: SqlProcLinkKey,
):
    def make_comparison(col: ColumnClause, key: SqlProcLinkKey, other_col: ColumnClause):
        cut_off = abs(key.cut_off if key.cut_off is not None else 0)
        max_col = make_interval_delta_time_col(col, key.delta_time, cut_off)
        min_col = make_interval_delta_time_col(col, key.delta_time, -cut_off)
        return [
            other_col > col,
            other_col < max_col,
            other_col > min_col,
        ]

    if from_key.delta_time:
        return make_comparison(from_col, from_key, to_col)

    if to_key.delta_time:
        return make_comparison(to_col, to_key, from_col)

    return []


def make_comparisons_column(
    from_col: ColumnClause,
    from_key: SqlProcLinkKey,
    from_data_type: str,
    to_col: ColumnClause,
    to_key: SqlProcLinkKey,
    to_data_type: str,
):
    if from_key.delta_time or to_key.delta_time:
        # TODO: check data type
        comparisons = make_comparisons_column_delta_time_and_cut_off(
            from_col,
            from_key,
            to_col,
            to_key,
        )
        return comparisons
    else:
        comparison = make_comparison_column_with_cast_and_substr(
            from_col,
            from_key,
            from_data_type,
            to_col,
            to_key,
            to_data_type,
        )
        return [comparison]


def make_delta_time_diff(
    from_col: ColumnClause,
    from_key: SqlProcLinkKey,
    to_col: ColumnClause,
    to_key: SqlProcLinkKey,
):
    def _make_delta_time_diff(col: ColumnClause, key: SqlProcLinkKey, other_col: ColumnClause):
        added_col = make_interval_delta_time_col(col, key.delta_time, cut_off=0)
        diff_col = other_col - added_col
        extract_col = sa.extract('epoch', diff_col)
        return sa.func.abs(extract_col)

    if from_key.delta_time is not None:
        return _make_delta_time_diff(from_col, from_key, to_col)

    if to_key.delta_time is not None:
        return _make_delta_time_diff(to_col, to_key, from_col)

    return None


@log_execution_time(SQL_GENERATOR_PREFIX)
def gen_tracing_cte(
    tracing_table_alias: str,
    cte_proc_list: list[CTE],
    sql_objs: list[SqlProcLink],
    duplicated_serial_show: DuplicateSerialShow,
    dict_cond_procs: dict,
):
    start_proc_table = cte_proc_list[0] if cte_proc_list else None
    assert start_proc_table is not None
    stmt = select(cte_proc_list)

    join_conditions: Optional[Join] = None

    for i in range(1, len(sql_objs)):
        sql_obj = sql_objs[i]
        cte_proc = cte_proc_list[i]
        link_keys = sql_obj.link_keys

        prev_sql_obj = sql_objs[i - 1]
        prev_cte_proc = cte_proc_list[i - 1]
        prev_link_keys = prev_sql_obj.next_link_keys or prev_sql_obj.link_keys

        comparisons = []
        for from_key, to_key in zip(link_keys, prev_link_keys):
            from_cfg_col = sql_obj.trans_data.get_cfg_column_by_name(from_key.name)
            to_cfg_col = prev_sql_obj.trans_data.get_cfg_column_by_name(to_key.name)
            if from_cfg_col.is_master_data_column() and to_cfg_col.is_master_data_column():
                from_col = cte_proc.c.get(from_cfg_col.master_data_column_id_label())
                to_col = prev_cte_proc.c.get(to_cfg_col.master_data_column_id_label())
            elif not from_cfg_col.is_master_data_column() and not to_cfg_col.is_master_data_column():
                from_col = cte_proc.c.get(from_cfg_col.gen_sql_label())
                to_col = prev_cte_proc.c.get(to_cfg_col.gen_sql_label())
            else:
                raise AssertionError(
                    f'No link between {from_cfg_col.column_name} and {to_cfg_col.column_name}',
                )

            comparisons.extend(
                make_comparisons_column(
                    from_col,
                    from_key,
                    from_cfg_col.raw_data_type,
                    to_col,
                    to_key,
                    to_cfg_col.raw_data_type,
                ),
            )

        if duplicated_serial_show == duplicated_serial_show.SHOW_BOTH:
            from_col = cte_proc.c.get(gen_row_number_col_name(sql_obj.process_id))
            to_col = prev_cte_proc.c.get(gen_row_number_col_name(prev_sql_obj.process_id))
            if from_col is not None and to_col is not None:
                comparisons.append(from_col == to_col)

        join_conditions = join_table(
            join_conditions,
            left=prev_cte_proc,
            right=cte_proc,
            onclause=and_(*comparisons),
            isouter=not is_has_condition(
                sql_obj.process_id,
                dict_cond_procs,
            ),  # TODO: this is complicated
        )

    if join_conditions is not None:
        stmt = stmt.select_from(join_conditions)
    return stmt.cte(tracing_table_alias)


def gen_conditions_per_column(filters):
    ands = []
    for cfg_filter in filters:
        comp_ins = []
        comp_likes = []
        comp_regexps = []
        cfg_filter_detail: CfgFilterDetail
        for cfg_filter_detail in cfg_filter.cfg_filter_details:
            val = cfg_filter_detail.filter_condition
            if cfg_filter_detail.filter_function == FilterFunc.REGEX.name:
                comp_regexps.append(val)
            elif not cfg_filter_detail.filter_function or cfg_filter_detail.filter_function == FilterFunc.MATCHES.name:
                comp_ins.append(val)
            else:
                comp_likes.extend(
                    gen_sql_like_value(
                        val,
                        FilterFunc[cfg_filter_detail.filter_function],
                        position=cfg_filter_detail.filter_from_pos,
                    ),
                )
        ands.append((comp_ins, comp_likes, comp_regexps))
    return ands


def gen_sql_condition_per_col(
    col: ColumnClause,
    datatype: str,
    filters: list[ConditionProcDetail],
) -> Operators:
    text_col = cast_col_to_text(col, datatype)

    and_conditions = gen_conditions_per_column(filters)
    ands = []
    for in_vals, like_vals, regex_vals in and_conditions:
        ors = []
        if in_vals:
            ors.append(col.in_(in_vals))
        if like_vals:
            ors.extend(text_col.like(like_val) for like_val in like_vals)
        if regex_vals:
            # sqlalchemy 1.3 does not support regex yet
            # in 1.4 we could use col.regex_match(regex_val)
            ors.extend(text_col.operate(operators.custom_op('~'), regex_val) for regex_val in regex_vals)
        if ors:
            ands.append(or_(*ors))
    return and_(*ands)


def gen_tracing_cte_with_delta_time_cut_off(cte_tracing: CTE, sql_objs: list[SqlProcLink]) -> CTE:
    timediff_cols = []
    for sql_obj, prev_sql_obj in zip(sql_objs[1:], sql_objs):
        link_keys = sql_obj.link_keys
        prev_link_keys = prev_sql_obj.next_link_keys or prev_sql_obj.link_keys
        for from_key, to_key in zip(link_keys, prev_link_keys):
            if from_key.delta_time is None and to_key.delta_time is None:
                continue
            from_cfg_col = sql_obj.trans_data.get_cfg_column_by_name(from_key.name)
            to_cfg_col = prev_sql_obj.trans_data.get_cfg_column_by_name(to_key.name)

            if from_cfg_col.is_master_data_column() or to_cfg_col.is_master_data_column():
                raise NotImplementedError('Do not implement delta time link for master column')

            from_col = cte_tracing.c.get(from_cfg_col.gen_sql_label())
            to_col = cte_tracing.c.get(to_cfg_col.gen_sql_label())
            timediff_cols.append(make_delta_time_diff(from_col, from_key, to_col, to_key))

    if not timediff_cols:
        return cte_tracing

    timediff_labels = [f'{TIMEDIFF_PREFIX}_{i}' for i in range(len(timediff_cols))]
    timediff_cols = [col.label(label) for col, label in zip(timediff_cols, timediff_labels)]

    cte_tracing_timediff = select([*cte_tracing.columns, *timediff_cols]).cte(CTE_TRACING_TIMEDIFF)

    rank_by_timediff = (
        sa.func.rank()
        .over(
            partition_by=cte_tracing_timediff.c.get(TransactionData.id_col_name),
            order_by=[cte_tracing_timediff.c.get(col) for col in timediff_labels],
        )
        .label(RANK_BY_TIMEDIFF)
    )
    cte_tracing_rank = select([*cte_tracing_timediff.columns, rank_by_timediff]).cte(CTE_TRACING_RANK_BY_TIMEDIFF)

    cte_tracing_filtered_rank = (
        select([*cte_tracing_rank.columns])
        .where(cte_tracing_rank.c.get(RANK_BY_TIMEDIFF) == 1)
        .cte(CTE_TRACING_MIN_TIMEDIFF)
    )

    return cte_tracing_filtered_rank


@log_execution_time(SQL_GENERATOR_PREFIX)
def gen_show_stmt(cte_tracing: CTE, sql_objs: list[SqlProcLink]) -> Select:
    # we must add id and time to shown_cols
    shown_cols = [
        cte_tracing.c.get(TransactionData.id_col_name).label(TransactionData.id_col_name),
    ]

    for idx, sql_obj in enumerate(sql_objs):
        is_start_proc = idx == 0
        # we get the time column
        time_col_alias_name = sql_obj.gen_proc_time_label(is_start_proc)
        time_col = cte_tracing.c.get(time_col_alias_name)
        assert time_col is not None, "If time_col is None, sql_obj.get() function isn't written correctly"
        time_col_alias = time_col.label(time_col_alias_name)
        shown_cols.append(time_col_alias)

        # TODO: do we need this?
        # we add our start proc as time_{id}
        if is_start_proc:
            time_col_alias_with_id = sql_obj.gen_proc_time_label(is_start_proc=False)
            time_col_alias = time_col.label(time_col_alias_with_id)
            shown_cols.append(time_col_alias)

    for sql_obj in sql_objs:
        for cfg_col in sql_obj.all_cfg_columns:
            col = cte_tracing.c.get(cfg_col.gen_sql_label())
            if col is None:
                continue

            # we only cast normal column
            if not cfg_col.is_master_data_column():
                data_type = DataType(cfg_col.data_type)
                # try to cast if graph's data type is text
                if data_type is DataType.TEXT:
                    col = cast_col_to_text(col, cfg_col.raw_data_type)

            shown_cols.append(col)

            # get additional id for master columns
            if cfg_col.is_master_data_column():
                shown_cols.append(cte_tracing.c.get(cfg_col.master_data_column_id_label()))

    return select(shown_cols)


def gen_id_stmt(
    cte_tracing: CTE,
) -> Select:
    col = cte_tracing.c.get(TransactionData.id_col_name)
    return select([col])


@log_execution_time(SQL_GENERATOR_PREFIX)
def gen_cte_show_category(
    cte: CTE,
    sql_obj: SqlProcLink,
) -> CTE:
    category_cols = []
    join_conditions = None
    for col_id in sql_obj.select_col_ids:
        cfg_column = sql_obj.trans_data.get_cfg_column_by_id(col_id)
        col_data_type = cfg_column.raw_data_type
        if not RawDataTypeDB.is_category_data_type(col_data_type):
            continue

        # find group id
        m_column_groups = MColumnGroup.get_by_data_ids(data_ids=[col_id])
        target_id = m_column_groups[0].group_id if m_column_groups else col_id

        col_name_alias = gen_alias_col_name(sql_obj.trans_data, cfg_column.bridge_column_name)
        col = cte.c.get(col_name_alias)

        right: AliasedClass = aliased(
            ap_models.SemiMaster,
            name=f'{SEMI_TABLE_ALIAS_PREFIX}_{col_id}',
        )
        semi_id_col = right.group_id
        semi_factor_col = right.factor
        semi_value_col = right.value

        category_cols.append(semi_value_col.label(col_name_alias))
        join_conditions = join_table(
            join_conditions,
            left=cte,
            right=right,
            onclause=and_(semi_id_col == target_id, semi_factor_col == col),
        )

    if join_conditions is None:
        return cte

    selected_cols = []
    for col in cte.columns:
        if any(col.name == cat_col.name for cat_col in category_cols):
            continue
        selected_cols.append(col)
    selected_cols.extend(category_cols)

    table_alias = f"{cte.name}_with_category"  # noqa
    stmt = select(selected_cols).select_from(join_conditions)
    return stmt.cte(table_alias)


def gen_sql_proc_link_count(trace: CfgTrace, limit: Optional[int] = None) -> Select:
    self_proc_link_keys: list[SqlProcLinkKey] = []
    target_proc_link_keys: list[SqlProcLinkKey] = []

    self_trans_data = TransactionData(trace.self_process_id)
    target_trans_data = TransactionData(trace.target_process_id)

    for trace_key in trace.trace_keys:
        self_proc_link_keys.append(
            SqlProcLinkKey(
                id=trace_key.self_column_id,
                name=self_trans_data.get_column_name(trace_key.self_column_id),
                substr_from=trace_key.self_column_substr_from,
                substr_to=trace_key.self_column_substr_to,
                delta_time=trace_key.delta_time,
                cut_off=trace_key.cut_off,
            ),
        )
        target_proc_link_keys.append(
            SqlProcLinkKey(
                id=trace_key.target_column_id,
                name=target_trans_data.get_column_name(trace_key.target_column_id),
                substr_from=trace_key.target_column_substr_from,
                substr_to=trace_key.target_column_substr_to,
                # delta time and cut_off apply only self process link key, self_link_key + delta_time = target_link_key
                delta_time=None,
                cut_off=None,
            ),
        )
    self_query_builder = TransactionDataProcLinkQueryBuilder(
        self_trans_data,
        self_proc_link_keys,
        table_alias='self',
        limit=limit,
    )
    target_query_builder = TransactionDataProcLinkQueryBuilder(
        target_trans_data,
        target_proc_link_keys,
        table_alias='target',
        limit=limit,
    )
    return self_query_builder.build_count_query(target_query_builder)


def is_has_condition(proc_id, dict_cond_procs):
    if proc_id not in dict_cond_procs:
        return False

    return any(cond_proc.dic_col_id_filters for cond_proc in dict_cond_procs[proc_id])
