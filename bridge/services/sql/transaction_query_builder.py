from __future__ import annotations

import uuid
from typing import TYPE_CHECKING, Optional, Union

import sqlalchemy as sa

from ap.common.constants import DataGroupType
from ap.setting_module.models import (
    CfgDataSource,
    CfgProcessColumn,
    RFactoryMachine,
    RProdPart,
)
from bridge.models.transaction_model import TransactionData
from bridge.services.master_catalog import RelationMasterSingleton
from bridge.services.sql.mapping_master import RelationMaster

if TYPE_CHECKING:
    from sqlalchemy.sql import Join, Select
    from sqlalchemy.sql.elements import ColumnClause, Label
    from sqlalchemy.sql.operators import ColumnOperators, Operators
    from sqlalchemy.sql.selectable import CTE
    from typing_extensions import Self

    from ap.setting_module.models import CommonDBModel
    from bridge.services.sql.sql_generator import SqlProcLinkKey


class TransactionDataQueryBuilder:
    def __init__(self, trans_model: TransactionData) -> None:
        self.trans_model = trans_model
        self.table = self.trans_model.table_model

        self.selected_columns: list[Label] = []
        self.join_conditions: Optional[Join] = None
        self.joined_master_relations: set[RelationMaster] = set()
        self.where_clauses = []

        self.distinct_columns = []
        self.orderby_columns = []

        self.joined_r_factory_machine = False
        self.joined_r_prod_part = False

    def join_r_prod_part(self) -> None:
        """Join with r_prod_part before joining master"""
        if self.joined_r_prod_part:
            return
        self.join(right=RProdPart, onclause=self.table_column(TransactionData.prod_part_id_col_name) == RProdPart.id)
        self.joined_r_prod_part = True

    def join_r_factory_machine(self) -> None:
        """Join with r_factory_machine before joining master"""
        if self.joined_r_factory_machine:
            return
        self.join(
            right=RFactoryMachine,
            onclause=self.table_column(TransactionData.factory_machine_id_col_name) == RFactoryMachine.id,
        )
        self.joined_r_factory_machine = True

    def table_column(self, name: str) -> sa.Column:
        column = self.table.c.get(name)
        if column is None:
            raise AssertionError(f'{column} does not existed in {self.table.name}')
        return column

    def column(self, name: str) -> Label:
        for column in self.selected_columns:
            if column.name == name:
                return column
        raise RuntimeError(f"Column {name} doesn't exist.")

    def add_column(
        self,
        *,
        column: Union[ColumnClause, str, Label],
        label: Optional[str] = None,
    ) -> None:
        if isinstance(column, str):
            column = self.table_column(column)
        if label is not None:
            column = column.label(label)
        self.selected_columns.append(column)

    def add_columns(self, columns: list[Label]) -> None:
        self.selected_columns.extend(columns)

    def distinct(self, *, columns: list[sa.Column | Label]) -> None:
        self.distinct_columns = columns

    def order_by(self, *, columns: list[sa.Column | Label]) -> None:
        self.orderby_columns = columns

    def join(self, *, right: type[CommonDBModel], onclause: Union[Operators, bool]):
        if self.join_conditions is None:
            self.join_conditions = sa.outerjoin(left=self.table, right=right, onclause=onclause)
        else:
            self.join_conditions = self.join_conditions.outerjoin(right=right, onclause=onclause)

    def join_data_source_name(self, label: Optional[str] = None) -> None:
        self.add_column(column=CfgDataSource.name.label(label))
        self.join(
            right=CfgDataSource,
            onclause=self.table_column(TransactionData.data_source_id_col_name) == CfgDataSource.id,
        )

    def join_master(
        self,
        *,
        data_group_type: DataGroupType,
        name_column: Optional[str] = None,
        master_id_column: Optional[str] = None,
    ) -> None:
        self.join_r_prod_part()
        self.join_r_factory_machine()

        relation_master = RelationMasterSingleton.instance(data_group_type)
        if relation_master is None:
            raise AssertionError(f'No relation master table for {data_group_type}')

        if name_column is not None:
            self.add_column(column=relation_master.name.label(name_column))
        if master_id_column is not None:
            self.add_column(column=relation_master.master_id.label(master_id_column))

        if relation_master not in self.joined_master_relations:
            r_table = relation_master.r_table
            r_column_name = relation_master.r_column_name
            r_col = getattr(r_table, r_column_name)

            self.join_conditions = self.join_conditions.outerjoin(
                right=relation_master.cte,
                onclause=r_col == relation_master.id,
            )

            self.joined_master_relations.add(relation_master)

    def between(self, *, start_tm: str, end_tm: str) -> None:
        time_col = self.table_column(self.trans_model.getdate_column.bridge_column_name)
        self.where_clauses.append(sa.and_(time_col >= start_tm, time_col < end_tm))

    def build(self, limit: Optional[int] = None) -> Select:
        stmt = sa.select(self.selected_columns)
        if self.join_conditions is not None:
            stmt = stmt.select_from(self.join_conditions)

        for clause in self.where_clauses:
            stmt.append_whereclause(clause)

        if self.distinct_columns:
            stmt = stmt.distinct(*self.distinct_columns)
        if self.orderby_columns:
            stmt = stmt.order_by(*self.orderby_columns)

        if limit is not None:
            stmt = stmt.limit(limit)

        del self
        return stmt


class TransactionDataProcLinkQueryBuilder:
    def __init__(
        self,
        trans_model: TransactionData,
        proc_link_keys: list[SqlProcLinkKey],
        table_alias: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> None:
        self.trans_model = trans_model
        self.proc_link_keys = proc_link_keys
        if table_alias is not None:
            self.table_alias = table_alias
        else:
            self.table_alias = uuid.uuid4().hex

        self.limit = limit
        self.cte: Optional[CTE] = None

    def build_proc_link_cte(self) -> Self:
        query_builder = TransactionDataQueryBuilder(self.trans_model)
        for key in self.proc_link_keys:
            cfg_col = self.trans_model.get_cfg_column_by_id(key.id)
            if not cfg_col.is_master_data_column():
                query_builder.add_column(
                    column=cfg_col.bridge_column_name,
                    label=cfg_col.gen_sql_label(),
                )
            else:
                query_builder.join_master(
                    data_group_type=DataGroupType(cfg_col.column_type),
                    master_id_column=cfg_col.master_data_column_id_label(),
                )
        stmt = query_builder.build(self.limit)
        self.cte = stmt.cte(self.table_alias)
        return self

    def get_column_by_label(self, label: str) -> Optional[ColumnClause]:
        return self.cte.c.get(label)

    def get_column_by_cfg_column(self, cfg_column: CfgProcessColumn) -> ColumnClause:
        if cfg_column.is_master_data_column():
            column = self.get_column_by_label(cfg_column.master_data_column_id_label())
        else:
            column = self.get_column_by_label(cfg_column.gen_sql_label())

        if column is None:
            raise AssertionError(f"column : {cfg_column.column_name} doesn't exist")

        return column

    def make_link_comparison(self, other: Self) -> list[ColumnOperators]:
        comparisons = []
        for key, other_key in zip(self.proc_link_keys, other.proc_link_keys):
            cfg_column: CfgProcessColumn = self.trans_model.get_cfg_column_by_id(key.id)
            other_cfg_column: CfgProcessColumn = other.trans_model.get_cfg_column_by_id(other_key.id)

            column = self.get_column_by_cfg_column(cfg_column)
            other_column = other.get_column_by_cfg_column(other_cfg_column)

            if cfg_column.is_master_data_column() and other_cfg_column.is_master_data_column():
                # master will be linked by id only
                comparisons.append(column == other_column)
            elif not cfg_column.is_master_data_column() and not other_cfg_column.is_master_data_column():
                # non-master column must be cast before linking
                # TODO(khanhdq): refactor this
                from bridge.services.sql.sql_generator import make_comparisons_column

                comparisons.extend(
                    make_comparisons_column(
                        column,
                        key,
                        cfg_column.raw_data_type,
                        other_column,
                        other_key,
                        other_cfg_column.raw_data_type,
                    ),
                )

            else:
                raise RuntimeError('Cannot data-link between master and normal column')

        return comparisons

    def build_count_query(self, other: Self) -> Select:
        if self.cte is None:
            self.build_proc_link_cte()
        if other.cte is None:
            other.build_proc_link_cte()
        comparisons = self.make_link_comparison(other)
        exists_stmt = sa.exists([1]).where(sa.and_(*comparisons))
        count_stmt = sa.select([sa.func.count()]).select_from(self.cte)
        return count_stmt.where(exists_stmt)
