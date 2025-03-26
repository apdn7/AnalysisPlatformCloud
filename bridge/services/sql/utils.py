from __future__ import annotations

from typing import TYPE_CHECKING, Iterator, TypeVar

import numpy as np
import pandas as pd
from flask_sqlalchemy import BaseQuery
from sqlalchemy.dialects import postgresql

from ap.common.pydn.dblib.postgresql import PostgreSQL

if TYPE_CHECKING:
    from sqlalchemy.sql import Select

T = TypeVar('T')


def gen_sql_and_params(stmt: Select) -> tuple[str, dict[str, str]]:
    compiled_stmt = stmt.compile(dialect=postgresql.dialect())
    return compiled_stmt.string, compiled_stmt.params


def run_sql_from_query_with_casted(*, query: BaseQuery | Select, db_instance: PostgreSQL, cls: type[T]) -> Iterator[T]:
    stmt = query.statement if isinstance(query, BaseQuery) else query
    sql, params = gen_sql_and_params(stmt)
    _, rows = db_instance.run_sql(sql, row_is_dict=True, params=params)
    for row in rows:
        yield cls(**row)


def df_from_query(*, query: BaseQuery | Select, db_instance: PostgreSQL) -> pd.DataFrame:
    stmt = query.statement if isinstance(query, BaseQuery) else query
    sql, params = gen_sql_and_params(stmt)
    cols, rows = db_instance.run_sql(sql, row_is_dict=True, params=params)
    df = pd.DataFrame(rows, columns=cols, dtype=np.object)
    return df
