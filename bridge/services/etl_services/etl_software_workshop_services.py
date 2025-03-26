from __future__ import annotations

from typing import Iterator, Optional, Tuple, Type, Union

import pandas as pd
import sqlalchemy as sa
from pandas import DataFrame

from ap import log_execution_time
from ap.common.common_utils import format_df
from ap.common.constants import (
    FETCH_MANY_SIZE,
    LATEST_RECORDS_SQL_LIMIT,
    SOFTWARE_WORKSHOP_LIMIT_PULL_DB,
    SOFTWARE_WORKSHOP_LIMIT_SCAN_MASTER,
    SQL_LIMIT_SCAN_DATA_TYPE,
    JobType,
)
from ap.common.memoize import memoize
from ap.common.pydn.dblib.db_proxy_readonly import ReadOnlyDbProxy
from ap.common.pydn.dblib.postgresql import PostgreSQL
from ap.common.services.normalization import normalize_big_rows, normalize_list
from ap.setting_module.models import (
    CfgDataSource,
    MappingFactoryMachine,
    MappingPart,
    MappingProcessData,
)
from bridge.models.bridge_station import BridgeStationModel
from bridge.models.cfg_data_table import CfgDataTable as BSCfgDataTable
from bridge.models.cfg_partition_table import CfgPartitionTable as BSCfgPartitionTable
from bridge.services.etl_services.etl_db_service import (
    EtlDbService,
    get_two_partitions_for_scan,
)

factory_table = sa.Table(
    'fctries',
    sa.MetaData(),
    sa.Column('fctry_id', sa.TEXT),
    sa.Column('fctry_name', sa.TEXT),
)

line_groups_table = sa.Table(
    'line_grps',
    sa.MetaData(),
    sa.Column('fctry_id', sa.TEXT),
    sa.Column('line_grp_id', sa.TEXT),
)

lines_table = sa.Table(
    'lines',
    sa.MetaData(),
    sa.Column('line_id', sa.TEXT),
    sa.Column('line_name', sa.TEXT),
    sa.Column('line_grp_id', sa.TEXT),
)

equips_table = sa.Table(
    'equips',
    sa.MetaData(),
    sa.Column('equip_id', sa.TEXT),
    sa.Column('line_id', sa.TEXT),
)

child_equips_table = sa.Table(
    'child_equips',
    sa.MetaData(),
    sa.Column('child_equip_id', sa.TEXT),
    sa.Column('child_equip_name', sa.TEXT),
    sa.Column('equip_id', sa.TEXT),
)

quality_measurements_table = sa.Table(
    'quality_measurements',
    sa.MetaData(),
    sa.Column('quality_measurement_id', sa.BIGINT),
    sa.Column('child_equip_id', sa.TEXT),
    sa.Column('event_time', sa.TIMESTAMP),
    sa.Column('part_no', sa.TEXT),
    sa.Column('lot_no', sa.TEXT),
    sa.Column('tray_no', sa.TEXT),
    sa.Column('serial_no', sa.TEXT),
)

measurements_table = sa.Table(
    'measurements',
    sa.MetaData(),
    sa.Column('quality_measurement_id', sa.BIGINT),
    sa.Column('code', sa.TEXT),
    sa.Column('unit', sa.TEXT),
    sa.Column('value', sa.REAL),
)

string_measurements_table = sa.Table(
    'string_measurements',
    sa.MetaData(),
    sa.Column('quality_measurement_id', sa.BIGINT),
    sa.Column('code', sa.TEXT),
    sa.Column('unit', sa.TEXT),
    sa.Column('value', sa.TEXT),
)

child_equip_meas_items_table = sa.Table(
    'child_equip_meas_items',
    sa.MetaData(),
    sa.Column('child_equip_id', sa.TEXT),
    sa.Column('meas_item_code', sa.TEXT),
    sa.Column('meas_item_name', sa.TEXT),
)


class SoftwareWorkshopService(EtlDbService):
    @log_execution_time(prefix='etl_software_workshop_service')
    @log_execution_time()
    def get_transaction_data(self, factory_db_instance, start_dt, end_dt, is_only_pull_sample_data=False):
        """
        Gets raw data from data source
        :param factory_db_instance:
        :param is_only_pull_sample_data:
        :param start_dt:
        :param end_dt:
        :return:
        """
        self.check_db_connection()

        stmt = self.get_transaction_data_stmt(
            start_date=start_dt,
            end_date=end_dt,
            limit=SOFTWARE_WORKSHOP_LIMIT_PULL_DB,
        )
        sql, params = factory_db_instance.gen_sql_and_params(stmt)
        data = factory_db_instance.fetch_many(sql, FETCH_MANY_SIZE, params=params)
        cols = next(data)
        if not cols:
            yield None
            return

        yield cols

        yield from data

    @log_execution_time(prefix='etl_software_workshop_service')
    @BridgeStationModel.use_db_instance_generator()
    def get_master_data(
        self,
        db_instance: PostgreSQL = None,
    ) -> Iterator[
        Tuple[
            dict[Type[Union[MappingProcessData, MappingPart, MappingFactoryMachine, str]], DataFrame],
            Union[int, float],
        ]
    ]:
        self.check_db_connection()

        cols, rows = self.get_vertical_sample_data(limit=SOFTWARE_WORKSHOP_LIMIT_SCAN_MASTER)
        df = pd.DataFrame(rows, columns=cols)

        # Add NULL for master column not select
        dic_use_cols = {col.column_name: col.data_type for col in self.cfg_data_table_columns}
        master_columns, master_values = self.get_dummy_master_column_value()
        self.add_dummy_master_columns(df, master_columns, master_values, dic_use_cols)

        # check and add columns to dataframe if not present in list
        self.add_dummy_horizon_columns(df)

        df = self.convert_to_standard_data(df)

        yield from self.split_master_data([(df, None, 99)])

        self.set_done_status_for_scan_master_job(db_instance=db_instance)

    @log_execution_time(prefix='etl_software_workshop_service')
    @BridgeStationModel.use_db_instance_generator()
    def get_data_for_data_type(self, generator_df=None, db_instance: PostgreSQL = None):
        self.check_db_connection()

        limit = LATEST_RECORDS_SQL_LIMIT if self.is_horizon_data() else SQL_LIMIT_SCAN_DATA_TYPE
        ignore_cols = None
        if generator_df is not None:
            for df_original, *_ in generator_df or []:
                df, dic_df_horizons = self.transform_horizon_columns_for_import(
                    df_original,
                    ignore_cols=ignore_cols,
                )
                yield df, dic_df_horizons, 99

            return

        # get partition that already scan master
        job_type = JobType.USER_APPROVED_MASTER if self.is_user_approved_master else JobType.SCAN_DATA_TYPE
        if isinstance(self.cfg_data_table, BSCfgDataTable):
            # TODO: implement later
            cfg_partitions = BSCfgPartitionTable.get_partition_for_job(
                db_instance,
                self.cfg_data_table.id,
                job_type,
            )
        else:
            cfg_partitions = self.cfg_data_table.get_partition_for_job(job_type, many=True)
        cfg_partitions = get_two_partitions_for_scan(cfg_partitions)
        if len(cfg_partitions):
            dic_use_cols = {col.column_name: col.data_type for col in self.cfg_data_table_columns}
            master_columns, master_values = self.get_dummy_master_column_value()
            one_step_percent = 100 // len(cfg_partitions)
            sent_count = 0
            for idx, cfg_partition in enumerate(cfg_partitions, start=1):
                if cfg_partition.is_no_min_max_date_time():
                    # Skip get data for empty partition tables
                    self.set_done_status_for_scan_data_type_job(cfg_partition, db_instance=db_instance)
                    continue

                cols, rows = self.get_vertical_sample_data(limit=SOFTWARE_WORKSHOP_LIMIT_SCAN_MASTER)
                if not rows:
                    self.set_done_status_for_scan_data_type_job(cfg_partition, db_instance=db_instance)
                    continue

                sent_count += len(rows)
                df = pd.DataFrame(rows, columns=cols, dtype='object')
                df = format_df(df)

                # Add NULL for master column not select
                self.add_dummy_master_columns(df, master_columns, master_values, dic_use_cols)
                # check and add columns to dataframe if not present in list
                self.add_dummy_horizon_columns(df)

                df = self.convert_to_standard_data(df)
                df, dic_df_horizons = self.transform_horizon_columns_for_import(
                    df,
                    ignore_cols=ignore_cols,
                )
                yield df, dic_df_horizons, one_step_percent * idx

                self.set_done_status_for_scan_data_type_job(cfg_partition, db_instance=db_instance)
                if limit and sent_count >= limit:
                    return

    def get_vertical_sample_data(
        self,
        process_factid: str | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
        limit: int | None = None,
    ) -> tuple[list[str], list[tuple]]:
        with ReadOnlyDbProxy(self.cfg_data_source) as factory_db_instance:
            stmt = self.get_transaction_data_stmt(process_factid, start_date, end_date, limit)
            sql, params = factory_db_instance.gen_sql_and_params(stmt)
            cols, rows = factory_db_instance.run_sql(sql, row_is_dict=False, params=params)
            return cols, rows

    @classmethod
    @memoize(duration=300)
    def get_info_from_db(
        cls,
        data_source_id: int,
        table_name: str,
        child_equip_id: Optional[str],
        sql_limit: int = 2000,
        is_transform_horizontal: bool = True,
    ) -> tuple[list[str], list[tuple]]:
        data_source = CfgDataSource.query.get(data_source_id)
        with ReadOnlyDbProxy(data_source) as db_instance:
            if not db_instance or not table_name:
                return [], []

            stmt = cls.get_transaction_data_stmt(child_equip_id, limit=sql_limit)
            sql, params = db_instance.gen_sql_and_params(stmt)
            cols, rows = db_instance.run_sql(sql, row_is_dict=False, params=params)

        df = pd.DataFrame(rows, columns=cols)
        if is_transform_horizontal:
            df = cls.transform_transaction_data_to_horizontal(df)
        transform_cols = df.columns.to_list()
        transform_rows = df.values.tolist()

        transform_cols = normalize_list(transform_cols)
        df_rows = normalize_big_rows(transform_rows, transform_cols, strip_quote=False)
        return transform_cols, df_rows

    @staticmethod
    def get_master_data_stmt(
        process_factid: str | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
        limit: int | None = 2000,
    ):
        join_master = (
            sa.join(
                left=quality_measurements_table,
                right=child_equips_table,
                onclause=child_equips_table.c.child_equip_id == quality_measurements_table.c.child_equip_id,
            )
            .join(
                right=equips_table,
                onclause=equips_table.c.equip_id == child_equips_table.c.equip_id,
            )
            .join(
                right=lines_table,
                onclause=lines_table.c.line_id == equips_table.c.line_id,
            )
            .join(
                right=line_groups_table,
                onclause=line_groups_table.c.line_grp_id == lines_table.c.line_grp_id,
            )
            .join(
                right=factory_table,
                onclause=factory_table.c.fctry_id == line_groups_table.c.fctry_id,
            )
        )

        conditions = []
        if process_factid is not None:
            conditions.append(quality_measurements_table.c.child_equip_id == process_factid)
        if start_date is not None:
            conditions.append(quality_measurements_table.c.event_time > start_date)
        if end_date is not None:
            conditions.append(quality_measurements_table.c.event_time <= end_date)

        stmt = sa.select(
            [
                quality_measurements_table.c.quality_measurement_id,
                quality_measurements_table.c.event_time,
                quality_measurements_table.c.part_no,
                quality_measurements_table.c.lot_no,
                quality_measurements_table.c.tray_no,
                quality_measurements_table.c.serial_no,
                factory_table.c.fctry_id,
                factory_table.c.fctry_name,
                lines_table.c.line_id,
                lines_table.c.line_name,
                child_equips_table.c.child_equip_id,
                child_equips_table.c.child_equip_name,
            ],
        ).select_from(join_master)

        if conditions:
            stmt = stmt.where(sa.and_(*conditions))

        if limit is not None:
            stmt = stmt.limit(limit)

        return stmt

    @staticmethod
    def get_transaction_data_stmt(
        process_factid: str | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
        limit: int | None = None,
    ):
        cte = SoftwareWorkshopService.get_master_data_stmt(process_factid, start_date, end_date, limit).cte(
            'master_data',
        )

        measurements_stmt = sa.select(
            [
                cte,
                measurements_table.c.code,
                measurements_table.c.unit,
                # need to cast data to text in order to union
                sa.cast(measurements_table.c.value, sa.sql.sqltypes.TEXT).label(measurements_table.c.value.name),
                child_equip_meas_items_table.c.meas_item_name,
            ],
        ).select_from(
            sa.join(
                left=cte,
                right=measurements_table,
                onclause=cte.c.quality_measurement_id == measurements_table.c.quality_measurement_id,
            ).join(
                right=child_equip_meas_items_table,
                onclause=(
                    measurements_table.c.code == child_equip_meas_items_table.c.meas_item_code
                    and quality_measurements_table.c.child_equip_id == child_equip_meas_items_table.c.child_equip_id
                ),
            ),
        )

        string_measurements_stmt = sa.select(
            [
                cte,
                string_measurements_table.c.code,
                string_measurements_table.c.unit,
                string_measurements_table.c.value,
                child_equip_meas_items_table.c.meas_item_name,
            ],
        ).select_from(
            sa.join(
                left=cte,
                right=string_measurements_table,
                onclause=cte.c.quality_measurement_id == string_measurements_table.c.quality_measurement_id,
            ).join(
                right=child_equip_meas_items_table,
                onclause=(
                    string_measurements_table.c.code == child_equip_meas_items_table.c.meas_item_code
                    and quality_measurements_table.c.child_equip_id == child_equip_meas_items_table.c.child_equip_id
                ),
            ),
        )

        stmt = measurements_stmt.union_all(string_measurements_stmt)
        stmt = stmt.order_by(stmt.c.event_time)

        return stmt

    @staticmethod
    def transform_transaction_data_to_horizontal(software_workshop_vertical_df: pd.DataFrame) -> pd.DataFrame:
        # all master columns in dataframe
        master_columns = [
            factory_table.c.fctry_id.name,
            factory_table.c.fctry_name.name,
            lines_table.c.line_id.name,
            lines_table.c.line_name.name,
            child_equips_table.c.child_equip_id.name,
            child_equips_table.c.child_equip_name.name,
        ]

        # columns for getting unique records
        index_columns = [
            child_equips_table.c.child_equip_id.name,
            quality_measurements_table.c.event_time.name,
            quality_measurements_table.c.serial_no.name,
            quality_measurements_table.c.part_no.name,
        ]

        # horizontal columns in vertical dataframe
        horizontal_columns = [
            quality_measurements_table.c.lot_no.name,
            quality_measurements_table.c.tray_no.name,
        ]

        # all required columns from those columns above. We use this hack to preserve order
        required_columns = list(dict.fromkeys([*master_columns, *index_columns, *horizontal_columns]))

        # columns used for pivoting
        pivot_column = measurements_table.c.code.name
        pivot_value = measurements_table.c.value.name

        df_pivot = (
            # only select required columns, ignore unneeded ones
            software_workshop_vertical_df[[*index_columns, pivot_column, pivot_value]]
            # drop duplicated columns, to make sure pivot can work properly
            .drop_duplicates(subset=[*index_columns, pivot_column], keep='last')
            .pivot(index=index_columns, columns=pivot_column, values=pivot_value)
            .reset_index()
        )

        # merge to get master data
        df_with_master = software_workshop_vertical_df[required_columns].drop_duplicates(
            subset=index_columns,
            keep='last',
        )
        df_horizontal = df_pivot.merge(right=df_with_master, on=index_columns)

        # sort vertical columns for better output, we don't want our data being shown as col_03 col_01 col_02
        sorted_vertical_columns = sorted(c for c in df_horizontal.columns if c not in required_columns)
        df_horizontal = df_horizontal[[*required_columns, *sorted_vertical_columns]]

        return df_horizontal
