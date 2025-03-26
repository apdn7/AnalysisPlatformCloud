from __future__ import annotations

from typing import TYPE_CHECKING, Iterator, Optional

from ap.common.constants import RawDataTypeDB
from ap.common.pydn.dblib.db_common import SqlComparisonOperator
from ap.setting_module import models as ap_models
from bridge.models.bridge_station import MasterModel
from bridge.models.model_utils import TableColumn
from bridge.services.sql.utils import df_from_query, run_sql_from_query_with_casted

if TYPE_CHECKING:
    from ap.common.pydn.dblib.postgresql import PostgreSQL


class MProcess(MasterModel):
    class Columns(TableColumn):
        id = (1, RawDataTypeDB.INTEGER)
        prod_family_id = (2, RawDataTypeDB.INTEGER)
        process_factid = (3, RawDataTypeDB.TEXT)
        process_name_jp = (4, RawDataTypeDB.TEXT)
        process_name_en = (5, RawDataTypeDB.TEXT)
        process_name_sys = (6, RawDataTypeDB.TEXT)
        process_name_local = (7, RawDataTypeDB.TEXT)
        process_abbr_jp = (8, RawDataTypeDB.TEXT)
        process_abbr_en = (9, RawDataTypeDB.TEXT)
        process_abbr_local = (10, RawDataTypeDB.TEXT)
        created_at = (97, RawDataTypeDB.DATETIME)
        updated_at = (98, RawDataTypeDB.DATETIME)
        deleted_at = (99, RawDataTypeDB.DATETIME)

    def __init__(self, dict_proc=None) -> None:
        dict_proc = dict_proc if dict_proc else {}
        self.id = dict_proc.get(MProcess.Columns.id.name)
        self.prod_family_id = dict_proc.get(MProcess.Columns.prod_family_id.name)
        self.process_factid = dict_proc.get(MProcess.Columns.process_factid.name)
        self.process_name_jp = dict_proc.get(MProcess.Columns.process_name_jp.name)
        self.process_name_en = dict_proc.get(MProcess.Columns.process_name_en.name)
        self.process_name_sys = dict_proc.get(MProcess.Columns.process_name_sys.name)
        self.process_name_local = dict_proc.get(MProcess.Columns.process_name_local.name)
        self.process_abbr_jp = dict_proc.get(MProcess.Columns.process_abbr_jp.name)
        self.process_abbr_en = dict_proc.get(MProcess.Columns.process_abbr_en.name)
        self.process_abbr_local = dict_proc.get(MProcess.Columns.process_abbr_local.name)

    _table_name = 'm_process'
    primary_keys = [Columns.id]
    not_null_columns = [Columns.process_name_jp]

    @classmethod
    def get_name_column(cls):  # unused todo remove
        """
        Human-friendly column

        :return:
        """
        return cls.Columns.process_name_jp

    @classmethod
    def get_existed_process_ids(cls, db_instance: PostgreSQL, process_ids=None):
        query = ap_models.MProcess.get_existed_process_ids_query(process_ids=process_ids)
        df = df_from_query(query=query, db_instance=db_instance)
        return df[MProcess.Columns.id.name].tolist()

    @classmethod
    def get_by_id(
        cls,
        db_instance: PostgreSQL,
        id: int,  # noqa: A002
    ) -> Optional[ap_models.MProcess]:
        query = ap_models.MProcess.query.filter(ap_models.MProcess.id == int(id))
        results = list(
            run_sql_from_query_with_casted(
                query=query,
                db_instance=db_instance,
                cls=ap_models.MProcess,
            ),
        )
        if not results:
            return None
        if len(results) > 1:
            raise RuntimeError(f'There are 2 duplicated process_id={id}')
        return results[0]

    @classmethod
    def get_in_ids(cls, db_instance, m_process_ids: [tuple, list], is_return_dict=False):
        if not m_process_ids:
            return {} if is_return_dict else []
        dict_cond = {cls.Columns.id.name: [(SqlComparisonOperator.IN, tuple(m_process_ids))]}
        _col, rows = cls.select_records(db_instance, dic_conditions=dict_cond, filter_deleted=False)
        process_s = [MProcess(row) for row in rows]
        if is_return_dict:
            return {process.id: process for process in process_s}
        return process_s

    def get_id(self):  # see AbstractProcess
        return self.id

    @classmethod
    def get_unique_by_data_table_ids(
        cls,
        *,
        db_instance: PostgreSQL,
        data_table_ids: list[int],
    ) -> Iterator[ap_models.MProcess]:
        query = (
            ap_models.MProcess.query.join(
                ap_models.RFactoryMachine,
                ap_models.MProcess.id == ap_models.RFactoryMachine.process_id,
            )
            .join(
                ap_models.MappingFactoryMachine,
                ap_models.RFactoryMachine.id == ap_models.MappingFactoryMachine.factory_machine_id,
            )
            .filter(ap_models.MappingFactoryMachine.data_table_id.in_(data_table_ids))
            .order_by(ap_models.MProcess.id)
        )

        query_mapping_process_data = (
            ap_models.MProcess.query.join(
                ap_models.MData,
                ap_models.MData.process_id == ap_models.MProcess.id,
            )
            .join(
                ap_models.MappingProcessData,
                ap_models.MappingProcessData.data_id == ap_models.MData.id,
            )
            .filter(ap_models.MappingProcessData.data_table_id.in_(data_table_ids))
            .order_by(ap_models.MProcess.id)
        )

        yield from run_sql_from_query_with_casted(
            query=query,
            db_instance=db_instance,
            cls=ap_models.MProcess,
        )

        yield from run_sql_from_query_with_casted(
            query=query_mapping_process_data,
            db_instance=db_instance,
            cls=ap_models.MProcess,
        )
