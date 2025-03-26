from __future__ import annotations

from typing import TYPE_CHECKING, Iterator

from ap.common.constants import RawDataTypeDB
from ap.setting_module import models as ap_models
from bridge.models.bridge_station import MasterModel
from bridge.models.model_utils import TableColumn
from bridge.services.sql.utils import run_sql_from_query_with_casted

if TYPE_CHECKING:
    from ap.common.pydn.dblib.postgresql import PostgreSQL


class MProd(MasterModel):
    class Columns(TableColumn):
        id = (1, RawDataTypeDB.INTEGER)
        prod_family_id = (2, RawDataTypeDB.INTEGER)
        prod_factid = (3, RawDataTypeDB.INTEGER)
        prod_name_jp = (4, RawDataTypeDB.TEXT)
        prod_name_en = (5, RawDataTypeDB.TEXT)
        prod_name_sys = (6, RawDataTypeDB.TEXT)
        prod_name_local = (7, RawDataTypeDB.TEXT)
        prod_abbr_jp = (8, RawDataTypeDB.TEXT)
        prod_abbr_en = (9, RawDataTypeDB.TEXT)
        prod_abbr_local = (10, RawDataTypeDB.TEXT)
        created_at = (97, RawDataTypeDB.DATETIME)
        updated_at = (98, RawDataTypeDB.DATETIME)

    _table_name = 'm_prod'
    primary_keys = [Columns.id]
    not_null_columns = [Columns.prod_family_id]

    def __init__(self, dict_proc=None) -> None:
        dict_proc = dict_proc if dict_proc else {}
        self.id = dict_proc.get(MProd.Columns.id.name)
        self.prod_family_id = dict_proc.get(MProd.Columns.prod_family_id.name)
        self.prod_factid = dict_proc.get(MProd.Columns.prod_factid.name)
        self.prod_name_jp = dict_proc.get(MProd.Columns.prod_name_jp.name)
        self.prod_name_en = dict_proc.get(MProd.Columns.prod_name_en.name)
        self.prod_name_sys = dict_proc.get(MProd.Columns.prod_name_sys.name)
        self.prod_name_local = dict_proc.get(MProd.Columns.prod_name_local.name)
        self.prod_abbr_jp = dict_proc.get(MProd.Columns.prod_abbr_jp.name)
        self.prod_abbr_en = dict_proc.get(MProd.Columns.prod_abbr_en.name)
        self.prod_abbr_local = dict_proc.get(MProd.Columns.prod_abbr_local.name)

    @classmethod
    def get_unique_by_data_table_ids(
        cls,
        *,
        db_instance: PostgreSQL,
        data_table_ids: list[int],
    ) -> Iterator[ap_models.MProd]:
        query = (
            ap_models.MProd.query.join(
                ap_models.RProdPart,
                ap_models.MProd.id == ap_models.RProdPart.prod_id,
            )
            .join(
                ap_models.MappingPart,
                ap_models.RProdPart.part_id == ap_models.MappingPart.part_id,
            )
            .filter(ap_models.MappingPart.data_table_id.in_(data_table_ids))
            .order_by(ap_models.MProd.id)
        )

        query_mapping_factory_machine = (
            ap_models.MProd.query.join(
                ap_models.MProdFamily,
                ap_models.MProdFamily.id == ap_models.MProd.prod_family_id,
            )
            .join(
                ap_models.MProcess,
                ap_models.MProcess.prod_family_id == ap_models.MProdFamily.id,
            )
            .join(
                ap_models.RFactoryMachine,
                ap_models.RFactoryMachine.process_id == ap_models.MProcess.id,
            )
            .join(
                ap_models.MappingFactoryMachine,
                ap_models.MappingFactoryMachine.factory_machine_id == ap_models.RFactoryMachine.id,
            )
            .filter(ap_models.MappingFactoryMachine.data_table_id.in_(data_table_ids))
            .order_by(ap_models.MProd.id)
        )

        query_mapping_process_data = (
            ap_models.MProd.query.join(
                ap_models.MProdFamily,
                ap_models.MProdFamily.id == ap_models.MProd.prod_family_id,
            )
            .join(
                ap_models.MProcess,
                ap_models.MProcess.prod_family_id == ap_models.MProdFamily.id,
            )
            .join(ap_models.MData, ap_models.MData.process_id == ap_models.MProcess.id)
            .join(
                ap_models.MappingProcessData,
                ap_models.MappingProcessData.data_id == ap_models.MData.id,
            )
            .filter(ap_models.MappingProcessData.data_table_id.in_(data_table_ids))
            .order_by(ap_models.MProd.id)
        )

        yield from run_sql_from_query_with_casted(
            query=query,
            db_instance=db_instance,
            cls=ap_models.MProd,
        )

        yield from run_sql_from_query_with_casted(
            query=query_mapping_factory_machine,
            db_instance=db_instance,
            cls=ap_models.MProd,
        )

        yield from run_sql_from_query_with_casted(
            query=query_mapping_process_data,
            db_instance=db_instance,
            cls=ap_models.MProd,
        )
