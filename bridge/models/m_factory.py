from __future__ import annotations

from typing import TYPE_CHECKING, Iterator

from ap.common.constants import RawDataTypeDB
from ap.setting_module import models as ap_models
from bridge.models.bridge_station import MasterModel
from bridge.models.model_utils import TableColumn
from bridge.services.sql.utils import run_sql_from_query_with_casted

if TYPE_CHECKING:
    from ap.common.pydn.dblib.postgresql import PostgreSQL


class MFactory(MasterModel):
    class Columns(TableColumn):
        id = (1, RawDataTypeDB.INTEGER)
        factory_factid = (2, RawDataTypeDB.TEXT)
        factory_name_jp = (3, RawDataTypeDB.TEXT)
        factory_name_en = (4, RawDataTypeDB.TEXT)
        factory_name_sys = (5, RawDataTypeDB.TEXT)
        factory_name_local = (6, RawDataTypeDB.TEXT)
        location_id = (7, RawDataTypeDB.INTEGER)
        factory_abbr_jp = (8, RawDataTypeDB.TEXT)
        factory_abbr_en = (9, RawDataTypeDB.TEXT)
        factory_abbr_local = (10, RawDataTypeDB.TEXT)
        created_at = (97, RawDataTypeDB.DATETIME)
        updated_at = (98, RawDataTypeDB.DATETIME)

    _table_name = 'm_factory'
    primary_keys = [Columns.id]
    not_null_columns = [Columns.factory_name_jp]

    def __init__(self, dict_proc=None) -> None:
        dict_proc = dict_proc if dict_proc else {}
        self.id = dict_proc.get(MFactory.Columns.id.name)
        self.factory_factid = dict_proc.get(MFactory.Columns.factory_factid.name)
        self.factory_name_jp = dict_proc.get(MFactory.Columns.factory_name_jp.name)
        self.factory_name_en = dict_proc.get(MFactory.Columns.factory_name_en.name)
        self.factory_name_sys = dict_proc.get(MFactory.Columns.factory_name_sys.name)
        self.factory_name_local = dict_proc.get(MFactory.Columns.factory_name_local.name)
        self.location_id = dict_proc.get(MFactory.Columns.location_id.name)
        self.factory_abbr_jp = dict_proc.get(MFactory.Columns.factory_abbr_jp.name)
        self.factory_abbr_en = dict_proc.get(MFactory.Columns.factory_abbr_en.name)
        self.factory_abbr_local = dict_proc.get(MFactory.Columns.factory_abbr_local.name)

    @classmethod
    def get_unique_by_data_table_ids(
        cls,
        *,
        db_instance: PostgreSQL,
        data_table_ids: list[int],
    ) -> Iterator[ap_models.MFactory]:
        query = (
            ap_models.MFactory.query.join(
                ap_models.MPlant,
                ap_models.MFactory.id == ap_models.MPlant.factory_id,
            )
            .join(ap_models.MLine, ap_models.MPlant.id == ap_models.MLine.plant_id)
            .join(
                ap_models.RFactoryMachine,
                ap_models.MLine.id == ap_models.RFactoryMachine.line_id,
            )
            .join(
                ap_models.MappingFactoryMachine,
                ap_models.RFactoryMachine.id == ap_models.MappingFactoryMachine.factory_machine_id,
            )
            .filter(ap_models.MappingFactoryMachine.data_table_id.in_(data_table_ids))
            .order_by(ap_models.MFactory.id)
        )

        yield from run_sql_from_query_with_casted(
            query=query,
            db_instance=db_instance,
            cls=ap_models.MFactory,
        )
