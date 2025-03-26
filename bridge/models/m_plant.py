from __future__ import annotations

from typing import TYPE_CHECKING, Iterator

from ap.common.constants import RawDataTypeDB
from ap.setting_module import models as ap_models
from bridge.models.bridge_station import MasterModel
from bridge.models.model_utils import TableColumn
from bridge.services.sql.utils import run_sql_from_query_with_casted

if TYPE_CHECKING:
    from ap.common.pydn.dblib.postgresql import PostgreSQL


class MPlant(MasterModel):
    class Columns(TableColumn):
        id = (1, RawDataTypeDB.INTEGER)
        factory_id = (2, RawDataTypeDB.INTEGER)
        plant_factid = (3, RawDataTypeDB.TEXT)
        plant_name_jp = (4, RawDataTypeDB.TEXT)
        plant_name_en = (5, RawDataTypeDB.TEXT)
        plant_name_sys = (6, RawDataTypeDB.TEXT)
        plant_name_local = (7, RawDataTypeDB.TEXT)
        plant_abbr_jp = (8, RawDataTypeDB.TEXT)
        plant_abbr_en = (9, RawDataTypeDB.TEXT)
        plant_abbr_local = (10, RawDataTypeDB.TEXT)
        created_at = (97, RawDataTypeDB.DATETIME)
        updated_at = (98, RawDataTypeDB.DATETIME)

    _table_name = 'm_plant'
    primary_keys = [Columns.factory_id]

    def __init__(self, dict_proc=None) -> None:
        dict_proc = dict_proc if dict_proc else {}
        self.id = dict_proc.get(MPlant.Columns.id.name)
        self.factory_id = dict_proc.get(MPlant.Columns.factory_id.name)
        self.plant_factid = dict_proc.get(MPlant.Columns.plant_factid.name)
        self.plant_name_jp = dict_proc.get(MPlant.Columns.plant_name_jp.name)
        self.plant_name_en = dict_proc.get(MPlant.Columns.plant_name_en.name)
        self.plant_name_sys = dict_proc.get(MPlant.Columns.plant_name_sys.name)
        self.plant_name_local = dict_proc.get(MPlant.Columns.plant_name_local.name)
        self.plant_abbr_jp = dict_proc.get(MPlant.Columns.plant_abbr_jp.name)
        self.plant_abbr_en = dict_proc.get(MPlant.Columns.plant_abbr_en.name)
        self.plant_abbr_local = dict_proc.get(MPlant.Columns.plant_abbr_local.name)

    @classmethod
    def get_name_column(cls):
        """
        Human-friendly column

        :return:
        """
        return cls.Columns.plant_name_jp

    @classmethod
    def get_unique_by_data_table_ids(
        cls,
        *,
        db_instance: PostgreSQL,
        data_table_ids: list[int],
    ) -> Iterator[ap_models.MPlant]:
        query = (
            ap_models.MPlant.query.join(
                ap_models.MLine,
                ap_models.MPlant.id == ap_models.MLine.plant_id,
            )
            .join(
                ap_models.RFactoryMachine,
                ap_models.MLine.id == ap_models.RFactoryMachine.line_id,
            )
            .join(
                ap_models.MappingFactoryMachine,
                ap_models.RFactoryMachine.id == ap_models.MappingFactoryMachine.factory_machine_id,
            )
            .filter(ap_models.MappingFactoryMachine.data_table_id.in_(data_table_ids))
            .order_by(ap_models.MPlant.id)
        )
        yield from run_sql_from_query_with_casted(
            query=query,
            db_instance=db_instance,
            cls=ap_models.MPlant,
        )
