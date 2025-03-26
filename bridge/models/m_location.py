from typing import Iterator

from ap.common.constants import RawDataTypeDB
from ap.common.pydn.dblib.postgresql import PostgreSQL
from ap.setting_module import models as ap_models
from bridge.models.bridge_station import MasterModel
from bridge.models.model_utils import TableColumn
from bridge.services.sql.utils import run_sql_from_query_with_casted


class MLocation(MasterModel):
    class Columns(TableColumn):
        id = (1, RawDataTypeDB.INTEGER)
        location_name_jp = (2, RawDataTypeDB.TEXT)
        location_name_en = (3, RawDataTypeDB.TEXT)
        location_name_sys = (4, RawDataTypeDB.TEXT)
        location_abbr = (6, RawDataTypeDB.TEXT)
        created_at = (97, RawDataTypeDB.DATETIME)
        updated_at = (98, RawDataTypeDB.DATETIME)

    _table_name = 'm_location'
    primary_keys = []
    not_null_columns = [Columns.location_name_jp]

    def __init__(self, dict_proc=None):
        dict_proc = dict_proc if dict_proc else {}
        self.id = dict_proc.get(MLocation.Columns.id.name)
        self.location_name_jp = dict_proc.get(MLocation.Columns.location_name_jp.name)
        self.location_name_en = dict_proc.get(MLocation.Columns.location_name_jp.name)
        self.location_name_sys = dict_proc.get(MLocation.Columns.location_name_sys.name)
        self.location_abbr = dict_proc.get(MLocation.Columns.location_abbr.name)

    @classmethod
    def get_abbr_columns(cls):
        return [cls.Columns.location_abbr.name]

    @classmethod
    def get_unique_by_data_table_ids(
        cls,
        *,
        db_instance: PostgreSQL,
        data_table_ids: list[int],
    ) -> Iterator[ap_models.MLocation]:
        query = (
            ap_models.MLocation.query.join(
                ap_models.MPart,
                ap_models.MPart.location_id == ap_models.MLocation.id,
            )
            .join(
                ap_models.MappingPart,
                ap_models.MappingPart.part_id == ap_models.MPart.id,
            )
            .filter(ap_models.MappingPart.data_table_id.in_(data_table_ids))
            .order_by(ap_models.MLocation.id)
        )

        query_from_mapping_factory_machine = (
            ap_models.MLocation.query.join(
                ap_models.MFactory,
                ap_models.MFactory.location_id == ap_models.MLocation.id,
            )
            .join(
                ap_models.MPlant,
                ap_models.MPlant.factory_id == ap_models.MFactory.id,
            )
            .join(
                ap_models.MLine,
                ap_models.MLine.plant_id == ap_models.MPlant.id,
            )
            .join(
                ap_models.RFactoryMachine,
                ap_models.RFactoryMachine.line_id == ap_models.MLine.id,
            )
            .join(
                ap_models.MappingFactoryMachine,
                ap_models.MappingFactoryMachine.factory_machine_id == ap_models.RFactoryMachine.id,
            )
            .filter(ap_models.MappingFactoryMachine.data_table_id.in_(data_table_ids))
            .order_by(ap_models.MLocation.id)
        )

        yield from run_sql_from_query_with_casted(
            query=query,
            db_instance=db_instance,
            cls=ap_models.MLocation,
        )

        yield from run_sql_from_query_with_casted(
            query=query_from_mapping_factory_machine,
            db_instance=db_instance,
            cls=ap_models.MLocation,
        )
