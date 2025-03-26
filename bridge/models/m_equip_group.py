from typing import Iterator

from ap.common.constants import RawDataTypeDB
from ap.common.pydn.dblib.postgresql import PostgreSQL
from ap.setting_module import models as ap_models
from bridge.models.bridge_station import MasterModel
from bridge.models.model_utils import TableColumn
from bridge.services.sql.utils import run_sql_from_query_with_casted


class MEquipGroup(MasterModel):
    class Columns(TableColumn):
        id = (1, RawDataTypeDB.INTEGER)
        equip_name_jp = (3, RawDataTypeDB.TEXT)
        equip_name_en = (2, RawDataTypeDB.TEXT)
        equip_name_sys = (4, RawDataTypeDB.TEXT)
        equip_name_local = (5, RawDataTypeDB.TEXT)
        created_at = (97, RawDataTypeDB.DATETIME)
        updated_at = (98, RawDataTypeDB.DATETIME)

    _table_name = 'm_equip_group'
    primary_keys = [Columns.id]
    not_null_columns = [Columns.equip_name_jp]

    def __init__(self, dict_proc=None):
        dict_proc = dict_proc if dict_proc else {}
        self.id = dict_proc.get(MEquipGroup.Columns.id.name)
        self.equip_name_jp = dict_proc.get(MEquipGroup.Columns.equip_name_jp.name)
        self.equip_name_en = dict_proc.get(MEquipGroup.Columns.equip_name_en.name)
        self.equip_name_sys = dict_proc.get(MEquipGroup.Columns.equip_name_sys.name)
        self.equip_name_local = dict_proc.get(MEquipGroup.Columns.equip_name_local.name)

    @classmethod
    def get_unique_by_data_table_ids(
        cls,
        *,
        db_instance: PostgreSQL,
        data_table_ids: list[int],
    ) -> Iterator[ap_models.MEquipGroup]:
        query = (
            ap_models.MEquipGroup.query.join(
                ap_models.MEquip,
                ap_models.MEquip.equip_group_id == ap_models.MEquipGroup.id,
            )
            .join(
                ap_models.RFactoryMachine,
                ap_models.RFactoryMachine.equip_id == ap_models.MEquip.id,
            )
            .join(
                ap_models.MappingFactoryMachine,
                ap_models.MappingFactoryMachine.factory_machine_id == ap_models.RFactoryMachine.id,
            )
            .filter(ap_models.MappingFactoryMachine.data_table_id.in_(data_table_ids))
            .order_by(ap_models.MEquipGroup.id)
        )

        yield from run_sql_from_query_with_casted(
            query=query,
            db_instance=db_instance,
            cls=ap_models.MEquipGroup,
        )
