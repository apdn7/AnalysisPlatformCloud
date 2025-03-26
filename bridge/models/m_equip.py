from __future__ import annotations

from typing import TYPE_CHECKING, Iterator

from ap.common.constants import DEFAULT_EQUIP_SIGN, RawDataTypeDB
from ap.setting_module import models as ap_models
from bridge.models.bridge_station import MasterModel
from bridge.models.model_utils import TableColumn
from bridge.services.sql.utils import run_sql_from_query_with_casted

if TYPE_CHECKING:
    from ap.common.pydn.dblib.postgresql import PostgreSQL


class MEquip(MasterModel):
    class Columns(TableColumn):
        id = (1, RawDataTypeDB.INTEGER)
        equip_group_id = (2, RawDataTypeDB.INTEGER)
        equip_no = (3, RawDataTypeDB.SMALL_INT)
        equip_sign = (3, RawDataTypeDB.TEXT)
        equip_factid = (5, RawDataTypeDB.TEXT)
        equip_product_no = (6, RawDataTypeDB.TEXT)
        equip_product_date = (7, RawDataTypeDB.DATETIME)
        created_at = (97, RawDataTypeDB.DATETIME)
        updated_at = (98, RawDataTypeDB.DATETIME)

    _table_name = 'm_equip'
    primary_keys = [Columns.id]
    not_null_columns = [Columns.equip_group_id]

    def __init__(self, dict_proc=None) -> None:
        dict_proc = dict_proc if dict_proc else {}
        self.id = dict_proc.get(MEquip.Columns.id.name)
        self.equip_group_id = dict_proc.get(MEquip.Columns.equip_group_id.name)
        self.equip_no = dict_proc.get(MEquip.Columns.equip_no.name)
        self.equip_sign = dict_proc.get(MEquip.Columns.equip_sign.name)
        self.equip_factid = dict_proc.get(MEquip.Columns.equip_factid.name)
        self.equip_product_no = dict_proc.get(MEquip.Columns.equip_product_no.name)
        self.equip_product_date = dict_proc.get(MEquip.Columns.equip_product_date.name)

    @classmethod
    def get_name_column(cls):
        """
        Human-friendly column

        :return:
        """
        return cls.Columns.equip_name_jp

    @classmethod
    def get_sign_column(cls):
        return cls.Columns.equip_sign

    @classmethod
    def get_default_sign_value(cls):
        return DEFAULT_EQUIP_SIGN

    @classmethod
    def get_unique_by_data_table_ids(
        cls,
        *,
        db_instance: PostgreSQL,
        data_table_ids: list[int],
    ) -> Iterator[ap_models.MEquip]:
        query = (
            ap_models.MEquip.query.join(
                ap_models.RFactoryMachine,
                ap_models.MEquip.id == ap_models.RFactoryMachine.equip_id,
            )
            .join(
                ap_models.MappingFactoryMachine,
                ap_models.RFactoryMachine.id == ap_models.MappingFactoryMachine.factory_machine_id,
            )
            .filter(ap_models.MappingFactoryMachine.data_table_id.in_(data_table_ids))
            .order_by(ap_models.MEquip.id)
        )

        yield from run_sql_from_query_with_casted(
            query=query,
            db_instance=db_instance,
            cls=ap_models.MEquip,
        )
