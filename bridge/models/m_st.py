from __future__ import annotations

from typing import TYPE_CHECKING, Iterator

from ap.common.constants import DEFAULT_ST_SIGN, RawDataTypeDB
from ap.setting_module import models as ap_models
from bridge.models.bridge_station import MasterModel
from bridge.models.model_utils import TableColumn
from bridge.services.sql.utils import run_sql_from_query_with_casted

if TYPE_CHECKING:
    from ap.common.pydn.dblib.postgresql import PostgreSQL


class MSt(MasterModel):
    class Columns(TableColumn):
        id = (1, RawDataTypeDB.INTEGER)
        equip_id = (2, RawDataTypeDB.INTEGER)
        st_no = (3, RawDataTypeDB.SMALL_INT)
        st_sign = (4, RawDataTypeDB.TEXT)
        created_at = (97, RawDataTypeDB.DATETIME)
        updated_at = (98, RawDataTypeDB.DATETIME)

    _table_name = 'm_st'
    primary_keys = [Columns.id]

    def __init__(self, dict_proc=None) -> None:
        dict_proc = dict_proc if dict_proc else {}
        self.id = dict_proc.get(MSt.Columns.id.name)
        self.equip_id = dict_proc.get(MSt.Columns.equip_id.name)
        self.st_no = dict_proc.get(MSt.Columns.st_no.name)
        self.st_sign = dict_proc.get(MSt.Columns.st_sign.name)

    @classmethod
    def get_name_column(cls):
        """
        Human-friendly column

        :return:
        """
        return cls.Columns.st_no

    @classmethod
    def get_sign_column(cls):
        return cls.Columns.st_sign

    @classmethod
    def get_default_sign_value(cls):
        return DEFAULT_ST_SIGN

    @classmethod
    def get_unique_by_data_table_ids(
        cls,
        *,
        db_instance: PostgreSQL,
        data_table_ids: list[int],
    ) -> Iterator[ap_models.MSt]:
        query = (
            ap_models.MSt.query.join(
                ap_models.RFactoryMachine,
                ap_models.MSt.id == ap_models.RFactoryMachine.st_id,
            )
            .join(
                ap_models.MappingFactoryMachine,
                ap_models.RFactoryMachine.id == ap_models.MappingFactoryMachine.factory_machine_id,
            )
            .filter(ap_models.MappingFactoryMachine.data_table_id.in_(data_table_ids))
            .order_by(ap_models.MSt.id)
        )

        yield from run_sql_from_query_with_casted(
            query=query,
            db_instance=db_instance,
            cls=ap_models.MSt,
        )
