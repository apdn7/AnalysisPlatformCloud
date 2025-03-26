from __future__ import annotations

from typing import TYPE_CHECKING, Iterator

from ap.common.constants import BaseEnum, RawDataTypeDB
from ap.setting_module import models as ap_models
from bridge.models.bridge_station import MasterModel
from bridge.models.model_utils import TableColumn
from bridge.services.sql.utils import run_sql_from_query_with_casted

if TYPE_CHECKING:
    from ap.common.pydn.dblib.postgresql import PostgreSQL


class PartUse(BaseEnum):
    Real = False  # 0: Real
    Dummy = True  # 1: Dummy


class MPart(MasterModel):
    class Columns(TableColumn):
        id = (1, RawDataTypeDB.INTEGER)
        part_type_id = (2, RawDataTypeDB.INTEGER)
        part_factid = (3, RawDataTypeDB.TEXT)
        part_no = (4, RawDataTypeDB.TEXT)
        part_use = (5, RawDataTypeDB.BOOLEAN)
        location_id = (6, RawDataTypeDB.INTEGER)
        created_at = (97, RawDataTypeDB.DATETIME)
        updated_at = (98, RawDataTypeDB.DATETIME)

    _table_name = 'm_part'
    primary_keys = [Columns.id]
    not_null_columns = [Columns.part_no]

    def __init__(self, dict_proc=None) -> None:
        dict_proc = dict_proc if dict_proc else {}
        self.id = dict_proc.get(MPart.Columns.id.name)
        self.part_type_id = dict_proc.get(MPart.Columns.part_type_id.name)
        self.part_factid = dict_proc.get(MPart.Columns.part_factid.name)
        self.part_no = dict_proc.get(MPart.Columns.part_no.name)
        self.part_use = dict_proc.get(MPart.Columns.part_use.name)
        self.location_id = dict_proc.get(MPart.Columns.location_id.name)

    @classmethod
    def get_name_column(cls):
        """
        Human-friendly column

        :return:
        """
        return cls.Columns.part_no

    @classmethod
    def get_unique_by_data_table_ids(
        cls,
        *,
        db_instance: PostgreSQL,
        data_table_ids: list[int],
    ) -> Iterator[ap_models.MPart]:
        query = (
            ap_models.MPart.query.join(
                ap_models.MappingPart,
                ap_models.MPart.id == ap_models.MappingPart.part_id,
            )
            .filter(ap_models.MappingPart.data_table_id.in_(data_table_ids))
            .order_by(ap_models.MPart.id)
        )
        yield from run_sql_from_query_with_casted(
            query=query,
            db_instance=db_instance,
            cls=ap_models.MPart,
        )
