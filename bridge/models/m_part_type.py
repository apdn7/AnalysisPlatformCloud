from __future__ import annotations

from typing import TYPE_CHECKING, Iterator

from ap.common.constants import BaseEnum, RawDataTypeDB
from ap.setting_module import models as ap_models
from bridge.models.bridge_station import MasterModel
from bridge.models.model_utils import TableColumn
from bridge.services.sql.utils import run_sql_from_query_with_casted

if TYPE_CHECKING:
    from ap.common.pydn.dblib.postgresql import PostgreSQL


class AssyFlag(BaseEnum):
    Part = False  # 0: Part
    Assy = True  # 1: Assy


class MPartType(MasterModel):
    class Columns(TableColumn):
        id = (1, RawDataTypeDB.INTEGER)
        part_type_factid = (2, RawDataTypeDB.TEXT)
        part_name_jp = (3, RawDataTypeDB.TEXT)
        part_name_en = (4, RawDataTypeDB.TEXT)
        part_name_local = (5, RawDataTypeDB.TEXT)
        part_abbr_jp = (6, RawDataTypeDB.TEXT)
        part_abbr_en = (7, RawDataTypeDB.TEXT)
        part_abbr_local = (8, RawDataTypeDB.TEXT)
        assy_flag = (9, RawDataTypeDB.BOOLEAN)  # AssyFlag
        created_at = (97, RawDataTypeDB.DATETIME)
        updated_at = (98, RawDataTypeDB.DATETIME)

    _table_name = 'm_part_type'
    primary_keys = [Columns.id]
    not_null_columns = [Columns.part_type_factid]

    def __init__(self, dict_proc=None) -> None:
        dict_proc = dict_proc if dict_proc else {}
        self.id = dict_proc.get(MPartType.Columns.id.name)
        self.part_type_factid = dict_proc.get(MPartType.Columns.part_type_factid.name)
        self.part_name_jp = dict_proc.get(MPartType.Columns.part_name_jp.name)
        self.part_name_en = dict_proc.get(MPartType.Columns.part_name_en.name)
        self.part_name_local = dict_proc.get(MPartType.Columns.part_name_local.name)
        self.part_abbr_jp = dict_proc.get(MPartType.Columns.part_abbr_jp.name)
        self.part_abbr_en = dict_proc.get(MPartType.Columns.part_abbr_en.name)
        self.part_abbr_local = dict_proc.get(MPartType.Columns.part_abbr_local.name)
        self.assy_flag = dict_proc.get(MPartType.Columns.assy_flag.name)

    @classmethod
    def get_unique_by_data_table_ids(
        cls,
        *,
        db_instance: PostgreSQL,
        data_table_ids: int,
    ) -> Iterator[ap_models.MPartType]:
        query = (
            ap_models.MPartType.query.join(
                ap_models.MPart,
                ap_models.MPartType.id == ap_models.MPart.part_type_id,
            )
            .join(
                ap_models.MappingPart,
                ap_models.MPart.id == ap_models.MappingPart.part_id,
            )
            .filter(ap_models.MappingPart.data_table_id.in_(data_table_ids))
            .order_by(ap_models.MPartType.id)
        )

        yield from run_sql_from_query_with_casted(
            query=query,
            db_instance=db_instance,
            cls=ap_models.MPartType,
        )
