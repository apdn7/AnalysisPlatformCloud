from __future__ import annotations

from typing import TYPE_CHECKING, Iterator

from ap.common.constants import RawDataTypeDB
from ap.setting_module import models as ap_models
from bridge.models.bridge_station import MasterModel
from bridge.models.model_utils import TableColumn
from bridge.services.sql.utils import run_sql_from_query_with_casted

if TYPE_CHECKING:
    from ap.common.pydn.dblib.postgresql import PostgreSQL


class MDept(MasterModel):
    class Columns(TableColumn):
        id = (1, RawDataTypeDB.INTEGER)
        dept_factid = (2, RawDataTypeDB.TEXT)
        dept_name_jp = (3, RawDataTypeDB.TEXT)
        dept_name_en = (4, RawDataTypeDB.TEXT)
        dept_name_sys = (5, RawDataTypeDB.TEXT)
        dept_name_local = (6, RawDataTypeDB.TEXT)
        dept_abbr_jp = (7, RawDataTypeDB.TEXT)
        dept_abbr_en = (8, RawDataTypeDB.TEXT)
        dept_abbr_local = (9, RawDataTypeDB.TEXT)
        created_at = (97, RawDataTypeDB.DATETIME)
        updated_at = (98, RawDataTypeDB.DATETIME)

    _table_name = 'm_dept'
    primary_keys = [Columns.id]

    def __init__(self, dict_proc=None) -> None:
        dict_proc = dict_proc if dict_proc else {}
        self.id = dict_proc.get(MDept.Columns.id.name)
        self.dept_factid = dict_proc.get(MDept.Columns.dept_factid.name)
        self.dept_name_jp = dict_proc.get(MDept.Columns.dept_name_jp.name)
        self.dept_name_en = dict_proc.get(MDept.Columns.dept_name_en.name)
        self.dept_name_sys = dict_proc.get(MDept.Columns.dept_name_sys.name)
        self.dept_name_local = dict_proc.get(MDept.Columns.dept_name_local.name)
        self.dept_abbr_jp = dict_proc.get(MDept.Columns.dept_abbr_jp.name)
        self.dept_abbr_en = dict_proc.get(MDept.Columns.dept_abbr_en.name)
        self.dept_abbr_local = dict_proc.get(MDept.Columns.dept_abbr_local.name)

    @classmethod
    def get_unique_by_data_table_ids(
        cls,
        *,
        db_instance: PostgreSQL,
        data_table_ids: list[int],
    ) -> Iterator[ap_models.MDept]:
        query = (
            ap_models.MDept.query.join(
                ap_models.MSect,
                ap_models.MDept.id == ap_models.MSect.dept_id,
            )
            .join(
                ap_models.RFactoryMachine,
                ap_models.MSect.id == ap_models.RFactoryMachine.sect_id,
            )
            .join(
                ap_models.MappingFactoryMachine,
                ap_models.RFactoryMachine.id == ap_models.MappingFactoryMachine.factory_machine_id,
            )
            .filter(ap_models.MappingFactoryMachine.data_table_id.in_(data_table_ids))
            .order_by(ap_models.MDept.id)
        )

        yield from run_sql_from_query_with_casted(
            query=query,
            db_instance=db_instance,
            cls=ap_models.MDept,
        )
