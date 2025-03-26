from typing import Iterator

from ap.common.constants import RawDataTypeDB
from ap.common.pydn.dblib.postgresql import PostgreSQL
from ap.setting_module import models as ap_models
from bridge.models.bridge_station import MasterModel
from bridge.models.model_utils import TableColumn
from bridge.services.sql.utils import run_sql_from_query_with_casted


class MLineGroup(MasterModel):
    class Columns(TableColumn):
        id = (1, RawDataTypeDB.INTEGER)
        line_name_jp = (2, RawDataTypeDB.TEXT)
        line_name_en = (10, RawDataTypeDB.TEXT)
        line_name_sys = (3, RawDataTypeDB.TEXT)
        line_name_local = (4, RawDataTypeDB.TEXT)
        created_at = (97, RawDataTypeDB.DATETIME)
        updated_at = (98, RawDataTypeDB.DATETIME)

    _table_name = 'm_line_group'
    primary_keys = [Columns.id]
    not_null_columns = [Columns.line_name_jp]

    def __init__(self, dict_proc=None):
        dict_proc = dict_proc if dict_proc else {}
        self.id = dict_proc.get(MLineGroup.Columns.id.name)
        self.line_name_jp = dict_proc.get(MLineGroup.Columns.line_name_jp.name)
        self.line_name_en = dict_proc.get(MLineGroup.Columns.line_name_en.name)
        self.line_name_sys = dict_proc.get(MLineGroup.Columns.line_name_sys.name)
        self.line_name_local = dict_proc.get(MLineGroup.Columns.line_name_local.name)

    @classmethod
    def get_unique_by_data_table_ids(
        cls,
        *,
        db_instance: PostgreSQL,
        data_table_ids: list[int],
    ) -> Iterator[ap_models.MLineGroup]:
        query = (
            ap_models.MLineGroup.query.join(
                ap_models.MLine,
                ap_models.MLine.line_group_id == ap_models.MLineGroup.id,
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
            .order_by(ap_models.MLineGroup.id)
        )

        yield from run_sql_from_query_with_casted(
            query=query,
            db_instance=db_instance,
            cls=ap_models.MLineGroup,
        )
