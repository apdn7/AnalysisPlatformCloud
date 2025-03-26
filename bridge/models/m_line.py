from __future__ import annotations

from typing import TYPE_CHECKING, Iterator

from ap.common.constants import DEFAULT_LINE_SIGN, BaseEnum, RawDataTypeDB
from ap.setting_module import models as ap_models
from bridge.models.bridge_station import MasterModel
from bridge.models.model_utils import TableColumn
from bridge.services.sql.utils import run_sql_from_query_with_casted

if TYPE_CHECKING:
    from ap.common.pydn.dblib.postgresql import PostgreSQL


class OutsourcingFlag(BaseEnum):
    Make = False  # 0: Make
    Buy = True  # 1: Buy


class MLine(MasterModel):
    class Columns(TableColumn):
        id = (1, RawDataTypeDB.INTEGER)
        plant_id = (2, RawDataTypeDB.INTEGER)
        prod_family_id = (3, RawDataTypeDB.INTEGER)
        line_group_id = (4, RawDataTypeDB.INTEGER)
        line_factid = (5, RawDataTypeDB.TEXT)
        line_no = (6, RawDataTypeDB.SMALL_INT)
        line_sign = (7, RawDataTypeDB.TEXT)
        outsourcing_flag = (11, RawDataTypeDB.BOOLEAN)
        outsource = (12, RawDataTypeDB.TEXT)
        supplier = (13, RawDataTypeDB.TEXT)
        created_at = (97, RawDataTypeDB.DATETIME)
        updated_at = (98, RawDataTypeDB.DATETIME)

    _table_name = 'm_line'
    primary_keys = [Columns.id]
    not_null_columns = [Columns.plant_id, Columns.prod_family_id, Columns.line_group_id]

    def __init__(self, dict_proc=None) -> None:
        dict_proc = dict_proc if dict_proc else {}
        self.id = dict_proc.get(MLine.Columns.id.name)
        self.plant_id = dict_proc.get(MLine.Columns.plant_id.name)
        self.prod_family_id = dict_proc.get(MLine.Columns.prod_family_id.name)
        self.line_group_id = dict_proc.get(MLine.Columns.line_group_id.name)
        self.line_factid = dict_proc.get(MLine.Columns.line_factid.name)
        self.line_no = dict_proc.get(MLine.Columns.line_no.name)
        self.line_sign = dict_proc.get(MLine.Columns.line_sign.name)
        self.outsourcing_flag = dict_proc.get(MLine.Columns.outsourcing_flag.name)
        self.outsource = dict_proc.get(MLine.Columns.outsource.name)
        self.supplier = dict_proc.get(MLine.Columns.supplier.name)

    @classmethod
    def get_by_id(cls, db_instance, id):
        _, row = cls.select_records(db_instance, {cls.Columns.id.name: int(id)}, limit=1)
        if not row:
            return None
        return row

    @classmethod
    def get_name_column(cls):
        """
        Human-friendly column

        :return:
        """
        return cls.Columns.line_name_jp

    @classmethod
    def get_sign_column(cls):
        return cls.Columns.line_sign

    @classmethod
    def get_default_sign_value(cls):
        return DEFAULT_LINE_SIGN

    @classmethod
    def get_unique_by_data_table_ids(
        cls,
        *,
        db_instance: PostgreSQL,
        data_table_ids: list[int],
    ) -> Iterator[ap_models.MLine]:
        query = (
            ap_models.MLine.query.join(
                ap_models.RFactoryMachine,
                ap_models.MLine.id == ap_models.RFactoryMachine.line_id,
            )
            .join(
                ap_models.MappingFactoryMachine,
                ap_models.RFactoryMachine.id == ap_models.MappingFactoryMachine.factory_machine_id,
            )
            .filter(ap_models.MappingFactoryMachine.data_table_id.in_(data_table_ids))
            .order_by(ap_models.MLine.id)
        )

        yield from run_sql_from_query_with_casted(
            query=query,
            db_instance=db_instance,
            cls=ap_models.MLine,
        )
