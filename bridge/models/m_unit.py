from typing import Iterator, List, Tuple

from ap.common.constants import NULL_DEFAULT_STRING, RawDataTypeDB
from ap.common.pydn.dblib.db_common import SqlComparisonOperator
from ap.common.pydn.dblib.postgresql import PostgreSQL
from ap.setting_module import models as ap_models
from bridge.models.bridge_station import MasterModel
from bridge.models.model_utils import TableColumn
from bridge.services.sql.utils import run_sql_from_query_with_casted


class MUnit(MasterModel):
    class Columns(TableColumn):
        id = (1, RawDataTypeDB.INTEGER)
        quantity_jp = (2, RawDataTypeDB.TEXT)
        quantity_en = (3, RawDataTypeDB.TEXT)
        unit = (4, RawDataTypeDB.TEXT)
        type = (5, RawDataTypeDB.TEXT)
        base = (6, RawDataTypeDB.INTEGER)
        conversion = (7, RawDataTypeDB.REAL)
        denominator = (8, RawDataTypeDB.REAL)
        offset = (9, RawDataTypeDB.REAL)
        created_at = (97, RawDataTypeDB.DATETIME)
        updated_at = (98, RawDataTypeDB.DATETIME)

    _table_name = 'm_unit'
    primary_keys = [Columns.id]
    not_null_columns = [Columns.unit]
    # message_cls = MsgMPart

    def __init__(self, dict_proc=None):
        dict_proc = dict_proc if dict_proc else {}
        self.id = dict_proc.get(MUnit.Columns.id.name)
        self.quantity_jp = dict_proc.get(MUnit.Columns.quantity_jp.name)
        self.quantity_en = dict_proc.get(MUnit.Columns.quantity_en.name)
        self.unit = dict_proc.get(MUnit.Columns.unit.name)
        self.type = dict_proc.get(MUnit.Columns.type.name)
        self.base = dict_proc.get(MUnit.Columns.base.name)
        self.conversion = dict_proc.get(MUnit.Columns.conversion.name)
        self.denominator = dict_proc.get(MUnit.Columns.denominator.name)
        self.offset = dict_proc.get(MUnit.Columns.offset.name)

    @classmethod
    def get_in_ids(cls, db_instance, ids: [List, Tuple], is_return_dict=False):
        if not ids:
            return {} if is_return_dict else []
        id_col = cls.get_pk_column_names()[0]
        _, rows = cls.select_records(
            db_instance,
            {id_col: [(SqlComparisonOperator.IN, tuple(ids))]},
            filter_deleted=False,
        )
        if not rows:
            return []
        data_groups = [MUnit(row) for row in rows]
        if is_return_dict:
            return {data_group.id: data_group for data_group in data_groups}
        return data_groups

    @classmethod
    def get_count(cls, db_instance):
        sql = 'SELECT COUNT(1) FROM m_unit'
        _, rows = db_instance.run_sql(sql)
        return int(rows[0]['count'])

    @classmethod
    def get_empty_unit_id(cls, db_instance):
        selection = [cls.Columns.id.name]
        dic_conditions = {cls.Columns.unit.name: NULL_DEFAULT_STRING}
        cols, rows = cls.select_records(db_instance, dic_conditions, select_cols=selection, row_is_dict=False)
        non_unit_id = rows[0][0]
        return non_unit_id

    @classmethod
    def get_all_units(cls, db_instance: PostgreSQL) -> list[str]:
        selection = [cls.Columns.unit.name]
        _, rows = cls.select_records(db_instance, select_cols=selection, row_is_dict=False)
        if rows:
            return [unit for unit, *_ in rows]
        return []

    @classmethod
    def get_unique_by_data_table_ids(
        cls,
        *,
        db_instance: PostgreSQL,
        data_table_ids: list[int],
    ) -> Iterator[ap_models.MUnit]:
        query = (
            ap_models.MUnit.query.join(
                ap_models.MData,
                ap_models.MUnit.id == ap_models.MData.unit_id,
            )
            .join(
                ap_models.MappingProcessData,
                ap_models.MappingProcessData.data_id == ap_models.MData.id,
            )
            .filter(ap_models.MappingProcessData.data_table_id.in_(data_table_ids))
            .order_by(ap_models.MUnit.id)
        )

        query_mapping_factory_machine = (
            ap_models.MUnit.query.join(
                ap_models.MData,
                ap_models.MData.unit_id == ap_models.MUnit.id,
            )
            .join(
                ap_models.MProcess,
                ap_models.MProcess.id == ap_models.MData.process_id,
            )
            .join(
                ap_models.RFactoryMachine,
                ap_models.RFactoryMachine.process_id == ap_models.MProcess.id,
            )
            .join(
                ap_models.MappingFactoryMachine,
                ap_models.MappingFactoryMachine.factory_machine_id == ap_models.RFactoryMachine.id,
            )
            .filter(ap_models.MappingFactoryMachine.data_table_id.in_(data_table_ids))
            .order_by(ap_models.MUnit.id)
        )

        yield from run_sql_from_query_with_casted(
            query=query,
            db_instance=db_instance,
            cls=ap_models.MUnit,
        )

        yield from run_sql_from_query_with_casted(
            query=query_mapping_factory_machine,
            db_instance=db_instance,
            cls=ap_models.MUnit,
        )
