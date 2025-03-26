from ap.common.constants import DataType
from ap.common.pydn.dblib.db_common import SqlComparisonOperator
from ap.common.pydn.dblib.postgresql import PostgreSQL
from bridge.models.bridge_station import MasterModel
from bridge.models.m_equip import MEquip
from bridge.models.m_equip_group import MEquipGroup
from bridge.models.m_line import MLine
from bridge.models.m_line_group import MLineGroup
from bridge.models.mapping_factory_machine import MappingFactoryMachine
from bridge.models.model_utils import TableColumn


class RFactoryMachine(MasterModel):
    class Columns(TableColumn):
        id = (1, DataType.INTEGER)
        line_id = (2, DataType.INTEGER)
        process_id = (3, DataType.INTEGER)
        equip_id = (4, DataType.INTEGER)
        equip_st = (5, DataType.TEXT)
        sect_id = (6, DataType.INTEGER)
        st_id = (7, DataType.INTEGER)
        created_at = (97, DataType.DATETIME)
        updated_at = (98, DataType.DATETIME)

    _table_name = 'r_factory_machine'
    primary_keys = [Columns.id]
    not_null_columns = []

    def __init__(self, dict_proc=None):
        dict_proc = dict_proc if dict_proc else {}
        self.id = dict_proc.get(RFactoryMachine.Columns.id.name)
        self.line_id = dict_proc.get(RFactoryMachine.Columns.line_id.name)
        self.process_id = dict_proc.get(RFactoryMachine.Columns.process_id.name)
        self.equip_id = dict_proc.get(RFactoryMachine.Columns.equip_id.name)
        self.equip_st = dict_proc.get(RFactoryMachine.Columns.equip_st.name)
        self.sect_id = dict_proc.get(RFactoryMachine.Columns.sect_id.name)
        self.st_id = dict_proc.get(RFactoryMachine.Columns.st_id.name)

    @classmethod
    def get_all_data_with_name(cls, db_instance):
        sql = f'''
 SELECT
    rfm.*,
    (
        CASE
            WHEN ml.{ MLine.Columns.line_no.name } IS NULL THEN mlg.{ MLineGroup.Columns.line_name_jp.name }
            ELSE ml.{ MLine.Columns.line_no.name }
        END
    ) AS "{MLine.Columns.line_no.name}",
    (
        CASE
            WHEN ml.{ MLine.Columns.line_sign.name } IS NULL THEN mlg.{ MLineGroup.Columns.line_name_jp.name }
            ELSE ml.{ MLine.Columns.line_sign.name }
        END
    ) AS "{MLine.Columns.line_sign.name}",
    (
        CASE
            WHEN me.{ MEquip.Columns.equip_no.name } IS NULL THEN meg.{ MEquipGroup.Columns.equip_name_jp.name }
            ELSE me.{ MEquip.Columns.equip_no.name }
        END
    ) AS "{MEquip.Columns.equip_no.name}",
    (
        CASE
            WHEN me.{ MEquip.Columns.equip_sign.name } IS NULL THEN meg.{ MEquipGroup.Columns.equip_name_jp.name }
            ELSE me.{ MEquip.Columns.equip_sign.name }
        END
    ) AS "{MEquip.Columns.equip_sign.name}"
FROM
    "{cls._table_name}" rfm
    LEFT JOIN "{MLine.get_table_name()}" ml
        ON ml.{ MLine.Columns.id.name } = rfm.{ cls.Columns.line_id.name }
    LEFT JOIN "{MLineGroup.get_table_name()}" mlg
        ON mlg.{ MLineGroup.Columns.id.name } = ml.{ MLine.Columns.line_group_id.name }
    LEFT JOIN "{MEquip.get_table_name()}" me
        ON me.{ MEquip.Columns.id.name } = rfm.{ cls.Columns.equip_id.name }
    LEFT JOIN "{MEquipGroup.get_table_name()}" meg
        ON meg.{ MEquipGroup.Columns.id.name } = me.{ MEquip.Columns.equip_group_id.name };
        '''
        cols, rows = db_instance.run_sql(sql, row_is_dict=True)
        return rows

    @classmethod
    def find_id(cls, db_instance, line_id, process_id, equip_id, equip_st, sect_id):
        dict_cond = {
            cls.Columns.line_id.name: [(SqlComparisonOperator.EQUAL, line_id)],
            cls.Columns.process_id.name: [(SqlComparisonOperator.EQUAL, process_id)],
            cls.Columns.equip_id.name: [(SqlComparisonOperator.EQUAL, equip_id)],
            cls.Columns.sect_id.name: [(SqlComparisonOperator.EQUAL, sect_id)],
        }

        if equip_st != '' and equip_st is not None:
            dict_cond[cls.Columns.equip_st.name] = [(SqlComparisonOperator.EQUAL, equip_st)]

        _, row = cls.select_records(db_instance, dic_conditions=dict_cond, row_is_dict=True, limit=1)
        return row.get(cls.Columns.id.name) if row else None

    @classmethod
    def get_data_table_id_with_process_id(cls, db_instance, process_id):
        sql = f'''
SELECT
    DISTINCT mfm.{ MappingFactoryMachine.Columns.data_table_id.name }
FROM
    "{cls._table_name}" rfm
    LEFT JOIN "{MappingFactoryMachine.get_table_name()}" mfm
        ON rfm.{ MappingFactoryMachine.Columns.id.name } = mfm.{ MappingFactoryMachine.Columns.factory_machine_id.name }
WHERE
    rfm.{ cls.Columns.process_id.name } = { process_id };
        '''
        cols, rows = db_instance.run_sql(sql, row_is_dict=True)
        return rows

    @classmethod
    def get_all_data_table_id_with_process_id(cls, db_instance: PostgreSQL):
        sql = f'''
SELECT
    DISTINCT mfm.{MappingFactoryMachine.Columns.data_table_id.name}, rfm.{cls.Columns.process_id.name}
FROM
    "{cls._table_name}" rfm
    LEFT JOIN "{MappingFactoryMachine.get_table_name()}" mfm
        ON rfm.{MappingFactoryMachine.Columns.id.name} = mfm.{MappingFactoryMachine.Columns.factory_machine_id.name}
WHERE
    mfm.{MappingFactoryMachine.Columns.data_table_id.name} IS NOT NULL
    AND rfm.{cls.Columns.process_id.name} IS NOT NULL
ORDER BY
    mfm.{MappingFactoryMachine.Columns.data_table_id.name}
;'''
        cols, rows = db_instance.run_sql(sql, row_is_dict=True)
        return rows
