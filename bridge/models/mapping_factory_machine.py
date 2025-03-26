import re
from typing import Union

from sqlalchemy.orm import scoped_session

from ap.common.constants import RawDataTypeDB
from ap.common.pydn.dblib.db_common import SqlComparisonOperator
from ap.common.pydn.dblib.postgresql import PostgreSQL
from bridge.models.bridge_station import BridgeStationModel, MasterModel
from bridge.models.model_utils import TableColumn


class MappingFactoryMachine(MasterModel):
    class Columns(TableColumn):
        id = (1, RawDataTypeDB.INTEGER)
        t_location_name = (2, RawDataTypeDB.TEXT)
        t_location_abbr = (3, RawDataTypeDB.TEXT)
        t_factory_id = (4, RawDataTypeDB.TEXT)
        t_factory_name = (5, RawDataTypeDB.TEXT)
        t_factory_abbr = (6, RawDataTypeDB.TEXT)
        t_plant_id = (7, RawDataTypeDB.TEXT)
        t_plant_name = (8, RawDataTypeDB.TEXT)
        t_plant_abbr = (9, RawDataTypeDB.TEXT)
        t_dept_id = (10, RawDataTypeDB.TEXT)
        t_dept_name = (11, RawDataTypeDB.TEXT)
        t_dept_abbr = (12, RawDataTypeDB.TEXT)
        t_sect_id = (13, RawDataTypeDB.TEXT)
        t_sect_name = (14, RawDataTypeDB.TEXT)
        t_sect_abbr = (15, RawDataTypeDB.TEXT)
        t_line_id = (16, RawDataTypeDB.TEXT)
        t_line_no = (17, RawDataTypeDB.TEXT)
        t_line_name = (18, RawDataTypeDB.TEXT)
        t_outsource = (19, RawDataTypeDB.TEXT)
        t_equip_id = (20, RawDataTypeDB.TEXT)
        t_equip_name = (21, RawDataTypeDB.TEXT)
        t_equip_product_no = (22, RawDataTypeDB.TEXT)
        t_equip_product_date = (23, RawDataTypeDB.TEXT)
        t_equip_no = (24, RawDataTypeDB.TEXT)
        t_station_no = (25, RawDataTypeDB.TEXT)
        t_prod_family_id = (26, RawDataTypeDB.TEXT)
        t_prod_family_name = (27, RawDataTypeDB.TEXT)
        t_prod_family_abbr = (28, RawDataTypeDB.TEXT)
        t_prod_id = (29, RawDataTypeDB.TEXT)
        t_prod_name = (30, RawDataTypeDB.TEXT)
        t_prod_abbr = (31, RawDataTypeDB.TEXT)
        t_process_id = (32, RawDataTypeDB.TEXT)
        t_process_name = (33, RawDataTypeDB.TEXT)
        t_process_abbr = (34, RawDataTypeDB.TEXT)
        factory_machine_id = (35, RawDataTypeDB.INTEGER)
        data_table_id = (36, RawDataTypeDB.INTEGER)

    __is_mapping_table__ = True
    _table_name = 'mapping_factory_machine'
    primary_keys = []
    unique_keys = []
    not_null_columns = []

    @classmethod
    def get_factory_machine_id(
        cls,
        db_instance,
        **kwargs,
    ):
        """
        Get data row by columns condition

        :param db_instance: a database instance
        :param kwargs: a dictionary which keys are column name of this table
        :return: data rows
        """
        dict_cond = {}
        for col in cls.Columns:
            if not re.match(rf'^t_.*|^{cls.Columns.data_table_id.name}$', col.name) or col.name not in kwargs:
                continue

            val = kwargs.get(col.name)
            if val != '' and val is not None:
                dict_cond[col.name] = [(SqlComparisonOperator.EQUAL, val)]

        _, row = cls.select_records(db_instance, dic_conditions=dict_cond, row_is_dict=True, limit=1)
        return row.get(cls.Columns.factory_machine_id.name) if row else None

    @classmethod
    def get_process_id_with_data_table_id(cls, db_instance: Union[PostgreSQL, scoped_session], data_table_ids):
        from bridge.models.r_factory_machine import RFactoryMachine

        param_marker = BridgeStationModel.get_parameter_marker()

        sql = f'''
SELECT
    DISTINCT rfm.{ RFactoryMachine.Columns.process_id.name },
    rfm.{ RFactoryMachine.Columns.line_id.name },
    rfm.{ RFactoryMachine.Columns.equip_id.name },
    mfm.{ MappingFactoryMachine.Columns.data_table_id.name }
FROM
    "{cls._table_name}" mfm
    LEFT JOIN "{RFactoryMachine.get_table_name()}" rfm
        ON rfm.{ RFactoryMachine.Columns.id.name } = mfm.{ MappingFactoryMachine.Columns.factory_machine_id.name }
WHERE
    mfm.{ cls.Columns.data_table_id.name } in {param_marker};
        '''
        if isinstance(db_instance, scoped_session):
            _rows = db_instance.execute(sql, params=data_table_ids)
            _cols = [
                RFactoryMachine.Columns.process_id.name,
                RFactoryMachine.Columns.line_id.name,
                RFactoryMachine.Columns.equip_id.name,
            ]
            rows = [dict(zip(_cols, row)) for row in _rows]
        else:
            _, rows = db_instance.run_sql(sql, row_is_dict=True, params=[tuple(data_table_ids)])

        return rows
