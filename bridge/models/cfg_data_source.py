from ap.common.constants import DataType, MasterDBType
from ap.common.pydn.dblib.db_common import SqlComparisonOperator, gen_select_by_condition_sql
from ap.common.pydn.dblib.postgresql import PostgreSQL
from bridge.models.bridge_station import BridgeStationModel, ConfigModel
from bridge.models.model_utils import TableColumn


class CfgDataSource(ConfigModel):
    class Columns(TableColumn):
        id = (1, DataType.INTEGER)
        name = (2, DataType.TEXT)
        type = (3, DataType.TEXT)  # type: DBType
        comment = (4, DataType.TEXT)
        order = (5, DataType.INTEGER)
        master_type = (6, DataType.TEXT)  # todo add this column thi edge
        is_direct_import = (7, DataType.BOOLEAN)  # todo add this column thi edge
        created_at = (97, DataType.DATETIME)
        updated_at = (98, DataType.DATETIME)

    _table_name = 'cfg_data_source'
    primary_keys = [Columns.id]

    def __init__(self, dict_db_source, is_cascade: bool = False, db_instance: PostgreSQL = None):
        if not dict_db_source:
            dict_db_source = {}
        self.id = dict_db_source.get(self.Columns.id.name)
        if self.id is None:
            del self.id
        self.name = dict_db_source.get(self.Columns.name.name)
        self.type = dict_db_source.get(self.Columns.type.name)
        self.comment = dict_db_source.get(self.Columns.comment.name)
        self.order = dict_db_source.get(self.Columns.order.name)
        self.master_type = dict_db_source.get(self.Columns.master_type.name)
        self.is_direct_import = dict_db_source.get(self.Columns.is_direct_import.name)
        self.created_at = dict_db_source.get(self.Columns.created_at.name)
        self.updated_at = dict_db_source.get(self.Columns.updated_at.name)
        self.db_detail = None  # : [CfgDataSourceCSV, CfgDataSourceDB]
        self.csv_detail = None  # : [CfgDataSourceCSV, CfgDataSourceDB]
        self.data_tables = []

        if not is_cascade:
            return

        @BridgeStationModel.use_db_instance(db_instance_argument_name='_db_instance')
        def _get_relation_data_(_self, _db_instance: PostgreSQL = None):
            from bridge.models.cfg_data_source_csv import CfgDataSourceCSV
            from bridge.models.cfg_data_source_db import CfgDataSourceDB
            from bridge.models.cfg_data_table import CfgDataTable

            _self.db_detail = CfgDataSourceDB.get_by_id(_db_instance, _self.id)
            _self.csv_detail = CfgDataSourceCSV.get_by_id(
                _db_instance,
                _self.id,
                is_cascade_column=True,
            )
            _self.data_tables = CfgDataTable.get_by_data_source_id(_db_instance, _self.id)

        _get_relation_data_(self, _db_instance=db_instance)

    @classmethod
    def get_data_source_efa_and_v2(cls, db_instance: PostgreSQL, is_cascade: bool = False):
        dict_condition = {
            CfgDataSource.Columns.master_type.name: [(SqlComparisonOperator.NOT_EQUAL, MasterDBType.OTHERS.name)],
        }
        sql, params = gen_select_by_condition_sql(CfgDataSource, dict_condition)
        _col, rows = db_instance.run_sql(sql, params=params)

        if not rows:
            return []

        return [CfgDataSource(row, db_instance=db_instance, is_cascade=is_cascade) for row in rows]


EFA_TABLES = ['XR_PLOT', 'PART_LOG']
EFA_MASTERS = [MasterDBType.EFA.name, MasterDBType.EFA_HISTORY.name]
# EFA_TABLES = ['PART_LOG']
# EFA_MASTERS = [MasterDBType.EFA_HISTORY.name]


def get_data_sourced_types_for_db():
    return [MasterDBType.EFA.name, MasterDBType.OTHERS.name]


def get_data_sourced_types_for_csv():
    return [MasterDBType.V2.name, MasterDBType.OTHERS.name]


def get_v2_measure_data_source():
    return [
        '/app/sky04/share/data/GD-I4/rawzip/L6_Assy組付測定',
        '/app/sky04/share/data/GD-I4/rawzip/L6_Valve組付測定',
        '/app/sky04/share/data/GD-I4/rawzip/L6_調整検査測定',
        '/app/sky04/share/data/GD-I4/rawzip/L7_Assy組付測定',
        '/app/sky04/share/data/GD-I4/rawzip/L7_Body噴孔測定',
        '/app/sky04/share/data/GD-I4/rawzip/L7_Valve組付測定',
        '/app/sky04/share/data/GD-I4/rawzip/L7_調整検査測定',
    ]


def get_v2_history_data_source():
    return [
        '/app/sky04/share/data/GD-I4/rawzip/L6_Assy組付履歴',
        '/app/sky04/share/data/GD-I4/rawzip/L6_Valve組付履歴',
        '/app/sky04/share/data/GD-I4/rawzip/L7_Assy組付履歴',
        '/app/sky04/share/data/GD-I4/rawzip/L7_Valve組付履歴',
    ]
