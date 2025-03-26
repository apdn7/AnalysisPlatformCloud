from ap.common.constants import DataType
from ap.common.pydn.dblib.db_common import gen_select_by_condition_sql
from bridge.models.bridge_station import BridgeStationModel, ConfigModel
from bridge.models.cfg_csv_column import CfgCsvColumn
from bridge.models.cfg_data_source import CfgDataSource
from bridge.models.model_utils import TableColumn


class CfgDataSourceCSV(ConfigModel):
    class Columns(TableColumn):
        id = (1, DataType.INTEGER)
        directory = (2, DataType.TEXT)
        second_directory = (9, DataType.TEXT)
        skip_head = (3, DataType.INTEGER)
        skip_tail = (4, DataType.INTEGER)
        n_rows = (5, DataType.INTEGER)
        is_transpose = (6, DataType.BOOLEAN)
        delimiter = (7, DataType.TEXT)
        etl_func = (8, DataType.TEXT)
        dummy_header = (9, DataType.BOOLEAN)
        is_file_path = (10, DataType.BOOLEAN)
        created_at = (98, DataType.DATETIME)
        updated_at = (99, DataType.DATETIME)

    def __init__(self, dict_db_source=None):
        if not dict_db_source:
            dict_db_source = {}
        self.id = dict_db_source.get(CfgDataSourceCSV.Columns.id.name)
        if self.id is None:
            del self.id
        self.directory = dict_db_source.get(CfgDataSourceCSV.Columns.directory.name)
        self.second_directory = dict_db_source.get(CfgDataSourceCSV.Columns.second_directory.name)
        self.skip_head = dict_db_source.get(CfgDataSourceCSV.Columns.skip_head.name)
        self.skip_tail = dict_db_source.get(CfgDataSourceCSV.Columns.skip_tail.name)
        self.n_rows = dict_db_source.get(CfgDataSourceCSV.Columns.n_rows.name)
        self.is_transpose = dict_db_source.get(CfgDataSourceCSV.Columns.is_transpose.name)
        self.delimiter = dict_db_source.get(CfgDataSourceCSV.Columns.delimiter.name)
        self.etl_func = dict_db_source.get(CfgDataSourceCSV.Columns.etl_func.name)
        self.dummy_header = dict_db_source.get(CfgDataSourceCSV.Columns.dummy_header.name)
        self.is_file_path = dict_db_source.get(CfgDataSourceCSV.Columns.is_file_path.name)
        self.created_at = dict_db_source.get(CfgDataSourceCSV.Columns.created_at.name)
        self.updated_at = dict_db_source.get(CfgDataSourceCSV.Columns.updated_at.name)
        self.csv_columns = None

        # File current files is in processing in 1 loop of job.
        self.next_targets = []  # type: list[str]

    _table_name = 'cfg_data_source_csv'
    primary_keys = [Columns.id]

    @classmethod
    def get_by_id(cls, db_instance, db_source_id: int, is_cascade_column=False):
        dict_condition = {cls.Columns.id.name: db_source_id}
        sql, params = gen_select_by_condition_sql(cls, dict_condition)
        _col, rows = db_instance.run_sql(sql, params=params)
        if not rows:
            return None
        cfg_data_source_csv = CfgDataSourceCSV(rows[0])

        dict_condition = {CfgDataSource.Columns.id.name: cfg_data_source_csv.id}
        sql, params = gen_select_by_condition_sql(CfgDataSource, dict_condition)
        _col, rows = db_instance.run_sql(sql, params=params)
        if rows:
            cfg_data_source_csv.cfg_data_source = CfgDataSource(rows[0])

        if is_cascade_column:
            cfg_data_source_csv.csv_columns = CfgCsvColumn.get_by_data_source_id(
                db_instance,
                db_source_id,
            )
        return cfg_data_source_csv

    def get_column_names_with_sorted(self):
        """
        get column names that sorted by key
        :param self:
        :return:
        """
        if self.csv_columns is None:
            with BridgeStationModel.get_db_proxy() as db_instance:
                self.csv_columns = CfgCsvColumn.get_by_data_source_id(db_instance, self.id)
            self.csv_columns.sort(key=lambda csv_column: getattr(csv_column, CfgCsvColumn.Columns.id.name))
        return [col.column_name for col in self.csv_columns]

    @classmethod
    def save(cls, form):
        pass
        # if form.id:
        #     row = cls()
        # else:
        #     row = cls.query.filter(cls.id == form.id)
        #
        # # create dataSource ins
        # form.populate_obj(row)
        #
        # return row

    @classmethod
    def delete(cls, id):
        pass
        # cls.query.filter(cls.id == id).delete()
