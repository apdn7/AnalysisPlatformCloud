from ap.common.constants import ARCHIVED_COLS, ARCHIVED_ROWS, COLS, ROWS, TABLE_NAME
from ap.common.model_utils import get_dic_tablename_models
from ap.common.pydn.dblib.db_common import SqlComparisonOperator
from bridge.models.archived_master_config import ArchivedConfigMaster
from bridge.models.bridge_station import BridgeStationModel, ConfigModel, MasterModel, SemiMasterModel
from grpc_server.services.grpc_service_proxy import grpc_api


@grpc_api()
def get_config_master_changed_data(table_name, updated_at=None):
    with BridgeStationModel.get_db_proxy() as db_instance:
        # cls = ConfigModel.get_model_cls_by_table(table_name) or MasterModel.get_model_cls_by_table(table_name)
        dict_tables = get_dic_tablename_models([ConfigModel, MasterModel, SemiMasterModel])
        cls = dict_tables.get(table_name)
        if cls is None:
            return False

        # get changed data
        dic_conditions = None
        if updated_at:
            dic_conditions = {cls.Columns.updated_at.name: [(SqlComparisonOperator.GREATER_THAN, updated_at)]}

        # select updated data
        cols, rows = cls.select_records(db_instance, dic_conditions=dic_conditions, row_is_dict=False)
        dic_archived_conditions = {ArchivedConfigMaster.Columns.table_name.name: table_name}
        if dic_conditions:
            dic_archived_conditions.update(dic_conditions)

        select_cols = [
            ArchivedConfigMaster.Columns.archived_id.name,
            ArchivedConfigMaster.Columns.updated_at.name,
        ]
        archived_cols, archived_rows = ArchivedConfigMaster.select_records(
            db_instance,
            select_cols=select_cols,
            dic_conditions=dic_archived_conditions,
            row_is_dict=False,
        )
    dic_output = {
        TABLE_NAME: table_name,
        COLS: cols,
        ROWS: rows,
        ARCHIVED_COLS: archived_cols,
        ARCHIVED_ROWS: archived_rows,
    }

    return dic_output
