from ap.common.constants import DBType
from ap.common.memoize import memoize
from ap.common.pydn.dblib.db_proxy_readonly import ReadOnlyDbProxy
from bridge.models.bridge_station import BridgeStationModel
from bridge.models.cfg_data_source import CfgDataSource
from bridge.models.cfg_data_source_db import CfgDataSourceDB
from grpc_server.services.grpc_service_proxy import grpc_api


@grpc_api()
def query_database_tables(db_id):
    with BridgeStationModel.get_db_proxy() as db_instance:
        dict_data_source = CfgDataSource.get_by_id(db_instance, db_id)
        data_source = CfgDataSource(dict_data_source)

        if not data_source:
            return None

        output = {'ds_type': data_source.type, 'tables': []}
        # return None if CSV
        if data_source.type.lower() == DBType.CSV.name.lower():
            return output

        data_source_db = CfgDataSourceDB.get_by_id(db_instance, db_id)
        updated_at = data_source_db.updated_at
        output['tables'] = get_list_tables_and_views(data_source_db.id, updated_at)

    return output


@memoize()
def get_list_tables_and_views(data_source_id, updated_at=None):
    # updated_at only for cache
    print('database config updated_at:', updated_at, ', so cache can not be used')
    with BridgeStationModel.get_db_proxy() as db_instance:
        data_source_db = CfgDataSourceDB.get_by_id(db_instance, data_source_id)

    with ReadOnlyDbProxy(data_source_db) as factory_db_instance:
        tables = factory_db_instance.list_tables_and_views()

    return tables
