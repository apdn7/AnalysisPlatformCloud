import json
import os
import traceback

import flask
from flask import Blueprint, Response, jsonify, request
from flask_babel import gettext as _

from ap import background_jobs
from ap.api.common.services.show_graph_services import check_path_exist, sorted_function_details
from ap.api.setting_module.services.autolink import SortMethod, get_processes_id_order
from ap.api.setting_module.services.common import (
    delete_user_setting_by_id,
    get_all_user_settings,
    get_page_top_setting,
    get_setting,
    is_local_client,
    is_title_exist,
    parse_user_setting,
    save_user_settings,
)
from ap.api.setting_module.services.data_import import (
    add_transaction_import_job,
    check_timezone_changed,
    remove_transaction_import_jobs,
    write_error_cast_data_types,
)
from ap.api.setting_module.services.direct_import import (
    gen_config_data,
    get_column_name_for_column_attribute,
)
from ap.api.setting_module.services.equations import (
    EquationSampleData,
    is_all_new_functions,
    remove_all_function_columns,
    validate_functions,
)
from ap.api.setting_module.services.filter_settings import (
    delete_cfg_filter_from_db,
    get_filter_config_values,
    save_filter_config,
)
from ap.api.setting_module.services.master_data_job import add_scan_master_job
from ap.api.setting_module.services.polling_frequency import (
    add_idle_monitoring_job,
    add_pull_data_job,
    change_polling_all_interval_jobs,
)
from ap.api.setting_module.services.process_delete import (
    add_del_proc_job,
    del_data_source,
    delete_data_table_and_relate_jobs,
)
from ap.api.setting_module.services.save_load_user_setting import map_form, transform_settings
from ap.api.setting_module.services.show_latest_record import (
    gen_preview_data_check_dict,
    get_exist_data_partition,
    get_latest_records,
    get_process_config_info,
    get_well_known_columns,
    preview_csv_data,
    save_master_vis_config,
)
from ap.api.setting_module.services.shutdown_app import shut_down_app
from ap.api.trace_data.services.proc_link import add_gen_proc_link_job, add_restructure_indexes_job, show_proc_link_info
from ap.common.common_utils import (
    EXTENSIONS,
    add_seconds,
    get_export_setting_path,
    get_files,
    get_hostname,
    get_log_path,
    get_preview_data_path,
    is_empty,
    parse_int_value,
    remove_non_ascii_chars,
)
from ap.common.constants import (
    ANALYSIS_INTERFACE_ENV,
    FISCAL_YEAR_START_MONTH,
    MASTER_INFO,
    OSERR,
    STATUS,
    UI_ORDER_DB,
    UI_ORDER_PROC,
    UI_ORDER_TABLE,
    WITH_IMPORT_OPTIONS,
    Action,
    AnnounceEvent,
    AppEnv,
    CfgConstantType,
    DataColumnType,
    DataGroupType,
    DataType,
    DBType,
    JobStatus,
    MasterDBType,
    ProcessCfgConst,
    RelationShip,
    dict_convert_raw_data_type,
)
from ap.common.cryptography_utils import encrypt
from ap.common.logger import logger
from ap.common.memoize import clear_cache
from ap.common.pydn.dblib.db_proxy_readonly import check_db_con
from ap.common.scheduler import JobType, multiprocessingLock, remove_jobs, threadingLock
from ap.common.services.http_content import json_dumps, orjson_dumps
from ap.common.services.import_export_config_and_master_data import (
    clear_db_n_data,
    delete_file_and_folder_by_path,
    delete_folder_data,
    export_data,
    import_config_and_master,
    pause_job_running,
    pause_resume_current_running_jobs,
    reset_is_show_file_name,
    wait_done_jobs,
)
from ap.common.services.jp_to_romaji_utils import to_romaji
from ap.common.services.sse import background_announcer
from ap.setting_module.models import (
    AppLog,
    CfgConstant,
    CfgCsvColumn,
    CfgDataSource,
    CfgDataSourceCSV,
    CfgDataSourceDB,
    CfgDataTable,
    CfgDataTableColumn,
    CfgFilter,
    CfgFilterDetail,
    CfgProcess,
    CfgProcessColumn,
    CfgProcessFunctionColumn,
    CfgUserSetting,
    MData,
    MDataGroup,
    MFunction,
    crud_config,
    insert_or_update_config,
    make_session,
)
from ap.setting_module.schemas import (
    CfgUserSettingSchema,
    DataSourceSchema,
    DataTableSchema,
    ProcessColumnSchema,
    ProcessSchema,
)
from ap.setting_module.services.background_process import (
    get_background_jobs_service,
    get_job_detail_service,
)
from ap.setting_module.services.backup_and_restore.jobs import add_backup_data_job, add_restore_data_job
from ap.setting_module.services.process_config import (
    create_or_update_data_table_cfg,
    create_or_update_process_cfg,
    gen_function_column,
    gen_function_column_in_m_data,
    gen_partition_table_name,
    get_ct_range,
    get_data_table_cfg,
    get_data_tables_by_proc_id,
    get_efa_partitions,
    get_list_tables_and_views,
    get_process_cfg,
    get_process_columns,
    get_process_filters,
    get_process_visualizations,
    query_database_tables,
)
from ap.setting_module.services.register_from_file import (
    get_latest_records_for_register_by_file,
    get_url_to_redirect,
    handle_importing_by_one_click,
)
from ap.setting_module.services.trace_config import (
    gen_cfg_trace,
    get_all_processes_traces_info,
    trace_config_crud,
)
from bridge.models.bridge_station import BridgeStationModel, OthersDBModel, TransactionModel
from bridge.models.m_data_group import get_yoyakugo
from bridge.models.mapping_column import MappingColumn
from bridge.models.transaction_model import TransactionData
from bridge.services.clear_transaction_data import run_clean_trans_tables_job
from bridge.services.csv_management import add_scan_files_job
from bridge.services.data_import import MASTER_COL_TYPES
from bridge.services.etl_services.etl_db_service import (
    get_n_save_partition_range_time_from_factory_db,
)
from bridge.services.master_catalog import MasterColumnMetaCatalog
from bridge.services.proc_link_simulation import sim_gen_global_id
from grpc_server.connection import check_connection_to_server

api_setting_module_blueprint = Blueprint('api_setting_module', __name__, url_prefix='/ap/api/setting')


@api_setting_module_blueprint.route('/update_polling_freq', methods=['POST'])
def update_polling_freq():
    data_update = json.loads(request.data)
    with_import_option = data_update.get(WITH_IMPORT_OPTIONS)
    freq_min = parse_int_value(data_update.get(CfgConstantType.POLLING_FREQUENCY.name)) or 0

    # save/update POLLING_FREQUENCY to db
    freq_sec = freq_min * 60
    with make_session() as session:
        CfgConstant.create_or_update_by_type(
            session,
            const_type=CfgConstantType.POLLING_FREQUENCY.name,
            const_value=freq_sec,
        )

    is_user_request = bool(with_import_option and freq_min == 0)
    # re-set trigger time for all jobs
    change_polling_all_interval_jobs(interval_sec=freq_sec, run_now=with_import_option, is_user_request=is_user_request)

    message = {'message': _('Database Setting saved.'), 'is_error': False}

    return json_dumps(flask_message=message), 200


@api_setting_module_blueprint.route('/data_source_save', methods=['POST'])
def save_datasource_cfg():
    """
    Expected: ds_config = {"db_0001": {"master-name": name, "host": localhost, ...}}
    """
    try:
        data_src: CfgDataSource = DataSourceSchema().load(request.json)

        db_detail = data_src.db_detail
        # check force timezone
        if db_detail:
            use_os_tz = db_detail.use_os_timezone
            if check_timezone_changed(data_src.id, use_os_tz):
                print('CHANGED TIME ZONE')
                run_clean_trans_tables_job()

        partition_from = None
        partition_to = None
        is_has_data_table = False
        serial_cols, datetime_cols, order_cols = None, None, None
        with make_session() as meta_session:
            # data source: update cfg_data_source table
            data_src_rec: CfgDataSource = insert_or_update_config(
                meta_session,
                data_src,
                exclude_columns=[CfgDataSource.order.key],
            )

            # csv detail: update cfg_data_source_csv table
            csv_detail = data_src.csv_detail
            if csv_detail:
                # csv_detail.dummy_header = csv_detail.dummy_header == 'true' if csv_detail.dummy_header else None
                csv_columns = data_src.csv_detail.csv_columns
                csv_columns = [col for col in csv_columns if not is_empty(col.column_name)]
                data_src.csv_detail.csv_columns = csv_columns
                csv_detail_rec: CfgDataSourceCSV = insert_or_update_config(
                    meta_session,
                    csv_detail,
                    parent_obj=data_src_rec,
                    parent_relation_key=CfgDataSource.csv_detail.key,
                    parent_relation_type=RelationShip.ONE,
                )

                # CRUD
                csv_columns = csv_detail.csv_columns
                crud_config(
                    meta_session,
                    csv_columns,
                    CfgCsvColumn.data_source_id.key,
                    [CfgCsvColumn.column_name.key, CfgCsvColumn.directory_no.key],
                    parent_obj=csv_detail_rec,
                    parent_relation_key=CfgDataSourceCSV.csv_columns.key,
                    parent_relation_type=RelationShip.MANY,
                )

            # db detail
            cfg_data_source_db = None
            if db_detail:
                # encrypt password
                if db_detail.id:
                    cfg_data_source_db = CfgDataSourceDB.query.get(db_detail.id)
                if db_detail.password:
                    if cfg_data_source_db and cfg_data_source_db.hashed and len(db_detail.password) > 50:
                        db_detail.password = db_detail.password
                    else:
                        db_detail.password = encrypt(db_detail.password).decode()
                db_detail.hashed = True
                # avoid blank string
                db_detail.port = db_detail.port or None
                db_detail.schema = db_detail.schema or None
                insert_or_update_config(
                    meta_session,
                    db_detail,
                    parent_obj=data_src_rec,
                    parent_relation_key=CfgDataSource.db_detail.key,
                    parent_relation_type=RelationShip.ONE,
                )
            # commit data to get id
            meta_session.commit()

            # dump ds
            ds_schema = DataSourceSchema()
            data_source = CfgDataSource.get_ds(data_src_rec.id)
            ds = ds_schema.dumps(data_source)

            # run V2 zip files
            # TODO: move to SCAN button
            # if data_src_rec.master_type in (MasterDBType.V2.name, MasterDBType.V2_HISTORY.name):
            #     add_zip_files_job(data_source_id=data_src_rec.id)

            data_source_id = data_src_rec.id

            # In case user execute direct import, system will add data table and do scan-gen master automatically
            # without Mapping Config page
            dict_tables = query_database_tables(data_source_id)
            cfg_data_table = CfgDataTable.get_by_data_source_id(data_source_id)
            if cfg_data_table:
                is_has_data_table = True
                partition_from = cfg_data_table.partition_from
                partition_to = cfg_data_table.partition_to

            partitions = dict_tables.get('partitions')
            detail_master_types = dict_tables.get('detail_master_types')

            if data_src.is_direct_import and data_src_rec.master_type in [MasterDBType.V2.name]:
                # if data_src.is_direct_import and data_src_rec.master_type in [MasterDBType.OTHERS.name,
                #                                                               MasterDBType.V2.name]:
                insert_data_table_config_direct(meta_session, data_source_id, detail_master_types=detail_master_types)

            if data_src.is_direct_import and data_src_rec.master_type == MasterDBType.OTHERS.name:
                serial_cols, datetime_cols, order_cols = get_column_name_for_column_attribute(data_source)

    except Exception as e:
        logger.exception(e)
        message = {'message': _('Database Setting failed to save'), 'is_error': True}
        return json_dumps(flask_message=message), 500

    message = {'message': _('Database Setting saved.'), 'is_error': False}
    return (
        json_dumps(
            id=data_source_id,
            data_source_name=data_source.name,
            data_source=ds,
            flask_message=message,
            is_has_data_table=is_has_data_table,
            serial_cols=serial_cols,
            datetime_cols=datetime_cols,
            order_cols=order_cols,
            partitions=partitions,
            partition_from=partition_from,
            partition_to=partition_to,
        ),
        200,
    )


@api_setting_module_blueprint.route('/save_data_table_config_direct', methods=['POST'])
def save_data_table_config_direct():
    data = json.loads(request.data)
    data_source_id = data.get('data_source_id')
    partition_from = data.get('partition_from')
    partition_to = data.get('partition_to')
    serial_col = data.get('serial')
    datetime_col = data.get('datetime')
    order_col = data.get('order')
    with make_session() as meta_session:
        data_table = insert_data_table_config_direct(
            meta_session,
            data_source_id,
            serial_col,
            datetime_col,
            order_col,
            partition_from,
            partition_to,
        )

    return json_dumps({'status': 200, 'data': data_table})


def insert_data_table_config_direct(
    meta_session,
    data_source_id,
    serial_col=None,
    datetime_col=None,
    order_col=None,
    partition_from=None,
    partition_to=None,
    detail_master_types=None,
):
    cfg_data_source, cfg_data_tables = gen_config_data(
        meta_session,
        data_source_id,
        serial_col,
        datetime_col,
        order_col,
        partition_from,
        partition_to,
        detail_master_types,
    )  # type: CfgDataSource, list[CfgDataTable]

    if not cfg_data_tables:
        return {}, 200

    data_tables = []
    for cfg_data_table in cfg_data_tables:
        get_n_save_partition_range_time_from_factory_db(cfg_data_table, is_scan=True)
        # 4. Scan & gen master data automatically (without exporting scan master files)
        if cfg_data_source.csv_detail:
            # ['登録日時', '工程名', 'ライン名']
            # split_cols = [DataGroupType.DATA_TIME, DataGroupType.PROCESS_NAME, DataGroupType.LINE_NAME]
            split_cols = CfgDataTableColumn.get_split_columns(cfg_data_table.id)
            columns = CfgDataTableColumn.get_column_names_by_data_group_types(cfg_data_table.id, split_cols)
            add_scan_files_job(data_table_id=cfg_data_table.id, columns=columns, is_scan_master=True)
        else:
            add_scan_master_job(cfg_data_table.id)

        data_tables.append(
            {
                CfgDataTable.id.key: cfg_data_table.id,
                CfgDataTable.name.key: cfg_data_table.name or '',
                CfgDataTable.data_source_id.key: cfg_data_table.data_source_id or '',
                'data_source': cfg_data_source.as_dict(),
                CfgDataTable.table_name.key: cfg_data_table.table_name or '',
                CfgDataTable.comment.key: cfg_data_table.comment or '',
            },
        )
    return data_tables


# TODO: refactoring check connection without this function
@api_setting_module_blueprint.route('/database_tables', methods=['GET'])
def get_database_tables():
    db_tables = CfgDataSource.get_all()
    ds_schema = DataSourceSchema(many=True)
    dump_data = ds_schema.dumps(db_tables)
    list_ds = json.loads(dump_data)
    for ds in list_ds:
        ds['en_name'] = to_romaji(ds['name'])
    dump_data = json_dumps(list_ds)
    if not db_tables:
        logger.debug('There are no data source in DB, please check!!!')
    return dump_data, 200 if db_tables else 500


@api_setting_module_blueprint.route('/database_tables_source', methods=['GET'])
def get_database_tables_source():
    db_source = CfgDataSource.get_all_db_source()
    ds_schema = DataSourceSchema(many=True)
    dump_data = ds_schema.dumps(db_source)
    return dump_data, 200 if db_source else 500


@api_setting_module_blueprint.route('/database_table/<db_id>', methods=['GET'])
def get_database_table(db_id):
    if not db_id:
        return json_dumps({'tables': [], 'msg': 'Invalid data source id'}), 400

    dict_tables = query_database_tables(db_id)

    if dict_tables is None:
        return json_dumps({'tables': [], 'msg': 'Invalid data source id'}), 400
    else:
        return json_dumps(dict_tables), 200


@api_setting_module_blueprint.route('/get_partition_table', methods=['POST'])
def get_partition_table():
    data = json.loads(request.data)
    db_id = data.get('data_source_id')
    table_prefix = data.get('table_prefix')

    dict_tables = query_database_tables(db_id, table_prefix)

    if dict_tables is None:
        return json_dumps({'tables': [], 'msg': 'Invalid data source id'}), 400
    else:
        return json_dumps(dict_tables), 200


@api_setting_module_blueprint.route('/check_db_connection', methods=['POST'])
def check_db_connection():
    """Check if we can connect to database. Supported databases: SQLite, PostgreSQL, MSSQLServer.
    Returns:
        HTTP Response - (True + OK message) if connection can be established, return (False + NOT OK message) otherwise.
    """
    params = json.loads(request.data).get('db')
    db_type = str(params.get('db_type')).upper()
    host = params.get('host')
    port = params.get('port')
    dbname = params.get('dbname')
    schema = params.get('schema')
    username = params.get('username')
    password = params.get('password')
    id = params.get('id')

    result = None
    try:
        result = check_db_con(id, db_type, host, port, dbname, schema, username, password)
    except Exception as e:
        logger.exception(e)

    if result:
        message = {'db_type': db_type, 'message': _('Connected'), 'connected': True}
    else:
        message = {'db_type': db_type, 'message': _('Failed to connect'), 'connected': False}

    return json_dumps(flask_message=message), 200


@api_setting_module_blueprint.route('/show_latest_records_for_register_by_file', methods=['POST'])
def show_latest_records_for_register_by_file():
    """[summary]
    Show 5 latest records
    Returns:
        [type] -- [description]
    """
    dic_form = request.form.to_dict()
    file_name = dic_form.get('fileName') or None
    limit = parse_int_value(dic_form.get('limit')) or 10
    folder = dic_form.get('folder') or None
    latest_rec = get_latest_records_for_register_by_file(file_name, folder, limit)
    return json_dumps(latest_rec)


@api_setting_module_blueprint.route('/show_latest_records', methods=['POST'])
def show_latest_records():
    """[summary]
    Show 5 latest records
    Returns:
        [type] -- [description]
    """
    # TODO: make show latest records works with pydantic
    dic_form = request.form.to_dict()
    data_source_id = dic_form.get('databaseName') or dic_form.get('dataTableDsID') or None
    data_table_name = dic_form.get('dataTableName')
    table_name = dic_form.get('tableName') or None
    file_name = dic_form.get('fileName') or None
    limit = parse_int_value(dic_form.get('limit')) or 5  # BS show 5 record in column attribute modal
    detail_master_type = dic_form.get('detailMasterType') or ''
    if not table_name:
        data_table_id = dic_form.get('dataTableId')
        if data_table_id:
            cfg_data_table = CfgDataTable.get_by_id(data_table_id)
            if cfg_data_table is not None:
                table_name = cfg_data_table.table_name

    data_source = CfgDataSource.query.get(data_source_id)  # type: CfgDataSource
    if data_source.master_type == MasterDBType.EFA.name:
        exist_data_table = None
        partition_tables = None
        partition_to = dic_form.get('partitionTo')
        if partition_to:
            partition_to_table_name = gen_partition_table_name(table_name, partition_to)
            exist_data_table = get_exist_data_partition(data_source, [partition_to_table_name])

        if exist_data_table is None:
            tables = get_list_tables_and_views(data_source)
            *_, partition_tables = get_efa_partitions(tables, table_name)
            exist_data_table = get_exist_data_partition(data_source, partition_tables)

        table_name = exist_data_table if exist_data_table else partition_to if partition_to else partition_tables[-1]

    latest_rec = get_latest_records(data_source, table_name, file_name, limit, detail_master_type)
    # TODO: dummy datetime
    # cols_with_types, rows, cols_duplicated, previewed_files, has_ct_col, dummy_datetime_idx = latest_rec
    cols_with_types, rows, cols_duplicated, previewed_files, master_type, is_rdb = latest_rec
    cfg_data_source: CfgDataSource = CfgDataSource.get_by_id(data_source_id)
    is_csv = cfg_data_source.type == DBType.CSV.name
    if is_csv:
        file_name_info = {
            'name': DataGroupType.FileName.name,
            'column_name': DataGroupType.FileName.name,
            'data_type': DataType.TEXT.value,
            'name_en': DataGroupType.FileName.name,
            'romaji': DataGroupType.FileName.name,
            'is_get_date': False,
            'check_same_value': {'is_null': False, 'is_same': False, 'is_dupl': False},
            'is_big_int': False,
            'name_jp': DataGroupType.FileName.name,
            'name_local': DataGroupType.FileName.name,
            'column_raw_name': DataGroupType.FileName.name,
            'is_show': False,
        }
        cols_with_types.append(file_name_info)
    yoyakugo = get_yoyakugo()

    all_cols = [dict_col['column_name'] for dict_col in cols_with_types]
    well_known_columns = get_well_known_columns(master_type, all_cols)
    if is_csv:  # add FileName for well knows columns
        well_known_columns[DataGroupType.FileName.name] = DataGroupType.HORIZONTAL_DATA.value

    # TODO: filter columns that only are process_name, data_name, data_value, datetime column
    sorted_cols_with_types = []
    allow_column_type = [
        DataGroupType.DATA_ID.value,
        DataGroupType.DATA_NAME.value,
        DataGroupType.PROCESS_ID.value,
        DataGroupType.PROCESS_NAME.value,
        DataGroupType.DATA_VALUE.value,
        DataGroupType.DATA_TIME.value,
    ]
    for col in cols_with_types:
        col['is_checked'] = True
        # OTHERS show original order columns in data source
        if (
            master_type != MasterDBType.OTHERS.name
            and well_known_columns.get(col.get('column_name')) in allow_column_type
        ):
            col['is_show'] = True
            sorted_cols_with_types.insert(0, col)
        else:
            col['is_show'] = col['column_name'] != DataGroupType.FileName.name
            sorted_cols_with_types.append(col)

    dic_preview_limit = gen_preview_data_check_dict(rows, previewed_files)
    if data_source.data_tables:
        for data_table in data_source.data_tables:
            if data_table.name == data_table_name:
                columns = data_table.columns
                column_names = [column.column_name for column in columns]
                for column in columns:
                    well_known_columns[column.column_name] = column.data_group_type

                well_known_columns = {column: well_known_columns[column] for column in column_names}

    # handle guess column order for OTHERS master type
    if (
        master_type == MasterDBType.OTHERS.name
        and DataGroupType.AUTO_INCREMENTAL.value not in well_known_columns.values()
        and DataGroupType.DATA_TIME.value not in well_known_columns.values()
    ):
        for col in sorted_cols_with_types:
            if col['data_type'] == DataType.DATETIME.value:
                well_known_columns[col['column_raw_name']] = DataGroupType.AUTO_INCREMENTAL.value
                break

    data_group_type = {key: DataColumnType[key].value for key in DataColumnType.get_keys()}
    result = {
        'cols': sorted_cols_with_types,  # cols_with_types,
        'rows': rows,
        'cols_duplicated': cols_duplicated,
        'fail_limit': dic_preview_limit,
        # 'has_ct_col': has_ct_col,
        # 'dummy_datetime_idx': None if is_rdb else dummy_datetime_idx,
        'yoyakugo': yoyakugo,
        'auto_mapping_rules': well_known_columns,
        'data_group_type': data_group_type,
        'is_rdb': is_rdb,
    }

    return json_dumps(result)


@api_setting_module_blueprint.route('/get_csv_resources', methods=['POST'])
def get_csv_resources():
    folder_url = request.json.get('url')
    etl_func = request.json.get('etl_func')
    csv_delimiter = request.json.get('delimiter')
    is_v2 = request.json.get('isV2')
    # force line_skip to be None if we are inserting v2 data
    line_skip = None if is_v2 else request.json.get('line_skip', None)
    n_rows = request.json.get('n_rows')
    n_rows = None if n_rows is None else int(n_rows)
    is_transpose = request.json.get('is_transpose')
    is_file = request.json.get('is_file')

    # Get column names in file that have been normalized
    dic_output = preview_csv_data(
        folder_url,
        etl_func,
        csv_delimiter,
        line_skip=line_skip,
        n_rows=n_rows,
        is_transpose=is_transpose,
        limit=5,
        file_name=folder_url if is_file else None,
    )
    rows = dic_output['content']
    previewed_files = dic_output['previewed_files']
    dic_preview_limit = gen_preview_data_check_dict(rows, previewed_files)
    dic_output['fail_limit'] = dic_preview_limit

    return json_dumps(dic_output), 200


@api_setting_module_blueprint.route('/job', methods=['POST'])
def get_background_jobs():
    return json_dumps(background_jobs), 200


@api_setting_module_blueprint.route('/listen_background_job/<is_force>/<uuid>/<main_tab_uuid>', methods=['GET'])
def listen_background_job(is_force: str, uuid: str, main_tab_uuid: str):
    is_reject = False
    is_force = int(is_force)
    compare_time = add_seconds(seconds=-background_announcer.FORCE_SECOND)
    with multiprocessingLock, threadingLock:
        start_time = None
        if background_announcer.is_exist(uuid):
            if is_force:
                start_time = background_announcer.get_start_date(uuid)
                if start_time >= compare_time:
                    is_reject = True
            elif not background_announcer.is_exist(uuid, main_tab_uuid=main_tab_uuid):
                is_reject = True

        logger.debug(
            f'[SSE] {"Rejected" if is_reject else "Accepted"}: UUID = {uuid}; main_tab_uuid = {main_tab_uuid};'
            f' is_force = {is_force}; compare_time = {compare_time}; start_time = {start_time};',
        )

        if is_reject:
            return Response('SSE Rejected', status=202)

        return Response(
            background_announcer.init_stream_sse(uuid, main_tab_uuid),
            mimetype='text/event-stream',
        )


@api_setting_module_blueprint.route('/check_folder', methods=['POST'])
def check_folder():
    try:
        data = request.json.get('url')
        is_file = request.json.get('isFile') or False
        is_existing = os.path.isfile(data) if is_file else (os.path.isdir(data) and os.path.exists(data))
        is_valid_file = True
        if not is_file:
            os.listdir(data)
            is_not_empty = False
            files = get_files(data, depth_from=1, depth_to=100, extension=EXTENSIONS)
            for file in files:
                is_not_empty = any(file.lower().endswith(ext) for ext in EXTENSIONS)
                if is_not_empty:
                    break
        else:
            is_valid_file = any(data.lower().endswith(ext) for ext in EXTENSIONS)
            is_not_empty = os.path.isfile(data)

        is_valid = is_existing and is_not_empty
        err_msg = _('File not found')  # empty folder
        file_not_valid = _('File not valid')
        return json_dumps(
            {
                'status': 200,
                'url': data,
                'is_exists': is_existing,
                'dir': os.path.dirname(data),
                'not_empty_dir': is_not_empty,
                'is_valid': is_valid,
                'err_msg': err_msg if not is_valid else file_not_valid if not is_valid_file else '',
                'is_valid_file': is_valid_file,
            },
        )
    except OSError as e:
        # raise
        return jsonify({'status': 500, 'err_msg': _(OSERR[e.errno]), 'is_valid': False})


@api_setting_module_blueprint.route('/check_folder_or_file', methods=['POST'])
def check_folder_or_file():
    try:
        data = request.json.get('path')
        return jsonify(
            {
                'status': 200,
                'isFile': os.path.isfile(data),
                'isFolder': os.path.isdir(data),
            },
        )
    except OSError as e:
        # raise
        return jsonify(
            {
                'status': 500,
                'err_msg': _(OSERR[e.errno]),
                'isFile': False,
                'isFolder': False,
            },
        )


@api_setting_module_blueprint.route('/job_detail/<job_id>', methods=['GET'])
def get_job_detail(job_id):
    """[Summary] Get job details
    Returns:
        [json] -- [job details content]
    """
    job_details = get_job_detail_service(job_id=job_id)
    return json_dumps(job_details), 200


@api_setting_module_blueprint.route('/delete_process', methods=['POST'])
def delete_proc_from_db():
    # get proc_id
    params = json.loads(request.data)
    proc_id = params.get('proc_id')
    proc_id = int(proc_id) or None
    target_jobs = [JobType.TRANSACTION_IMPORT, JobType.TRANSACTION_PAST_IMPORT]
    remove_jobs(target_jobs, proc_id=proc_id)
    add_del_proc_job(proc_id)
    return json_dumps(result={}), 200


@api_setting_module_blueprint.route('/delete_data_table', methods=['POST'])
def delete_data_table_from_db():
    # get proc_id
    params = json.loads(request.data)
    data_table_id = params.get('data_table_id')

    # delete config and add job to delete data
    delete_data_table_and_relate_jobs(data_table_id)

    return jsonify(result={}), 200


@api_setting_module_blueprint.route('/save_order/<order_name>', methods=['POST'])
def save_order(order_name):
    """[Summary] Save orders to DB
    Returns: 200/500
    """
    try:
        orders = json.loads(request.data)
        if '' in orders.keys():
            orders.pop('', None)
        with make_session() as meta_session:
            if order_name == UI_ORDER_DB:
                for key, val in orders.items():
                    CfgDataSource.update_order(meta_session, key, val)
            elif order_name == UI_ORDER_TABLE:
                for key, val in orders.items():
                    CfgDataTable.update_order(meta_session, key, val)
            elif order_name == UI_ORDER_PROC:
                for key, val in orders.items():
                    CfgProcess.update_order(meta_session, key, val)
            else:
                for key, val in orders.items():
                    CfgFilterDetail.update_order(meta_session, key, val)

    except Exception:
        traceback.print_exc()
        return json_dumps({}), 500

    return json_dumps({}), 200


@api_setting_module_blueprint.route('/delete_datasource_cfg', methods=['POST'])
def delete_datasource_cfg():
    params = json.loads(request.data)
    data_source_id = params.get('db_code')
    if data_source_id:
        del_data_source(int(data_source_id))

    return json_dumps(id=data_source_id), 200


@api_setting_module_blueprint.route('/shutdown', methods=['POST'])
def stop_jobs():
    try:
        if not is_local_client(request):
            return json_dumps({}), 403

        # save log to db
        with make_session() as meta_session:
            t_app_log = AppLog()
            t_app_log.ip = request.environ.get('X-Forwarded-For') or request.remote_addr
            t_app_log.action = Action.SHUTDOWN_APP.name
            t_app_log.description = request.user_agent.string
            meta_session.add(t_app_log)
    except Exception as ex:
        traceback.print_exc()
        logger.error(ex)

    # backup database now
    # add_backup_dbs_job(True)

    # add a job to check for shutdown time
    # add_shutdown_app_job()
    shut_down_app()

    response = flask.make_response(jsonify({}))
    response.set_cookie('locale', '', 0)
    return response, 200


@api_setting_module_blueprint.route('/function_config', methods=['POST'])
def post_function_config():
    data = request.json
    functions = data.get('functions')
    proc_id = data.get('process_id')
    dict_rename_col_id = {}
    sorted_functions = sorted(functions, key=lambda x: x.get(CfgProcessFunctionColumn.order.key))
    validate_functions(proc_id, sorted_functions)

    with make_session() as meta_session:
        df_function_column = CfgProcessFunctionColumn.get_by_process_id(proc_id, session=meta_session)
        func_col_ids = [_id.item() for _id in df_function_column['process_function_column_id'].tolist()]
        exist_func_col_ids = []

        # In case of paste all, exist function columns will be deleted, request function columns will be newly inserted
        if sorted_functions and is_all_new_functions(sorted_functions):
            remove_all_function_columns(meta_session, proc_id)

        for dic_func in sorted_functions:
            is_me_func = dic_func.pop('is_me_function')
            dic_proc_col = dic_func.pop('process_column')
            is_new_col = False
            if not is_me_func and dic_proc_col['id'] < 0:
                old_col_id = dic_proc_col['id']
                dic_proc_col['id'] = None
                is_new_col = True

            cfg_func = CfgProcessFunctionColumn(**{key: val if val != '' else None for key, val in dic_func.items()})

            if cfg_func.id < 0:
                cfg_func.id = None

            if cfg_func.var_x and cfg_func.var_x < 0:
                cfg_func.var_x = dict_rename_col_id.get(cfg_func.var_x)

            if cfg_func.var_y and cfg_func.var_y < 0:
                cfg_func.var_y = dict_rename_col_id.get(cfg_func.var_y)

            if is_me_func:
                if cfg_func.process_column_id and cfg_func.process_column_id < 0:
                    cfg_func.process_column_id = dict_rename_col_id.get(cfg_func.process_column_id)
                exist_func_col_ids.append(cfg_func.id)
                meta_session.merge(cfg_func)
            else:
                cfg_col = CfgProcessColumn(**{key: val if val != '' else None for key, val in dic_proc_col.items()})
                # convert raw_data_type to data_type(same ES)
                cfg_col.data_type = dict_convert_raw_data_type.get(cfg_col.raw_data_type, cfg_col.data_type)
                if is_new_col:
                    # save equation column
                    new_ids = gen_function_column_in_m_data(cfg_col, cfg_func=cfg_func)
                    new_col_id = new_ids.get('data_id')
                    dict_rename_col_id[old_col_id] = new_col_id
                    cfg_col.id = new_col_id
                else:
                    exist_func_col_ids.append(cfg_func.id)

                if cfg_func.process_column_id < 0:
                    cfg_func.process_column_id = dict_rename_col_id[cfg_func.process_column_id]

                cfg_col.function_details = [cfg_func]
                meta_session.merge(cfg_col)

        # remove function column not exist in request function columns
        not_exits_func_col_ids = set(func_col_ids) - set(exist_func_col_ids)
        for func_col_id in not_exits_func_col_ids:
            func_col: CfgProcessFunctionColumn = CfgProcessFunctionColumn.get_by_id(func_col_id)
            if not func_col:
                # In case this function already was deleted by cascade
                continue
            elif func_col.is_me_function:
                CfgProcessFunctionColumn.delete_by_ids([func_col_id], session=meta_session)
            else:
                CfgProcessColumn.delete_by_ids([func_col.process_column_id], session=meta_session)
                MData.delete_by_ids([func_col.process_column_id], session=meta_session)

    cfg_col_ids = CfgProcessFunctionColumn.get_all_cfg_col_ids()

    return jsonify({'cfg_col_ids': cfg_col_ids, 'dict_rename_col_id': dict_rename_col_id}), 200


@api_setting_module_blueprint.route('/proc_config', methods=['POST'])
def post_proc_config(proc_data=None, unchecked_proc_data=None, allow_unchecked_proc_data=False):
    # TODO: separate test functions and production function.
    # Should we allow unchecked_proc_data to be empty or not?
    if not unchecked_proc_data and allow_unchecked_proc_data:
        unchecked_proc_data = {}

    if proc_data and unchecked_proc_data is not None:
        should_import_data = True
    else:
        process_schema = ProcessSchema()
        proc_data = process_schema.load(request.json.get('proc_config'))
        unchecked_proc_data = process_schema.load(request.json.get('uncheck_proc_config'))
        should_import_data = request.json.get('import_data')
    error_type = None
    try:
        # get exists process from id
        proc_id = proc_data.get(ProcessCfgConst.PROC_ID.value)
        if proc_id:
            process = CfgProcess.get_by_id(proc_id)
            if not process:
                return (
                    json_dumps(
                        {
                            'status': 404,
                            'message': 'Not found {}'.format(proc_id),
                        },
                    ),
                    200,
                )

            with BridgeStationModel.get_db_proxy() as db_instance:
                error_type = 'CastError'
                transaction_data_obj = TransactionData(process.id)
                is_success, failed_change_columns = transaction_data_obj.cast_data_type_for_columns(
                    db_instance,
                    process,
                    proc_data,
                )

                if not is_success:
                    # Collect data to send to front-end and show it on modal
                    failed_column_data = transaction_data_obj.get_failed_cast_data(db_instance, failed_change_columns)

                    # Export data that cannot convert to new data type to csv file
                    file_full_path = write_error_cast_data_types(process, failed_column_data)

                    return (
                        json_dumps(
                            {
                                'status': 500,
                                'message': _(
                                    'Cast error: There are some columns that cannot be cast to another'
                                    ' data type. Please check the real data of the columns listed below.'
                                    ' You can also see the data that will be exported and stored in file'
                                    ' path {0}',
                                ).format(file_full_path),
                                'errorType': error_type,
                                'data': {
                                    # Convert failed_column_data to truly dictionary
                                    column.id: {'detail': ProcessColumnSchema().dump(column), 'data': data}
                                    for column, data in failed_column_data.items()
                                },
                            },
                        ),
                        200,
                    )
                else:
                    error_type = None

        process = create_or_update_process_cfg(
            proc_data,
            unchecked_proc_data=unchecked_proc_data,
        )

        # create process json
        process_schema = ProcessSchema()
        cfg_data_table_dicts = CfgDataTable.get_by_process_id(process.id)
        data_source_ids = [e.get('data_source_id') for e in cfg_data_table_dicts]
        cfg_data_source_dicts = [source.as_dict() for source in CfgDataSource.get_in_ids(data_source_ids)]
        process.data_sources = list(cfg_data_table_dicts)
        process.data_source_name = ' | '.join([e.get('name') for e in cfg_data_table_dicts])
        process.data_tables = list(cfg_data_source_dicts)
        process.data_table_name = ' | '.join([e.get('name') for e in cfg_data_source_dicts])
        process_json = process_schema.dump(process) or {}

        # import data
        if should_import_data:
            interval_sec = CfgConstant.get_value_by_type_first(CfgConstantType.POLLING_FREQUENCY.name, int)
            add_transaction_import_job(process.id, interval_sec=interval_sec, run_now=True)

            # pull data table
            cfg_data_tables = get_data_tables_by_proc_id(process.id)
            for cfg_data_table in cfg_data_tables:
                add_pull_data_job(
                    cfg_data_table,
                    interval_sec=interval_sec,
                    run_now=True,
                    import_process_id=process.id,
                )

        return (
            json_dumps(
                {
                    'status': 200,
                    'data': process_json,
                },
            ),
            200,
        )
    except Exception as ex:
        traceback.print_exc()
        return (
            json_dumps(
                {
                    'status': 500,
                    'message': str(ex),
                    'errorType': error_type,
                },
            ),
            500,
        )


@api_setting_module_blueprint.route('/trace_config', methods=['GET'])
def get_trace_configs():
    """[Summary] Save orders to DB
    Returns: 200/500
    """
    try:
        procs = get_all_processes_traces_info()
        # generate english name for process
        for proc_data in procs:
            for column in proc_data.get('columns', []):
                column['is_master_col'] = column.get(CfgProcessColumn.column_type.name) in MASTER_COL_TYPES
            if not proc_data['name_en']:
                proc_data['name_en'] = to_romaji(proc_data['name'])
        # use jsonify here ok.
        return jsonify({'trace_config': {'procs': procs}}), 200
    except Exception:
        traceback.print_exc()
        return jsonify({}), 500


@api_setting_module_blueprint.route('/trace_config', methods=['POST'])
def save_trace_configs():
    """[Summary] Save trace_configs to DB
    Returns: 200/500
    """

    try:
        traces = json.loads(request.data)
        trace_config_crud(traces)
        add_restructure_indexes_job()
        add_gen_proc_link_job(is_user_request=True)
    except Exception:
        traceback.print_exc()
        return json_dumps({}), 500

    return json_dumps({}), 200


@api_setting_module_blueprint.route('/ds_load_detail/<ds_id>', methods=['GET'])
def ds_load_detail(ds_id):
    ds_schema = DataSourceSchema()
    ds = CfgDataSource.get_ds(ds_id)
    return ds_schema.dumps(ds), 200


@api_setting_module_blueprint.route('/proc_config/<proc_id>', methods=['DELETE'])
def del_proc_config(proc_id):
    return (
        json_dumps(
            {
                'status': 200,
                'data': {
                    'proc_id': proc_id,
                },
            },
        ),
        200,
    )


@api_setting_module_blueprint.route('/proc_config/<proc_id>', methods=['GET'])
def get_proc_config(proc_id):
    process_detail = get_process_config_info(proc_id)
    if not process_detail:
        return json_dumps({'status': 404, 'data': 'Not found'}), 200

    return json_dumps(process_detail), 200


@api_setting_module_blueprint.route('/proc_filter_config/<proc_id>', methods=['GET'])
def get_proc_config_filter_data(proc_id):
    process = get_process_cfg(proc_id)
    # filter_col_data = get_filter_col_data(process) or {}
    columns = process.get('columns')
    process['columns'] = [
        column
        for column in columns
        if column.get(CfgProcessColumn.column_type.name) in [DataGroupType.GENERATED.value]
        and not len(column.get(CfgProcessColumn.function_details.key))
    ]
    filter_col_data = {}
    if process:
        if not process['name_en']:
            process['name_en'] = to_romaji(process['name'])
        return (
            json_dumps(
                {
                    'status': 200,
                    'data': process,
                    'filter_col_data': filter_col_data,
                },
            ),
            200,
        )
    else:
        return (
            json_dumps(
                {
                    'status': 404,
                    'data': {},
                    'filter_col_data': {},
                },
            ),
            200,
        )


@api_setting_module_blueprint.route('/proc_table_viewer_columns/<proc_id>', methods=['GET'])
def get_table_viewer_columns(proc_id):
    process = get_process_cfg(proc_id)
    # get data source of process
    data_table_dict = CfgDataTable.get_by_process_id(proc_id)
    data_source_ids = [e.get('id') for e in data_table_dict]
    cfg_data_table: CfgDataTable = CfgDataTable.get_by_id(data_source_ids[0])  # TODO: get all data table
    process_schema = DataSourceSchema()
    process['data_source'] = process_schema.dump(cfg_data_table.data_source)
    process['data_tables'] = [cfg_data_table]
    columns = []
    for column in process.get('columns'):
        if column.get(CfgProcessColumn.column_type.name) in DataGroupType.get_hide_column_type_cfg_proces_columns():
            continue

        if column.get(CfgProcessColumn.column_raw_name.name) == DataGroupType.FileName.name:
            continue

        columns.append(column)

    process['columns'] = columns
    if process:
        if not process['name_en']:
            process['name_en'] = to_romaji(process['name'])
        return (
            json_dumps(
                {
                    'status': 200,
                    'data': process,
                    'filter_col_data': {},
                },
            ),
            200,
        )
    else:
        return (
            json_dumps(
                {
                    'status': 404,
                    'data': {},
                    'filter_col_data': {},
                },
            ),
            200,
        )


@api_setting_module_blueprint.route('/proc_config/<proc_id>/columns', methods=['GET'])
def get_proc_column_config(proc_id):
    columns = get_process_columns(proc_id)
    if columns:
        return (
            json_dumps(
                {
                    'status': 200,
                    'data': columns,
                },
            ),
            200,
        )
    else:
        return (
            json_dumps(
                {
                    'status': 404,
                    'data': [],
                },
            ),
            200,
        )


@api_setting_module_blueprint.route('/proc_config/<proc_id>/get_ct_range', methods=['GET'])
def get_proc_ct_range(proc_id):
    columns = get_process_columns(proc_id)
    ct_range = get_ct_range(proc_id, columns)
    return (
        jsonify(
            {
                'status': 200,
                'data': ct_range,
            },
        ),
        200,
    )


@api_setting_module_blueprint.route('/proc_config/<proc_id>/filters', methods=['GET'])
def get_proc_filter_config(proc_id):
    filters = get_process_filters(proc_id)
    if filters:
        for filter in filters:
            master_info = {}
            if filter.get(CfgFilter.filter_type.name) in [
                DataGroupType.LINE.name,
                DataGroupType.EQUIP.name,
                DataGroupType.PART.name,
            ]:
                filter_master_ids = [
                    filter_detail.get(CfgFilterDetail.filter_condition.name)
                    for filter_detail in filter.get('filter_details')
                ]
                column: CfgProcessColumn = CfgProcessColumn.get_by_id(filter.get(CfgFilter.column_id.name))
                data_group_type = DataGroupType(column.column_type)
                column_meta_data = MasterColumnMetaCatalog.instance(data_group_type)
                if column_meta_data is None:
                    continue
                hover_model = column_meta_data.hover_model
                with make_session() as db_session:
                    hover_datas = hover_model.get_hover_by_ids(db_session=db_session, ids=filter_master_ids)
                    for hover in hover_datas:
                        master_info.update({hover.id: hover.model_dump(mode='json')})

            filter[MASTER_INFO] = master_info
        return (
            json_dumps(
                {
                    'status': 200,
                    'data': filters,
                },
            ),
            200,
        )
    else:
        return json_dumps({'status': 404, 'data': []}), 200


@api_setting_module_blueprint.route('/proc_config/<proc_id>/visualizations', methods=['GET'])
def get_proc_visualization_config(proc_id):
    proc_with_visual_settings = get_process_visualizations(proc_id)
    if proc_with_visual_settings:
        return (
            json_dumps(
                {
                    'status': 200,
                    'data': proc_with_visual_settings,
                },
            ),
            200,
        )
    else:
        return json_dumps({'status': 404, 'data': []}), 200


@api_setting_module_blueprint.route('/proc_config/<proc_id>/traces_with/<start_proc_id>', methods=['GET'])
def get_proc_traces_with_start_proc(proc_id, start_proc_id):
    start_proc_id = int(start_proc_id) if str(start_proc_id).isnumeric() else None
    proc_id = int(proc_id) if str(proc_id).isnumeric() else None
    has_traces_with_start_proc = check_path_exist(proc_id, start_proc_id)
    if has_traces_with_start_proc:
        return jsonify(
            {
                'status': 200,
                'data': True,
            },
        )
    else:
        return jsonify({'status': 404, 'data': False})


@api_setting_module_blueprint.route('/filter_config', methods=['POST'])
def save_filter_config_configs():
    """[Summary] Save filter_config to DB
    Returns: 200/500
    """
    try:
        params = json.loads(request.data)
        filter_id = save_filter_config(params)

        proc_id = params.get('processId')
        process = get_process_cfg(proc_id)
    except Exception:
        traceback.print_exc()
        return json_dumps({}), 500

    return json_dumps({'proc': process, 'filter_id': filter_id}), 200


@api_setting_module_blueprint.route('/filter_config/<filter_id>', methods=['DELETE'])
def delete_filter_config(filter_id):
    """[Summary] delete filter_config from DB
    Returns: 200/500
    """
    try:
        delete_cfg_filter_from_db(filter_id)
    except Exception:
        traceback.print_exc()
        return json_dumps({}), 500

    return json_dumps({}), 200


@api_setting_module_blueprint.route('/distinct_sensor_values/<cfg_col_id>', methods=['GET'])
def get_sensor_distinct_values(cfg_col_id):
    # sensor_data = get_last_distinct_sensor_values(cfg_col_id)
    # tu cfg_col_id->table(m_line??m_equip??process
    data = get_filter_config_values(cfg_col_id)
    if data:
        return (
            json_dumps(
                {
                    'data': data,
                },
            ),
            200,
        )
    else:
        return json_dumps({'data': []}), 200


@api_setting_module_blueprint.route('/proc_config/<proc_id>/visualizations', methods=['POST'])
def post_master_visualizations_config(proc_id):
    try:
        save_master_vis_config(proc_id, request.json)
        proc_with_visual_settings = get_process_visualizations(proc_id)
        return (
            json_dumps(
                {
                    'status': 200,
                    'data': proc_with_visual_settings,
                },
            ),
            200,
        )
    except Exception as ex:
        traceback.print_exc()
        return (
            json_dumps(
                {
                    'status': 500,
                    'message': str(ex),
                },
            ),
            500,
        )


@api_setting_module_blueprint.route('/simulate_proc_link', methods=['POST'])
def simulate_proc_link():
    """[Summary] simulate proc link id
    Returns: 200/500
    """
    traces = json.loads(request.data)
    cfg_traces = [gen_cfg_trace(trace) for trace in traces]

    dic_proc_cnt, dic_edge_cnt = sim_gen_global_id(cfg_traces)

    # if there is no key in dic, set zero
    for cfg_trace in cfg_traces:
        self_proc_id = cfg_trace.self_process_id
        target_proc_id = cfg_trace.target_process_id
        edge_id = f'{self_proc_id}-{target_proc_id}'

        if dic_proc_cnt.get(self_proc_id) is None:
            dic_proc_cnt[self_proc_id] = 0

        if dic_proc_cnt.get(target_proc_id) is None:
            dic_proc_cnt[target_proc_id] = 0

        if dic_edge_cnt.get(edge_id) is None:
            dic_edge_cnt[edge_id] = 0

    return orjson_dumps(nodes=dic_proc_cnt, edges=dic_edge_cnt), 200


@api_setting_module_blueprint.route('/count_proc_link', methods=['POST'])
def count_proc_link():
    """[Summary] count proc link id
    Returns: 200/500
    """
    dic_proc_cnt, dic_edge_cnt = show_proc_link_info()
    return json_dumps(nodes=dic_proc_cnt, edges=dic_edge_cnt), 200


@api_setting_module_blueprint.route('/to_eng', methods=['POST'])
def to_eng():
    request_col = request.json
    col_english_name = to_romaji(request_col['colname'])
    return json_dumps({'status': 200, 'data': col_english_name}), 200


@api_setting_module_blueprint.route('/list_to_english', methods=['POST'])
def list_to_english():
    request_json = request.json
    raw_english_names = request_json.get('english_names') or []
    romaji_english_names = [to_romaji(raw_name) for raw_name in raw_english_names]

    return json_dumps({'status': 200, 'data': romaji_english_names}), 200


@api_setting_module_blueprint.route('/list_normalize_ascii', methods=['POST'])
def list_normalize_ascii():
    request_json = request.json
    raw_input_names = request_json.get('names') or []
    normalized_names = [remove_non_ascii_chars(raw_name) for raw_name in raw_input_names]

    return json_dumps({'status': 200, 'data': normalized_names}), 200


@api_setting_module_blueprint.route('/user_setting', methods=['POST'])
def save_user_setting():
    """[Summary] Save user settings to DB
    Returns: 200/500
    """
    try:
        dict_params = json.loads(request.data)
        # luu vao db cua ES
        cfg_user_settings = save_user_settings([dict_params], __FORCE_OFFLINE_MODE__=True, synced=False)
        # sync data tu db cua ES sang db cua BS va chang synced=True
        save_user_settings(cfg_user_settings, __FORCE_OFFLINE_MODE__=False, synced=True)

        # find setting id after creating a new setting
        setting = parse_user_setting(dict_params)
        if not setting.id:
            setting = CfgUserSetting.get_by_title(setting.title)[0]
        setting = CfgUserSettingSchema().dump(setting)
    except Exception as ex:
        logger.exception(ex)
        return json_dumps({'status': 'error'}), 500

    return json_dumps({'status': 200, 'data': setting}), 200


@api_setting_module_blueprint.route('/user_settings', methods=['GET'])
def get_user_settings():
    settings = get_all_user_settings()
    return json_dumps({'status': 200, 'data': settings}), 200


@api_setting_module_blueprint.route('/user_setting/<setting_id>', methods=['GET'])
def get_user_setting(setting_id):
    setting_id = parse_int_value(setting_id)
    setting = get_setting(setting_id)
    hostname = get_hostname()
    if not setting:
        return json_dumps({}), 404

    return json_dumps({'status': 200, 'data': setting, 'hostname': hostname}), 200


@api_setting_module_blueprint.route('/user_setting_page_top', methods=['GET'])
def get_user_setting_page_top():
    page = request.args.get('page')
    if not page:
        return json_dumps({}), 400

    setting = get_page_top_setting(page) or {}

    return json_dumps({'status': 200, 'data': setting}), 200


@api_setting_module_blueprint.route('/user_setting/<setting_id>', methods=['DELETE'])
def delete_user_setting(setting_id):
    """[Summary] delete user_setting from DB
    Returns: 200/500
    """
    try:
        setting_id = parse_int_value(setting_id)
        if not setting_id:
            return json_dumps({}), 400

        delete_user_setting_by_id(setting_id)

    except Exception as ex:
        logger.exception(ex)
        return json_dumps({}), 500

    return json_dumps({}), 200


@api_setting_module_blueprint.route('/get_env', methods=['GET'])
def get_current_env():
    current_env = os.environ.get(ANALYSIS_INTERFACE_ENV, AppEnv.PRODUCTION.value)
    return json_dumps({'status': 200, 'env': current_env}), 200


@api_setting_module_blueprint.route('/get_fiscal_year_default', methods=['GET'])
def get_fiscal_year():
    fy = os.environ.get('fiscal_year_start_month', FISCAL_YEAR_START_MONTH)
    return jsonify({'status': 200, 'fiscal_year_start_month': fy}), 200


@api_setting_module_blueprint.route('/load_user_setting', methods=['POST'])
def load_user_setting():
    request_data = json.loads(request.data)
    setting_id = request_data.get('setting_id')
    dic_orig_settings = request_data.get('dic_original_setting')
    active_form = request_data.get('active_form')
    shared_setting = request_data.get('shared_user_setting')
    if setting_id:
        setting_id = parse_int_value(setting_id)
        dic_setting = get_setting(setting_id)
        if not dic_setting:
            return json_dumps({}), 404

    else:
        dic_setting = {}
        dic_src_settings = {'dataForm': shared_setting}

        dic_des_setting = dic_orig_settings
        if active_form and active_form in dic_orig_settings:
            dic_des_setting = {active_form: dic_orig_settings[active_form]}

        mapping_groups = map_form(dic_src_settings, dic_des_setting)

        dic_setting['settings'] = transform_settings(mapping_groups)

    return json_dumps({'status': 200, 'data': dic_setting}), 200


@api_setting_module_blueprint.route('/check_exist_title_setting', methods=['POST'])
def check_exist_title_setting():
    """[Summary] Check input title setting is existed on DB or not
    Returns: status: 200/500 and is_exist: True/False
    """
    try:
        params = json.loads(request.data)
        is_exist = is_title_exist(params.get('title'))
    except Exception as ex:
        logger.exception(ex)
        return json_dumps({'status': 'error'}), 500

    return json_dumps({'status': 'ok', 'is_exist': is_exist}), 200


@api_setting_module_blueprint.route('/get_v2_ordered_processes', methods=['POST'])
def get_v2_auto_link_ordered_processes():
    try:
        params = json.loads(request.data)
        groups_processes = get_processes_id_order(params, method=SortMethod.FunctionCountReversedOrder)
        return jsonify({'status': 'ok', 'ordered_processes': groups_processes}), 200
    except Exception as ex:
        logger.exception(ex)
        return jsonify({'status': 'error'}), 500


@api_setting_module_blueprint.route('/get_jobs', methods=['GET'])
def get_jobs():
    offset = request.args.get('offset')
    per_page = request.args.get('limit')
    sort = request.args.get('sort')
    order = request.args.get('order')
    show_past_import_job = request.args.get('show_past_import_job')
    show_proc_link_job = request.args.get('show_proc_link_job')
    error_page = request.args.get('error_page')
    ignore_job_types = []
    if not show_proc_link_job or show_proc_link_job == 'false':
        ignore_job_types.append(JobType.GEN_GLOBAL.name)
    if not show_past_import_job or show_past_import_job == 'false':
        ignore_job_types.append(JobType.FACTORY_PAST_IMPORT.name)

    if offset and per_page:
        offset = int(offset)
        per_page = int(per_page)
        page = offset // per_page + 1
    else:
        page = 1
        per_page = 50

    dic_jobs = {}
    rows, jobs = get_background_jobs_service(page, per_page, sort, order, ignore_job_types, error_page)
    dic_jobs['rows'] = rows
    dic_jobs['total'] = jobs.total

    return jsonify(dic_jobs), 200


@api_setting_module_blueprint.route('/check_duplicated_db_source', methods=['POST'])
def check_duplicated_db_source_name():
    dbs_name = json.loads(request.data).get('name', '')
    is_duplicated = CfgDataSource.check_duplicated_name(dbs_name)
    return jsonify({'is_duplicated': is_duplicated}), 200


@api_setting_module_blueprint.route('/check_duplicated_process_name', methods=['POST'])
def check_duplicated_process_name():
    params = json.loads(request.data)
    name_en = params.get('name_en', '')
    name_jp = params.get('name_jp', '')
    name_local = params.get('name_local', '')
    is_duplicated_en, is_duplicated_jp, is_duplicated_local = CfgProcess.check_duplicated_name(
        name_en,
        name_jp,
        name_local,
    )
    return jsonify({'is_duplicated': [is_duplicated_en, is_duplicated_jp, is_duplicated_local]}), 200


@api_setting_module_blueprint.route('/register_source_and_proc', methods=['POST'])
def register_source_and_proc():
    try:
        new_process_ids = handle_importing_by_one_click(request.json)

        data_register_data = {
            'RegisterByFileRequestID': request.json.get('RegisterByFileRequestID'),
            'status': JobStatus.PROCESSING.name,
            'is_first_imported': False,
        }
        background_announcer.announce(data_register_data, AnnounceEvent.DATA_REGISTER.name)
    except Exception as e:
        logger.exception(e)
        data = {'message': _('Database Setting failed to save'), 'is_error': True, 'detail': str(e)}
        return jsonify(data), 500

    data = {'message': _('Database Setting saved.'), 'is_error': False, 'processIds': new_process_ids}
    return jsonify(data), 200


@api_setting_module_blueprint.route('/redirect_to_page', methods=['POST'])
def redirect_to_page():
    page = request.json.get('page')
    proc_ids = request.json.get('processIds')
    target_url = get_url_to_redirect(request, proc_ids, page)
    return jsonify(url=target_url), 200


@api_setting_module_blueprint.route('/check_bridge_connection_status', methods=['GET'])
def check_bridge_connection_status():
    """[Summary] check connection status to Bridge Station
    Returns: 200
    """
    status = check_connection_to_server()
    return {'status': status}, 200


@api_setting_module_blueprint.route('/data_table_config', methods=['POST'])
def post_data_table_config():
    # TODO: always set is_direct_import = True after click OK button on "Column Attribute Definition" modal
    with make_session() as meta_session:
        data_source_id = request.json.get('proc_config').get('data_source_id')
        cfg_data_source = CfgDataSource.get_by_id(data_source_id, session=meta_session)
        # cfg_data_source.is_direct_import = True
        insert_or_update_config(
            meta_session=meta_session,
            data=cfg_data_source,
            key_names=CfgDataSource.id.key,
            model=CfgDataSource,
        )

    data_table_schema = DataTableSchema()
    cfg_data_table: CfgDataTable = data_table_schema.load(request.json.get('proc_config'))
    cfg_data_table.columns = [col for col in cfg_data_table.columns if col.data_group_type]
    is_checks = request.json.get('is_checks')
    cfg_data_table = create_or_update_data_table_cfg(cfg_data_table, is_checks)

    # create process json
    dic_cfg_data_table = data_table_schema.dump(cfg_data_table) or {}
    dic_cfg_data_table['data_source_name'] = cfg_data_table.data_source.name
    get_n_save_partition_range_time_from_factory_db(cfg_data_table, is_scan=True)

    # TODO: currently , everything will be direct import.
    if cfg_data_table.data_source.csv_detail:
        # ['登録日時', '工程名', 'ライン名']
        split_cols = CfgDataTableColumn.get_split_columns(cfg_data_table.id)
        columns = CfgDataTableColumn.get_column_names_by_data_group_types(cfg_data_table.id, split_cols)
        add_scan_files_job(data_table_id=cfg_data_table.id, columns=columns, is_scan_master=True)
    else:
        add_scan_master_job(cfg_data_table.id)

    return (
        {
            'status': 200,
            'data': dic_cfg_data_table,
        },
        200,
    )


# @api_setting_module_blueprint.route('/data_table_config', methods=['POST'])
# def post_data_table_config():
#     data_table_schema = DataTableSchema()
#     cfg_data_table: CfgDataTable = data_table_schema.load(request.json.get('proc_config'))
#     is_checks = request.json.get('is_checks')
#     should_import_data = request.json.get('import_data')  # todo ?
#     cfg_data_table = create_or_update_data_table_cfg(cfg_data_table, is_checks)
#
#     # create process json
#     cfg_data_table_json = data_table_schema.dump(cfg_data_table) or {}
#     get_n_save_partition_range_time_from_factory_db(cfg_data_table, is_scan=True)
#
#     if should_import_data:
#         if cfg_data_table.data_source.csv_detail:
#             # ['登録日時', '工程名', 'ライン名']
#             split_cols = [DataGroupType.DATA_TIME, DataGroupType.PROCESS_NAME, DataGroupType.LINE_NAME]
#             columns = CfgDataTableColumn.get_column_names_by_data_group_types(cfg_data_table.id, split_cols)
#             add_scan_files_job(data_table_id=cfg_data_table.id, columns=columns, is_scan_master=True)
#             # add_zip_files_job(data_source_id=cfg_data_table.data_source_id, is_scan_master=True)
#         else:
#             add_scan_master_job(cfg_data_table.id)
#
#     return dict({
#         'status': 200,
#         'data': cfg_data_table_json,
#     }), 200


@api_setting_module_blueprint.route('/cfg_data_table/<cfg_data_table_id>', methods=['GET'])
def get_cfg_data_table(cfg_data_table_id):
    cfg_data_table = get_data_table_cfg(cfg_data_table_id)
    if cfg_data_table:
        table_name = cfg_data_table[CfgDataTable.table_name.key]
        tables = query_database_tables(cfg_data_table['data_source_id'], table_name)
        cfg_data_table['partitions'] = tables.get('partitions')
        return (
            {
                'status': 200,
                'data': cfg_data_table,
            },
            200,
        )
    else:
        return {'status': 404, 'data': 'Not found'}, 200


@api_setting_module_blueprint.route('/start_transaction_import', methods=['POST'])
def start_transaction_import():
    cfg_data_table_id = request.json.get('cfg_data_table_id')
    cfg_data_table = CfgDataTable.get_by_id(cfg_data_table_id)

    interval_sec = CfgConstant.get_value_by_type_first(CfgConstantType.POLLING_FREQUENCY.name, int)
    add_pull_data_job(cfg_data_table, interval_sec=interval_sec, run_now=True)
    return {}, 200


@api_setting_module_blueprint.route('/zip_export_database', methods=['GET'])
def zip_export_database():
    """zip export

    Returns:
        [type] -- [description]
    """
    response = export_data()
    return response


@api_setting_module_blueprint.route('/zip_import_database', methods=['POST'])
def zip_import_database():
    """zip import

    Returns:
        [type] -- [description]
    """
    # stop job
    pause_job_running()
    wait_done_jobs()

    # Backup current config & mapping data before rollback data from upload file
    export_data(is_import_db=True)
    clear_db_n_data(is_drop_t_process_tables=True)
    clear_cache()

    # ↓--- Save upload file to export_setting folder ---↓
    file = request.files['file']
    file_path = os.path.join(get_export_setting_path(), file.filename)
    dirname = get_export_setting_path()
    if not os.path.exists(dirname):
        os.makedirs(dirname)
    file.save(file_path)
    # ↑--- Save upload file to export_setting folder ---↑

    # ↓--- Rollback data from upload file ---↓
    delete_file_and_folder_by_path(get_preview_data_path())
    import_config_and_master(file_path)
    # ↑--- Rollback data from upload file ---↑

    # add idle monitoring here since it was deleted
    add_idle_monitoring_job()

    response = {'status': 200, 'page': 'config?#data_source'}
    return json_dumps(response), 200


@api_setting_module_blueprint.route('/reset_transaction_data', methods=['DELETE'])
def reset_transaction_data():
    resume_jobs_func = pause_resume_current_running_jobs()
    wait_done_jobs()
    clear_db_n_data(models=[TransactionModel, OthersDBModel])
    reset_is_show_file_name()
    clear_cache()
    logger.debug('Done "TRANSACTION_CLEAN"')
    resume_jobs_func()

    message = {'message': _('Transaction data is deleted.'), 'is_error': False}
    background_announcer.announce(message, AnnounceEvent.CLEAR_TRANSACTION_DATA.name)
    return {}, 200


@api_setting_module_blueprint.route('/delete_log_data', methods=['DELETE'])
def delete_log_data():
    log_path = get_log_path()
    delete_file_and_folder_by_path(log_path)
    return {}, 200


@api_setting_module_blueprint.route('/delete_folder_data', methods=['DELETE'])
def delete_folder_data_api():
    delete_folder_data()
    return {}, 200


@api_setting_module_blueprint.route('/insert_update_column_group', methods=['POST'])
def insert_update_column_group():
    """
    insert or update column group
    :return:
    """
    try:
        params = json.loads(request.data)
        data_ids = params.get('data_ids', [])
        group_id = params.get('group_id', None)
        data_group_id = params.get('data_group_id', None)
        mapping_category_data_dict = params.get('mapping_category_data_dict', {})

        with make_session() as meta_session:
            if not data_group_id:
                data_name_jp = params.get('data_name_jp', None)
                data_name_en = params.get('data_name_en', None)
                data_name_sys = params.get('data_name_sys', None)
                m_data_group: MDataGroup = MDataGroup()
                m_data_group.data_name_jp = data_name_jp
                m_data_group.data_name_en = data_name_en
                m_data_group.data_name_sys = data_name_sys
                m_data_group = insert_or_update_config(meta_session, m_data_group)
                meta_session.flush()
                data_group_id = m_data_group.id

            mapping_column_obj = MappingColumn(
                m_data_group_id=data_group_id,
                m_group_id=group_id,
                data_ids=data_ids,
                meta_session=meta_session,
                mapping_category_data_dict=mapping_category_data_dict,
            )
            mapping_column_obj.gen_m_group(meta_session)
            mapping_column_obj.update_cfg_process_column(meta_session)

    except Exception as ex:
        logger.exception(ex)
        return jsonify({'status': 'error'}), 500

    return jsonify({'status': 'ok'}), 200


@api_setting_module_blueprint.route('/delete_column_group', methods=['POST'])
def delete_column_group():
    """
    insert or update column group
    :return:
    """
    try:
        params = json.loads(request.data)
        group_id = params.get('group_id')
        data_group_id = params.get('data_group_id')
        if group_id:
            with make_session() as meta_session:
                mapping_column_obj = MappingColumn(
                    m_data_group_id=data_group_id,
                    m_group_id=group_id,
                    meta_session=meta_session,
                )
                mapping_column_obj.delete_column_group(meta_session)

    except Exception as ex:
        logger.exception(ex)
        return jsonify({'status': 'error'}), 500

    return jsonify({'status': 'ok'}), 200


@api_setting_module_blueprint.route('/abort_transaction_import_job/<proc_id>', methods=['POST'])
def abort_transaction_import_job(proc_id: int):
    remove_transaction_import_jobs(proc_id)
    return jsonify({STATUS: 'ok'}), 200


@api_setting_module_blueprint.route('/function_config/sample_data', methods=['POST'])
def equations_sample_data():
    equation_sample_data = EquationSampleData.model_validate_json(request.data)
    return orjson_dumps(equation_sample_data.sample_data())


@api_setting_module_blueprint.route('/function_config/get_function_infos', methods=['POST'])
def get_function_infos():
    process_id = json.loads(request.data).get('process_id', None)
    dict_sample_data = json.loads(request.data).get('dic_sample_data', {})
    cfg_process_columns: list[CfgProcessColumn] = CfgProcessColumn.get_by_process_id(process_id)
    dict_cfg_process_column = {cfg_process_column.id: cfg_process_column for cfg_process_column in cfg_process_columns}
    result = []
    for function_detail in sorted_function_details(cfg_process_columns):
        process_col = dict_cfg_process_column[function_detail.process_column_id]
        function_id = function_detail.function_id
        m_function: MFunction = MFunction.get_by_id(function_id)
        var_x = function_detail.var_x
        var_y = function_detail.var_y
        var_x_name = ''
        x_data_type = ''
        var_y_name = ''
        y_data_type = ''
        var_x_data = []
        var_y_data = []
        if var_x:
            column_x: CfgProcessColumn = dict_cfg_process_column[var_x]
            var_x_name = column_x.shown_name
            x_data_type = column_x.raw_data_type
            var_x_data = dict_sample_data[str(var_x)]

        if var_y:
            column_y: CfgProcessColumn = dict_cfg_process_column[var_y]
            var_y_name = column_y.shown_name
            y_data_type = column_y.raw_data_type
            var_y_data = dict_sample_data[str(var_y)]

        # TODO: change if Khanh san change EquationSampleData
        equation_sample_data = EquationSampleData(
            equation_id=function_id,
            X=var_x_data,
            x_data_type=x_data_type,
            Y=var_y_data,
            y_data_type=y_data_type,
            **function_detail.as_dict(),
        )
        sample_datas = equation_sample_data.sample_data().sample_data
        dict_sample_data[str(process_col.id)] = sample_datas
        function_info = {
            'functionName': m_function.function_type,
            'output': function_detail.return_type,
            'systemName': process_col.name_en,
            'japaneseName': process_col.name_jp,
            'localName': process_col.name_local,
            'varXName': var_x_name,
            'varYName': var_y_name,
            'a': function_detail.a,
            'b': function_detail.b,
            'c': function_detail.c,
            'n': function_detail.n,
            'k': function_detail.k,
            's': function_detail.s,
            't': function_detail.t,
            'note': function_detail.note,
            'sampleDatas': sample_datas,
            'isChecked': False,
            'processColumnId': process_col.id,
            'functionColumnId': function_detail.id,
            'functionId': function_id,
            'varX': var_x,
            'varY': var_y,
            'index': function_detail.order,
        }
        result.append(function_info)

    result = sorted(result, key=lambda k: k['index'])
    return orjson_dumps({'functionData': result})


@api_setting_module_blueprint.route('/function_register', methods=['POST'])
def function_register():
    functions = json.loads(request.data).get('functions', None)
    with make_session() as meta_session:
        gen_function_column(functions, session=meta_session)

    return json_dumps({'status': 200}), 200


@api_setting_module_blueprint.route('/function_config/delete_function_columns', methods=['POST'])
def delete_function_column_config():
    """[Summary] delete function column from DB
    Returns: 200/500
    """
    data = json.loads(request.data)
    column_ids = data.get('column_ids', [])
    function_column_ids = data.get('function_column_ids', [])
    try:
        with make_session() as session:
            CfgProcessColumn.delete_by_ids(column_ids, session=session)
            MData.delete_by_ids(column_ids, session)
            CfgProcessFunctionColumn.delete_by_ids(function_column_ids, session=session)
    except Exception:
        traceback.print_exc()
        return json_dumps({}), 500

    return json_dumps({}), 200


@api_setting_module_blueprint.route('/backup_data', methods=['POST'])
def backup_data():
    """[Summary] backup data from DB
    Returns: 200/500
    """
    data = json.loads(request.data)
    process_id = data.get('process_id')
    start_time = data.get('start_time')
    end_time = data.get('end_time')
    if process_id:
        target_jobs = [JobType.TRANSACTION_IMPORT, JobType.TRANSACTION_PAST_IMPORT]
        remove_jobs(target_jobs, proc_id=process_id)
    add_backup_data_job(process_id, start_time, end_time)
    return json_dumps({}), 200


@api_setting_module_blueprint.route('/restore_data', methods=['POST'])
def restore_data():
    """[Summary] restore data from file
    Returns: 200/500
    """
    data = json.loads(request.data)
    process_id = data.get('process_id')
    start_time = data.get('start_time')
    end_time = data.get('end_time')
    if process_id:
        target_jobs = [JobType.TRANSACTION_IMPORT, JobType.TRANSACTION_PAST_IMPORT]
        remove_jobs(target_jobs, proc_id=process_id)
    add_restore_data_job(process_id, start_time, end_time)
    return json_dumps({}), 200


@api_setting_module_blueprint.route('/clear_cache', methods=['POST'])
def clear_cache_api():
    """[Summary] delete cache in backend, only used for test"""
    clear_cache()
    return json_dumps({}), 200
