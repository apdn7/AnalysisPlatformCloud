# import tkinter as tk
# from tkinter import filedialog
import datetime

from ap.api.setting_module.services.data_import import add_transaction_import_job
from ap.api.setting_module.services.direct_import import gen_config_data_db_instance
from ap.api.setting_module.services.polling_frequency import add_pull_data_job
from ap.api.setting_module.services.show_latest_record import (
    gen_cols_with_types,
    get_process_config_info,
    is_valid_list,
    preview_csv_data,
    transform_df_to_rows,
)
from ap.common.common_utils import (
    API_DATETIME_FORMAT,
    add_months,
    convert_time,
    delete_preview_data_file_folder,
    get_month_diff,
    is_empty,
)
from ap.common.constants import (
    EMPTY_STRING,
    AnnounceEvent,
    CacheType,
    CfgConstantType,
    DataGroupType,
    DataType,
    DBType,
    DirectoryNo,
    JobType,
    MasterDBType,
    PagePath,
    RawDataTypeDB,
)
from ap.common.memoize import set_all_cache_expired
from ap.common.pydn.dblib.postgresql import PostgreSQL
from ap.common.services.jp_to_romaji_utils import change_duplicated_columns
from ap.common.services.normalization import normalize_list, normalize_str
from ap.common.services.sse import background_announcer
from ap.setting_module.models import (
    CfgConstant,
    CfgDataSource,
    CfgProcess,
)
from ap.setting_module.schemas import DataSourceSchema, ProcessSchema
from ap.setting_module.services.background_process import JobInfo
from ap.setting_module.services.process_config import query_database_tables_db_instance
from bridge.models.bridge_station import BridgeStationModel
from bridge.models.cfg_csv_column import CfgCsvColumn as BSCfgCsvColumn
from bridge.models.cfg_data_source import CfgDataSource as BSCfgDataSource
from bridge.models.cfg_data_source_csv import CfgDataSourceCSV as BSCfgDataSourceCSV
from bridge.models.cfg_data_table import CfgDataTable as BSCfgDataTable
from bridge.models.cfg_data_table_column import CfgDataTableColumn as BSCfgDataTableColumn
from bridge.models.cfg_process import CfgProcess as BSCfgProcess
from bridge.models.cfg_process_column import CfgProcessColumn as BSCfgProcessColumn
from bridge.models.m_data import MData
from bridge.models.m_unit import MUnit
from bridge.models.mapping_factory_machine import MappingFactoryMachine
from bridge.models.transaction_model import TransactionData
from bridge.services.csv_management import save_scan_master_target_files, scan_files
from bridge.services.etl_services.etl_controller import ETLController
from bridge.services.etl_services.etl_db_service import (
    get_n_save_partition_range_time_from_factory_db_db_instance,
)
from bridge.services.etl_services.etl_import import pull_csv
from bridge.services.master_data_import import scan_master
from bridge.services.scan_data_type import scan_data_type
from bridge.services.utils import get_specific_v2_type_based_on_column_names

# def browse(resource_type):
#     window = tk.Tk()
#     window.wm_attributes('-topmost', 1)
#     window.withdraw()  # this supress the tk window
#
#     dialog = filedialog.askdirectory
#     if resource_type != RegisterDatasourceType.DIRECTORY.value:
#         dialog = filedialog.askopenfilename
#     f_path = dialog(parent=window)
#     return f_path, resource_type


def get_url_to_redirect(request, proc_ids, page):
    col_ids = []
    for proc_id in proc_ids:
        proc_cfg = CfgProcess.get_proc_by_id(proc_id)
        col_ids.extend([str(col.id) for col in proc_cfg.columns if col.is_serial_no or col.is_get_date])
    target_col_ids = ','.join(col_ids)

    # get start_datetime and end_datetime
    trans_data = TransactionData(proc_ids[0])
    max_datetime = trans_data.get_max_date_time_by_process_id()
    min_datetime = trans_data.get_min_date_time_by_process_id()

    host_url = request.host_url
    month_diff = get_month_diff(min_datetime, max_datetime)
    if page in PagePath.FPP.value and month_diff > 1:
        min_datetime = add_months(max_datetime, -1)
    min_datetime = convert_time(min_datetime, format_str=API_DATETIME_FORMAT)
    max_datetime = convert_time(max_datetime, format_str=API_DATETIME_FORMAT)

    end_procs = ','.join([str(_id) for _id in proc_ids])

    # get target page from bookmark
    target_url = f'{host_url}{page}?columns={target_col_ids}&start_datetime={min_datetime}&end_datetime={max_datetime}&end_procs=[{end_procs}]&load_gui_from_url=1&page={page.split("/")[1]}'  # noqa
    return target_url


def get_latest_records_core(dic_preview: dict, limit: int = 5):
    cols_with_types = []
    headers = normalize_list(dic_preview.get('header'))
    headers = [normalize_str(col) for col in headers]
    data_types = dic_preview.get('dataType')
    same_values = dic_preview.get('same_values')
    is_v2_history = dic_preview.get('v2_type') == DBType.V2_HISTORY
    if headers and data_types:
        column_raw_name = dic_preview.get('org_headers')
        cols_with_types = gen_cols_with_types(headers, data_types, same_values, is_v2_history, column_raw_name)
    cols = headers

    # get rows
    df_rows = dic_preview.get('content', None)
    previewed_files = dic_preview.get('previewed_files')

    # change name if romaji cols is duplicated
    cols_with_types, cols_duplicated = change_duplicated_columns(cols_with_types)
    has_ct_col = True
    dummy_datetime_idx = None

    rows = []
    if is_valid_list(df_rows):
        data_type_by_cols = {}
        for col_data in cols_with_types:
            data_type_by_cols[col_data['column_name']] = col_data['data_type']
        # convert to correct dtypes
        for col in df_rows.columns:
            try:
                if data_type_by_cols[col] == DataType.INTEGER.name:
                    df_rows[col] = df_rows[col].astype('float64').astype('Int64')

                if data_type_by_cols[col] == DataType.TEXT.name:
                    # fill na to '' for string column
                    df_rows[col] = df_rows[col].astype('string').fillna('')
            except Exception:
                continue
        rows = transform_df_to_rows(cols, df_rows, limit)

    # Set raw data type base on data_type
    for col_data in cols_with_types:
        if col_data['data_type'] == DataType.DATETIME.name:
            col_data['raw_data_type'] = RawDataTypeDB.DATETIME.value
        if col_data['data_type'] == DataType.DATE.name:
            col_data['raw_data_type'] = RawDataTypeDB.DATE.value
        if col_data['data_type'] == DataType.TIME.name:
            col_data['raw_data_type'] = RawDataTypeDB.TIME.value
        if col_data['data_type'] == DataType.BIG_INT.name:
            col_data['raw_data_type'] = RawDataTypeDB.BIG_INT.value
        elif col_data['data_type'] == DataType.INTEGER.name:
            if col_data['is_big_int']:
                col_data['raw_data_type'] = RawDataTypeDB.BIG_INT.value
            else:
                col_data['raw_data_type'] = RawDataTypeDB.INTEGER.value
        elif col_data['data_type'] == DataType.REAL.name:
            col_data['raw_data_type'] = RawDataTypeDB.REAL.value
        elif col_data['data_type'] == DataType.BOOLEAN.name:
            col_data['raw_data_type'] = RawDataTypeDB.BOOLEAN.value
        elif col_data['data_type'] == DataType.TEXT.name:
            col_data['raw_data_type'] = RawDataTypeDB.TEXT.value

    is_rdb = False
    return cols_with_types, rows, cols_duplicated, previewed_files, has_ct_col, dummy_datetime_idx, is_rdb


def proc_config_infos(dic_preview: dict, master_type) -> dict:
    [cols_with_types, _, _, _, _, _, _] = get_latest_records_core(dic_preview, 0)
    is_file_path = dic_preview['is_file_path']
    is_others = master_type is None
    data_src_dict = {
        'name': datetime.datetime.utcnow().timestamp().__str__(),
        'type': DBType.CSV.name,
        'master_type': MasterDBType.OTHERS.name if is_others else MasterDBType.V2.name,
        'csv_detail': {
            'directory': dic_preview['file_name'] if is_file_path else dic_preview.get('directory'),
            'delimiter': 'Auto',
            'csv_columns': cols_with_types,
            'is_file_path': is_file_path,
        },
    }
    proc_config_infos = []

    with BridgeStationModel.get_db_proxy() as db_instance:  # type: PostgreSQL
        # Generate data source
        data_src: CfgDataSource = DataSourceSchema().load(data_src_dict)
        data_source_id = generate_data_source(db_instance, data_src)
        generate_data_source_csv(db_instance, data_source_id, data_src)
        generate_csv_columns(db_instance, data_source_id, data_src)

        # Generate data table
        dict_tables = query_database_tables_db_instance(data_source_id, db_instance=db_instance)
        detail_master_types = dict_tables.get('detail_master_types') if not is_others else None
        datetime_col = None
        for col in cols_with_types:
            if col['is_get_date']:
                datetime_col = col['column_raw_name']

        cfg_data_source, cfg_data_tables = generate_data_table(
            db_instance,
            data_source_id,
            datetime_col=datetime_col,
            detail_master_types=detail_master_types,
        )

        temp_process_ids = []
        for cfg_data_table in cfg_data_tables:
            # Do scan file
            generate_csv_management(db_instance, cfg_data_table.id)

            # Do scan master
            generate_master_data(db_instance, cfg_data_table.id)

            # Do scan data type and gen process config
            generate_data_type(db_instance, cfg_data_table.id)

            # Do get process config info and data sample
            process_id_with_data_table_ids = MappingFactoryMachine.get_process_id_with_data_table_id(
                db_instance,
                [cfg_data_table.id],
            )
            process_ids = {x.get('process_id') for x in process_id_with_data_table_ids}
            temp_process_ids.extend(process_ids)
            for process_id in process_ids:
                proc_info_dict = get_process_config_info(process_id, db_instance=db_instance)
                proc_info_dict['data']['origin_name'] = proc_info_dict['data']['name_en']
                proc_config_infos.append(proc_info_dict)

        # Do remove pickle sample file
        for process_id in temp_process_ids:
            delete_preview_data_file_folder(process_id)

        db_instance.connection.rollback()

    data_src_dict['detail_master_type'] = master_type
    return {
        'processConfigs': proc_config_infos,
        'datasourceConfig': data_src_dict,
    }


def get_latest_records_for_register_by_file(file_name: str = None, directory: str = None, limit: int = 5):
    delimiter = 'Auto'
    skip_head = ''
    etl_func = ''

    dic_preview = preview_csv_data(
        directory,
        etl_func,
        delimiter,
        limit,
        return_df=True,
        max_records=1000,
        file_name=file_name,
        line_skip=skip_head,
    )

    dic_preview['is_file_path'] = file_name is not None
    column_raw_name = dic_preview.get('org_headers')
    master_type = get_specific_v2_type_based_on_column_names(column_raw_name)
    data_src_dict = proc_config_infos(dic_preview, master_type)
    return data_src_dict


def generate_data_source(db_instance: PostgreSQL, data_src):
    create_at = datetime.datetime.utcnow()
    # Insert data source
    data_source_id = BSCfgDataSource.insert_record(
        db_instance,
        {
            BSCfgDataSource.Columns.name.name: data_src.name,
            BSCfgDataSource.Columns.type.name: data_src.type or DBType.CSV.name,
            BSCfgDataSource.Columns.comment.name: data_src.comment,
            BSCfgDataSource.Columns.order.name: data_src.order,
            BSCfgDataSource.Columns.master_type.name: data_src.master_type or MasterDBType.OTHERS.name,
            BSCfgDataSource.Columns.is_direct_import.name: True,  # force always True to avoid file_mode in scan master
            BSCfgDataSource.Columns.created_at.name: create_at,
            BSCfgDataSource.Columns.updated_at.name: create_at,
        },
        is_return_id=True,
        is_normalize=False,
    )

    return data_source_id


def generate_data_source_csv(db_instance: PostgreSQL, data_source_id: int, data_src):
    create_at = datetime.datetime.utcnow()
    # Insert data source csv
    BSCfgDataSourceCSV.insert_record(
        db_instance,
        {
            BSCfgDataSourceCSV.Columns.id.name: data_source_id,
            BSCfgDataSourceCSV.Columns.directory.name: data_src.csv_detail.directory,
            BSCfgDataSourceCSV.Columns.second_directory.name: data_src.csv_detail.second_directory,
            BSCfgDataSourceCSV.Columns.skip_head.name: data_src.csv_detail.skip_head,
            BSCfgDataSourceCSV.Columns.skip_tail.name: data_src.csv_detail.skip_tail,
            BSCfgDataSourceCSV.Columns.delimiter.name: data_src.csv_detail.delimiter,
            BSCfgDataSourceCSV.Columns.etl_func.name: data_src.csv_detail.etl_func,
            BSCfgDataSourceCSV.Columns.dummy_header.name: data_src.csv_detail.dummy_header or False,
            BSCfgDataSourceCSV.Columns.is_file_path.name: data_src.csv_detail.is_file_path or False,
            BSCfgDataSourceCSV.Columns.created_at.name: create_at,
            BSCfgDataSourceCSV.Columns.updated_at.name: create_at,
        },
        is_return_id=True,
        is_normalize=False,
    )


def generate_csv_columns(db_instance: PostgreSQL, data_source_id: int, data_src):
    create_at = datetime.datetime.utcnow()
    # Insert csv columns
    for csv_column in data_src.csv_detail.csv_columns:
        BSCfgCsvColumn.insert_record(
            db_instance,
            {
                BSCfgCsvColumn.Columns.data_source_id.name: data_source_id,
                BSCfgCsvColumn.Columns.column_name.name: csv_column.column_name,
                # BSCfgCsvColumn.Columns.data_type.name: csv_column.data_type,  # Always <null>
                BSCfgCsvColumn.Columns.order.name: csv_column.order or 0,
                BSCfgCsvColumn.Columns.directory_no.name: csv_column.directory_no or DirectoryNo.ROOT_DIRECTORY.value,
                BSCfgCsvColumn.Columns.created_at.name: create_at,
                BSCfgCsvColumn.Columns.updated_at.name: create_at,
            },
            is_return_id=True,
            is_normalize=False,
        )


def generate_data_table(
    db_instance: PostgreSQL,
    data_source_id: int,
    proc_data: ProcessSchema = None,
    detail_master_types=None,
    datetime_col=None,
    serial_col=None,
) -> tuple[BSCfgDataSource, list[BSCfgDataTable]]:
    # insert cfg process & cfg process column
    for col in (proc_data or {'columns': []}).get('columns'):
        if col.is_get_date:
            datetime_col = col.column_raw_name
        elif col.is_serial_no:
            serial_col = col.column_raw_name

    # Gen data table
    cfg_data_source = BSCfgDataSource(
        BSCfgDataSource.get_by_id(db_instance, data_source_id),
        db_instance=db_instance,
        is_cascade=True,
    )
    cfg_data_source, cfg_data_tables = gen_config_data_db_instance(
        cfg_data_source,
        serial_col,
        datetime_col,
        None,
        None,
        None,
        detail_master_types,
        db_instance=db_instance,
        skip_merge=True,
    )  # type: BSCfgDataSource, list[BSCfgDataTable]

    for cfg_data_table in cfg_data_tables:
        get_n_save_partition_range_time_from_factory_db_db_instance(
            cfg_data_table,
            is_scan=True,
            db_instance=db_instance,
        )

    return cfg_data_source, cfg_data_tables


def generate_csv_management(db_instance: PostgreSQL, data_table_id: int):
    split_cols = BSCfgDataTableColumn.get_split_columns(db_instance, data_table_id)
    columns = BSCfgDataTableColumn.get_column_names_by_data_group_types(db_instance, data_table_id, split_cols)

    # Do scan file
    scan_files_generator = scan_files(data_table_id, columns, db_instance=db_instance)
    list(scan_files_generator)


def generate_master_data(db_instance: PostgreSQL, data_table_id: int):
    save_scan_master_target_files(db_instance, data_table_id)
    scan_master_generator = scan_master(data_table_id, db_instance=db_instance)
    list(scan_master_generator)


def generate_data_type(db_instance: PostgreSQL, data_table_id: int):
    scan_data_type_generator = scan_data_type(data_table_id, db_instance=db_instance)
    list(scan_data_type_generator)


def get_all_process_ids(db_instance: PostgreSQL, data_table_id: int) -> set[int]:
    process_id_rows = MappingFactoryMachine.get_process_id_with_data_table_id(db_instance, [data_table_id])
    return {row.get(MData.Columns.process_id.name) for row in process_id_rows}


def update_process_infos(
    db_instance: PostgreSQL,
    process_ids: set[int],
    proc_configs: list[dict],
    is_master_type_other: bool,
):
    for process_id in process_ids:
        proc = BSCfgProcess.get_by_process_id(db_instance, process_id, is_cascade_column=True)
        for request_proc_config in proc_configs:
            proc_config = request_proc_config['proc_config']
            if (
                is_master_type_other
                or proc_config.get('origin_name', '') == proc.name_en
                or proc_config.get('name', '') == proc.name
            ):
                del proc_config['origin_name']
                proc_config = ProcessSchema().load(proc_config)
                unused_columns = (request_proc_config.get('unused_columns') or {}).get('columns', [])
                unused_column_raw_names = [unused_column.get('column_raw_name') for unused_column in unused_columns]
                update_process_info(
                    db_instance,
                    process_id,
                    proc,
                    proc_config,
                    unused_columns=unused_column_raw_names,
                )
                break


def update_process_info(
    db_instance: PostgreSQL,
    process_id: int,
    existing_process,
    request_process,
    unused_columns: list[str] = None,
):
    transaction_data_obj = TransactionData(process_id, db_instance=db_instance)
    transaction_data_obj.cast_data_type_for_columns(db_instance, existing_process, request_process)
    existing_process_columns = existing_process.columns

    # Update process names & flag is_show_file_name
    BSCfgProcess.update_by_conditions(
        db_instance,
        {
            BSCfgProcess.Columns.name.name: request_process.get('name'),
            BSCfgProcess.Columns.name_jp.name: request_process.get('name_jp'),
            BSCfgProcess.Columns.name_local.name: request_process.get('name_local'),
            BSCfgProcess.Columns.name_en.name: request_process.get('name_en'),
            BSCfgProcess.Columns.is_show_file_name.name: DataGroupType.FileName.name not in unused_columns,
        },
        dic_conditions={BSCfgProcess.Columns.id.name: process_id},
    )

    # Delete column if it was uncheck
    for delete_column_name in unused_columns:
        # Not delete file name
        if delete_column_name == DataGroupType.FileName.name:
            continue

        target_column = next(
            filter(
                lambda column: column.column_raw_name == delete_column_name,
                existing_process_columns,
            ),
            None,
        )

        if target_column is None:
            # In case update an exist process, target column was already removed before -> do nothing.
            continue

        dic_conditions = {BSCfgProcessColumn.Columns.id.name: target_column.id}
        BSCfgProcessColumn.delete_by_condition(db_instance, dic_conditions, mode=0)
        MData.update_by_conditions(db_instance, {MData.Columns.is_hide.name: True}, dic_conditions=dic_conditions)

    # Gen main::Datetime from main::Date & main::Time
    datetime_cols = list(
        filter(
            lambda x: x.is_get_date or x.column_type in (DataGroupType.MAIN_DATE.value, DataGroupType.MAIN_TIME.value),
            request_process['columns'],
        ),
    )
    if len(datetime_cols) == 3:
        for request_process_column in datetime_cols:
            if not request_process_column.is_get_date:
                continue

            from bridge.services.master_data_import import gen_m_data_manual

            unit_id = MUnit.get_empty_unit_id(db_instance)
            new_ids = gen_m_data_manual(
                RawDataTypeDB.DATETIME.value,
                unit_id,
                request_process_column.name_en,
                request_process_column.name_jp,
                request_process_column.name_local,
                process_id,
                request_process_column.column_type,
                db_instance=db_instance,
            )
            request_process_column.id = new_ids['data_id']
            request_process_column.bridge_column_name = (
                f'_{request_process_column.id}_{request_process_column.column_raw_name}'
            )

            dic_update_values = {
                BSCfgProcessColumn.Columns.id.name: request_process_column.id,
                BSCfgProcessColumn.Columns.process_id.name: process_id,
                BSCfgProcessColumn.Columns.column_name.name: request_process_column.column_name,
                BSCfgProcessColumn.Columns.format.name: request_process_column.format,
                BSCfgProcessColumn.Columns.name_en.name: request_process_column.name_en,
                BSCfgProcessColumn.Columns.name_jp.name: request_process_column.name_jp,
                BSCfgProcessColumn.Columns.name_local.name: request_process_column.name_local,
                BSCfgProcessColumn.Columns.bridge_column_name.name: request_process_column.bridge_column_name,
                BSCfgProcessColumn.Columns.column_raw_name.name: request_process_column.column_raw_name,
                BSCfgProcessColumn.Columns.data_type.name: request_process_column.data_type,
                BSCfgProcessColumn.Columns.raw_data_type.name: request_process_column.raw_data_type,
                BSCfgProcessColumn.Columns.operator.name: request_process_column.operator,
                BSCfgProcessColumn.Columns.coef.name: request_process_column.coef,
                BSCfgProcessColumn.Columns.column_type.name: request_process_column.column_type,
                BSCfgProcessColumn.Columns.is_serial_no.name: request_process_column.is_serial_no,
                BSCfgProcessColumn.Columns.is_get_date.name: request_process_column.is_get_date,
                BSCfgProcessColumn.Columns.is_dummy_datetime.name: request_process_column.is_dummy_datetime,
                BSCfgProcessColumn.Columns.is_auto_increment.name: request_process_column.is_auto_increment,
                BSCfgProcessColumn.Columns.order.name: request_process_column.order,
                BSCfgProcessColumn.Columns.created_at.name: datetime.datetime.utcnow(),
                BSCfgProcessColumn.Columns.updated_at.name: datetime.datetime.utcnow(),
            }
            BSCfgProcessColumn.insert_record(
                db_instance,
                dic_update_values,
            )
            existing_process.columns.append(BSCfgProcessColumn(dic_update_values, db_instance=db_instance))

    # Update process columns
    for request_process_column in request_process['columns']:
        existing_process_column = next(
            filter(
                lambda column: column.column_raw_name == request_process_column.column_raw_name,
                existing_process_columns,
            ),
            None,
        )

        if existing_process_column is None:
            raise Exception('Missing column -> It maybe be bug relate to database session!')

        dic_update_values = {}
        # Update name english
        if existing_process_column.name_en != request_process_column.name_en:
            dic_update_values[BSCfgProcessColumn.Columns.name_en.name] = (
                request_process_column.name_en if not is_empty(request_process_column.name_en) else EMPTY_STRING
            )

        # Update name japanese
        if existing_process_column.name_jp != request_process_column.name_jp:
            dic_update_values[BSCfgProcessColumn.Columns.name_jp.name] = (
                request_process_column.name_jp if not is_empty(request_process_column.name_jp) else EMPTY_STRING
            )

        # Update name local
        if existing_process_column.name_local != request_process_column.name_local:
            dic_update_values[BSCfgProcessColumn.Columns.name_local.name] = (
                request_process_column.name_local if not is_empty(request_process_column.name_local) else EMPTY_STRING
            )

        # Update format
        if existing_process_column.format != request_process_column.format:
            dic_update_values[BSCfgProcessColumn.Columns.format.name] = (
                request_process_column.format if not is_empty(request_process_column.format) else None
            )

        # Update raw data type and data type
        if existing_process_column.raw_data_type != request_process_column.raw_data_type:
            dic_update_values[BSCfgProcessColumn.Columns.raw_data_type.name] = request_process_column.raw_data_type
            dic_update_values[
                BSCfgProcessColumn.Columns.data_type.name
            ] = RawDataTypeDB.convert_raw_data_type_to_data_type(request_process_column.raw_data_type)

        # Update column type
        if existing_process_column.column_type != request_process_column.column_type:
            dic_update_values[BSCfgProcessColumn.Columns.column_type.name] = request_process_column.column_type

        if dic_update_values:
            BSCfgProcessColumn.update_by_conditions(
                db_instance,
                dic_update_values,
                dic_conditions={BSCfgProcessColumn.Columns.id.name: existing_process_column.id},
            )


def pull_csv_data(db_instance, cfg_data_table):
    job_info = JobInfo()
    job_type = JobType.PULL_CSV_DATA
    job_info.job_type = job_type
    etl_service = ETLController.get_etl_service(cfg_data_table, db_instance=db_instance)
    pull_csv_generator = pull_csv(
        JobType.PULL_CSV_DATA,
        etl_service,
        job_info,
        ignore_add_job=True,
        db_instance=db_instance,
    )
    list(pull_csv_generator)


def handle_importing_by_one_click(request):
    register_by_file_request_id = request.get('request_id')
    request_datasource_dict = request.get('csv_info')

    # Detect truly detail master type (There are only 2 types: V2 or V2_HISTORY)
    detail_master_type = request_datasource_dict.get('detail_master_type')
    if 'detail_master_type' in request_datasource_dict:
        if detail_master_type in [MasterDBType.V2_HISTORY.name, MasterDBType.V2_MULTI_HISTORY.name]:
            request_datasource_dict['csv_detail']['second_directory'] = request_datasource_dict['csv_detail'][
                'directory'
            ]
            request_datasource_dict['csv_detail']['directory'] = None
            detail_master_type = MasterDBType.V2_HISTORY.name
        elif detail_master_type in [MasterDBType.V2.name, MasterDBType.V2_MULTI.name]:
            detail_master_type = MasterDBType.V2.name
        else:
            detail_master_type = MasterDBType.OTHERS.name
        del request_datasource_dict['detail_master_type']

    announce_data = {'RegisterByFileRequestID': register_by_file_request_id}
    new_process_ids = []
    with BridgeStationModel.get_db_proxy() as db_instance:
        # Do generate data source
        data_src: CfgDataSource = DataSourceSchema().load(request_datasource_dict)
        data_source_id = generate_data_source(db_instance, data_src)
        generate_data_source_csv(db_instance, data_source_id, data_src)
        generate_csv_columns(db_instance, data_source_id, data_src)

        # Do generate data table
        detail_master_types = []
        if request_datasource_dict.get('master_type') != MasterDBType.V2.name:
            proc_config = request.get('proc_configs')[0].get('proc_config')
            backup_origin_name = proc_config['origin_name']
            del proc_config['origin_name']
            proc_data = ProcessSchema().load(proc_config)
            proc_config['origin_name'] = backup_origin_name
        else:
            proc_data = None
            detail_master_types.append(detail_master_type)
        announce_data['step'] = 'GEN_DATA_TABLE'
        background_announcer.announce(announce_data, AnnounceEvent.DATA_REGISTER.name)
        cfg_data_source, cfg_data_tables = generate_data_table(
            db_instance,
            data_source_id,
            proc_data=proc_data,
            detail_master_types=detail_master_types,
        )

        for cfg_data_table in cfg_data_tables:
            # Do scan file
            announce_data['step'] = JobType.SCAN_FILE.name
            background_announcer.announce(announce_data, AnnounceEvent.DATA_REGISTER.name)
            generate_csv_management(db_instance, cfg_data_table.id)

            # Do scan master
            announce_data['step'] = JobType.SCAN_MASTER.name
            background_announcer.announce(announce_data, AnnounceEvent.DATA_REGISTER.name)
            generate_master_data(db_instance, cfg_data_table.id)

            # Do scan data type and gen process config
            announce_data['step'] = JobType.SCAN_DATA_TYPE.name
            background_announcer.announce(announce_data, AnnounceEvent.DATA_REGISTER.name)
            generate_data_type(db_instance, cfg_data_table.id)

            # Do update data type for process columns
            process_ids = get_all_process_ids(db_instance, cfg_data_table.id)
            new_process_ids.extend(process_ids)

            update_process_infos(
                db_instance,
                process_ids,
                request.get('proc_configs'),
                detail_master_type == MasterDBType.OTHERS.name,
            )

            # Do pull csv files
            announce_data['step'] = JobType.PULL_CSV_DATA.name
            background_announcer.announce(announce_data, AnnounceEvent.DATA_REGISTER.name)
            pull_csv_data(db_instance, cfg_data_table)

    set_all_cache_expired(CacheType.CONFIG_DATA)

    interval_sec = CfgConstant.get_value_by_type_first(CfgConstantType.POLLING_FREQUENCY.name, int)

    # Add job to import transaction data
    for process_id in new_process_ids:
        add_transaction_import_job(
            process_id,
            interval_sec=interval_sec,
            run_now=True,
            register_by_file_request_id=register_by_file_request_id,
        )

    # Add job to pull transaction data
    for cfg_data_table in cfg_data_tables:
        add_pull_data_job(cfg_data_table, interval_sec=interval_sec, run_now=True)

    return new_process_ids
