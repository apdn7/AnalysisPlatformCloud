import io
import os
import pickle
import shutil
from pathlib import Path
from time import sleep
from typing import Optional, Union
from zipfile import ZipFile

import pandas as pd
from pandas import DataFrame
from sqlalchemy.orm import scoped_session

from ap.common.common_utils import (
    get_data_path,
    get_files,
    get_nayose_path,
    get_preview_data_path,
)
from ap.common.constants import ID, PROCESS_QUEUE_FILE_NAME, AnnounceEvent, CfgConstantType, FileExtension, JobType
from ap.common.pydn.dblib.db_common import db_instance_exec
from ap.common.pydn.dblib.postgresql import PostgreSQL
from ap.common.scheduler import get_running_jobs, is_job_running, scheduler
from ap.common.services.import_export_config_n_data import (
    download_zip_file,
    get_table_name_in_file,
    zip_a_file,
)
from ap.common.services.sse import background_announcer
from ap.setting_module.models import (
    ApschedulerJobs,
    AutoLink,
    CfgConstant,
    ConfigDBModel,
    CsvManagement,
    MappingDBModel,
    MasterDBModel,
    SemiMasterDBModel,
    get_models,
    make_session,
)
from ap.setting_module.services.background_process import JobInfo
from bridge.common.bridge_station_config_utils import PostgresSequence
from bridge.models.bridge_station import BridgeStationModel, MasterModel
from bridge.models.cfg_constant import CfgConstantModel
from bridge.models.cfg_data_table import CfgDataTable
from bridge.models.cfg_process import CfgProcess

dict_table_with_latest_id = {}

dict_table_with_index = {
    'apscheduler_jobs': '00',
    'cfg_constant': '01',
    'cfg_data_source': '02',
    'cfg_data_source_csv': '03',
    'cfg_csv_column': '04',
    'cfg_data_source_db': '05',
    'cfg_data_table': '06',
    'cfg_data_table_column': '07',
    'cfg_process': '08',
    'cfg_process_column': '09',
    'cfg_filter': '10',
    'cfg_trace': '11',
    'cfg_trace_key': '12',
    'cfg_filter_detail': '13',
    'cfg_user_setting': '14',
    'cfg_visualization': '15',
    'm_function': '16',
    'm_data_group': '17',
    'm_process': '18',
    'm_unit': '19',
    'm_data': '20',
    'm_line_group': '21',
    'm_line': '22',
    'm_part': '23',
    'm_plant': '24',
    'r_prod_part': '25',
    'm_equip_group': '26',
    'm_equip': '27',
    'm_config_equation': '28',
    'm_dept': '29',
    'm_factory': '30',
    'm_location': '31',
    'm_part_type': '32',
    'm_prod': '33',
    'm_prod_family': '33',
    'm_sect': '35',
    'm_st': '36',
    'r_factory_machine': '37',
    'mapping_factory_machine': '38',
    'mapping_part': '39',
    'mapping_process_data': '40',
    'cfg_partition_table': '42',
    'm_group': '43',
    'm_column_group': '44',
    'mapping_category_data': '45',
    'semi_master': '46',
    'alembic_version': '47',
    't_csv_management': '48',
    't_auto_link': '49',
    'cfg_process_function_column': '50',
}
PREVIEW_DATA_FILE_NAME = 'data_preview.zip'
NAYOSE_DATA_FILE_NAME = 'data_nayose.zip'


def zip_preview_folder_to_byte() -> io.BytesIO:
    """
    returns: zip archive
    """
    preview_data_path = get_preview_data_path()
    if not os.path.exists(preview_data_path):
        return None

    archive = io.BytesIO()
    with ZipFile(archive, 'w') as zip_archive:
        for file_path in get_files(preview_data_path, depth_from=1, depth_to=100, extension=['csv', 'tsv', 'zip']):
            file_name = Path(file_path).as_posix().replace(f'{Path(preview_data_path).as_posix()}/', '')
            with zip_archive.open(file_name, 'w') as file, open(file_path, 'rb') as f:
                file.write(f.read())

    return archive


def zip_nayose_folder_to_byte() -> io.BytesIO:
    """
    returns: zip archive
    """
    nayose_path = get_nayose_path()
    if not os.path.exists(nayose_path):
        return None

    archive = io.BytesIO()
    with ZipFile(archive, 'w') as zip_archive:
        for file_path in get_files(nayose_path, depth_from=1, depth_to=100, extension=['ftr']):
            file_name = Path(file_path).as_posix().replace(f'{Path(nayose_path).as_posix()}/', '')
            with zip_archive.open(file_name, 'w') as file, open(file_path, 'rb') as f:
                file.write(f.read())

    return archive


def export_data(is_import_db=False):
    export_tables = get_models([ConfigDBModel, MasterDBModel, MappingDBModel, SemiMasterDBModel])
    export_tables = (*export_tables, CsvManagement, AutoLink)
    other_tables = [CfgConstant.__tablename__, 'alembic_version']
    table_data_s = []
    file_names = []
    for model in export_tables:
        data = {}
        cols, rows = model.get_all_records()
        data_buffer = io.BytesIO()
        df: DataFrame = pd.DataFrame(columns=cols, data=rows)
        if model is CsvManagement:
            df.drop(columns=[CsvManagement.dump_status.name], inplace=True)
        df = df.convert_dtypes()
        df.replace({pd.NA: None}, inplace=True)
        if ID in cols:
            df.sort_values(by=ID, inplace=True)
        data['cols'] = df.columns.tolist()
        data['rows'] = df.values.tolist()
        pickle.dump(data, data_buffer, pickle.HIGHEST_PROTOCOL)
        index = dict_table_with_index.get(model.get_table_name())
        file_name = f'{index}.{model.get_table_name()}.{FileExtension.Pickle.value}'
        file_names.append(file_name)
        table_data_s.append(data_buffer)

    # add preview data into zip file
    zip_preview_byte = zip_preview_folder_to_byte()
    if zip_preview_byte is not None:
        table_data_s.append(zip_preview_byte)
        file_names.append(PREVIEW_DATA_FILE_NAME)

    # add nayose data into zip file
    zip_nayose_byte = zip_nayose_folder_to_byte()
    if zip_nayose_byte is not None:
        table_data_s.append(zip_nayose_byte)
        file_names.append(NAYOSE_DATA_FILE_NAME)

    with BridgeStationModel.get_db_proxy() as db_instance:
        for table_name in other_tables:
            # get data in table apscheduler_jobs
            cols, rows = db_instance_exec(db_instance, from_table=table_name)
            data_schedulers = {'cols': cols}
            if table_name == ApschedulerJobs.__tablename__:
                for idx, data in enumerate(rows):
                    rows[idx] = data[:-1] + (pickle.loads(data[-1]),)
            data_schedulers['rows'] = rows
            scheduler_buffer = io.BytesIO()
            pickle.dump(data_schedulers, scheduler_buffer, pickle.HIGHEST_PROTOCOL)
            table_data_s.append(scheduler_buffer)
            idx = dict_table_with_index.get(table_name)
            scheduler_file_name = f'{idx}.{table_name}.{FileExtension.Pickle.value}'
            file_names.append(scheduler_file_name)

    if is_import_db:
        file_obj = io.BytesIO()
        response = zip_a_file(file_obj, table_data_s, file_names, backup_db=True)
    else:
        response = download_zip_file('export_file', table_data_s, file_names)

    return response


@BridgeStationModel.use_db_instance()
def import_config_and_master(file_path, db_instance: PostgreSQL = None, ignore_tables: list[str] = None):
    # file_path = get_zip_setting_full_path(filename)
    input_zip = ZipFile(file_path)
    global dict_table_with_latest_id

    # ↓--- Roll back all data for config tables & mapping tables ---↓
    file_names = sorted(input_zip.namelist())
    for name in file_names:
        if name in [PREVIEW_DATA_FILE_NAME, NAYOSE_DATA_FILE_NAME]:
            with ZipFile(input_zip.open(name)) as file_path:
                file_path.extractall(
                    get_preview_data_path() if name == PREVIEW_DATA_FILE_NAME else get_nayose_path(),
                )
            continue

        table_name = get_table_name_in_file(name)
        if ignore_tables and table_name in ignore_tables:
            continue

        data = input_zip.read(name)
        data = pickle.loads(data)
        cols = data.get('cols')
        rows = data.get('rows')
        if table_name == ApschedulerJobs.__tablename__:
            for idx, data in enumerate(rows):
                rows[idx] = data[:-1] + (pickle.dumps(data[-1]),)
        db_instance.bulk_insert(table_name, cols, rows)

        if table_name != ApschedulerJobs.__tablename__ and len(rows) and MasterModel.Columns.id.name in cols:
            index_of_id = cols.index(MasterModel.Columns.id.name)
            table_name_id_seq = f'{table_name}_id_seq'
            dict_table_with_latest_id[table_name_id_seq] = rows[-1][index_of_id]
    # ↑--- Roll back all data for config tables & mapping tables ---↑

    PostgresSequence.set_sequence_latest_id(db_instance, dict_table_with_latest_id=dict_table_with_latest_id)
    set_break_job_flag(False, db_instance=db_instance)
    scheduler.resume()

    background_announcer.announce(True, AnnounceEvent.IMPORT_CONFIG.name)

    return True


def pull_n_import_sample_data():
    from ap.api.trace_data.services.proc_link import proc_link_count_main, restructure_indexes_gen
    from ap.setting_module.models import CfgDataTable as ESCfgDataTable
    from bridge.services.data_import import get_import_files, import_trans_data_per_process_by_files
    from bridge.services.etl_services.etl_controller import ETLController
    from bridge.services.etl_services.etl_csv_service import EtlCsvService
    from bridge.services.etl_services.etl_import import pull_csv

    with BridgeStationModel.get_db_proxy() as db_instance:
        df_cfg_data_table = CfgDataTable.get_all_as_df(db_instance)
        df_cfg_process = CfgProcess.get_all_as_df(db_instance)

    data_table_ids = df_cfg_data_table['data_table_id'].unique()
    for data_table_id in data_table_ids:
        cfg_data_table = ESCfgDataTable.get_by_id(data_table_id.item())
        etl: Optional[EtlCsvService] = ETLController.get_etl_service(cfg_data_table)
        if etl is None:
            raise NotImplementedError

        job_info = JobInfo()
        job_type = JobType[JobType.PULL_CSV_DATA.name]
        job_info.job_type = job_type
        list(pull_csv(job_type, etl, job_info, ignore_add_job=True))

    process_ids = df_cfg_process['process_id'].unique()
    for process_id in process_ids:
        data_table_id_with_file_names = get_import_files(process_id.item())
        for data_table_id, file_names in data_table_id_with_file_names.items():
            if not file_names:
                continue

            list(
                import_trans_data_per_process_by_files(
                    data_table_id,
                    process_id.item(),
                    file_names,
                    ignore_add_job=True,
                ),
            )

        list(restructure_indexes_gen(process_id.item()))

    list(proc_link_count_main())


def truncate_datatables(models=None):
    with BridgeStationModel.get_db_proxy() as db_instance:
        all_table = db_instance.list_tables()
        if models:
            table_names = get_models(models)  # model from bridge
            table_names = list(table_names)
            table_names = [table_name._table_name for table_name in table_names]
            t_process_tables = [table_name for table_name in all_table if 't_process_' in table_name]
            table_names = table_names + t_process_tables
        else:
            table_names = all_table

    with make_session() as meta_session:
        for tbl_name in table_names:
            sql = f'TRUNCATE TABLE {tbl_name} RESTART IDENTITY CASCADE'
            meta_session.execute(sql)

    return True


def delete_t_process_tables():
    with BridgeStationModel.get_db_proxy() as db_instance:
        all_table = db_instance.list_tables()
        for table_name in all_table:
            if 't_process_' in table_name:
                sql = f'DROP TABLE IF EXISTS {table_name} CASCADE;'
                db_instance.execute_sql(sql)

    return True


def wait_done_jobs():
    while True:
        # Wait to all jobs stop to clear database
        running_jobs = get_running_jobs()
        if not running_jobs:
            break
        sleep(1)


def clear_db_n_data(models=None, is_drop_t_process_tables=False):
    truncate_datatables(models=models)
    if is_drop_t_process_tables:
        delete_t_process_tables()
    delete_folder_data()


def reset_is_show_file_name():
    with BridgeStationModel.get_db_proxy() as db_instance:
        _, rows = CfgProcess.get_all_records(db_instance, row_is_dict=True)
        ids = [row.get('id') for row in rows]
        CfgProcess.bulk_update_by_ids(db_instance, ids, {CfgProcess.Columns.is_show_file_name.name: None})


def pause_job_running(remove_jobs: bool = True):
    if scheduler.running:
        scheduler.pause()
    if remove_jobs:
        scheduler.remove_all_jobs()
    set_break_job_flag(True)


@BridgeStationModel.use_db_instance()
def set_break_job_flag(is_break: bool, db_instance: Union[PostgreSQL, scoped_session] = None):
    # do not allow to set BREAK_JOB=False when we're shutting down application
    if not is_break and is_job_running(job_name=JobType.SHUTDOWN_APP.name):
        return

    CfgConstantModel.create_or_update_by_type(
        db_instance,
        CfgConstantType.BREAK_JOB,
        is_break,
        CfgConstantType.BREAK_JOB,
    )


def pause_resume_current_running_jobs():
    pause_job_running()

    def _inner():
        set_break_job_flag(False)
        scheduler.resume()

    return _inner


def clean_data_folder():
    try:
        shutil.rmtree(get_data_path())
        print('Folder data has been deleted successfully.')
    except OSError as e:
        print(f'Error: Folder data could not be deleted. {e}')


def delete_file_and_folder_by_path(path, ignore_folder=None):
    is_data_path = path == get_data_path()
    for root, dirs, files in os.walk(path):
        if ignore_folder and ignore_folder in root:
            continue

        for file in files:
            if is_data_path and PROCESS_QUEUE_FILE_NAME in file:
                # do not remove process_queue.pkl file, it necessary for multiprocessing management
                continue

            file_path = os.path.join(root, file)
            try:
                os.remove(file_path)
            except OSError as e:
                print(f'Error: File could not be deleted. {e}')

        for dir in dirs:
            dir_path = os.path.join(root, dir)
            if os.path.basename(dir_path) != ignore_folder:
                try:
                    shutil.rmtree(dir_path)
                except OSError as e:
                    print(f'Error: Folder data could not be deleted. {e}')
    return {}, 200


def delete_folder_data(ignore_folder='preview'):
    data_path = get_data_path()
    delete_file_and_folder_by_path(data_path, ignore_folder=ignore_folder)
