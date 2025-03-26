import json
import os

from flask import Blueprint
from flask_babel import get_locale
from flask_babel import gettext as _

from ap import AppSource, app_source
from ap.common.common_utils import (
    get_about_md_file,
    get_data_path,
    get_error_trace_path,
    get_files,
    get_log_path,
    get_terms_of_use_md_file,
    get_wrapr_path,
)
from ap.common.constants import DEFAULT_POLLING_FREQ, CfgConstantType
from ap.common.flask_customize import render_template
from ap.common.services.jp_to_romaji_utils import to_romaji
from ap.setting_module.forms import DataSourceForm, DataTableForm
from ap.setting_module.models import CfgConstant, CfgDataSource, CfgDataTable, make_session
from ap.setting_module.schemas import DataSourceSchema
from ap.setting_module.services.about import markdown_to_html
from ap.setting_module.services.process_config import (
    get_all_data_tables,
    get_all_functions,
    get_all_process,
    get_all_process_no_nested,
)
from bridge.models.cfg_data_source import (
    get_data_sourced_types_for_csv,
    get_data_sourced_types_for_db,
)

# socketio = web_socketio[SOCKETIO]

setting_module_blueprint = Blueprint(
    'setting_module',
    __name__,
    template_folder=os.path.join('..', 'templates', 'setting_module'),
    static_folder=os.path.join('..', 'static', 'setting_module'),
    static_url_path=os.path.join(os.sep, 'static', 'setting_module'),
    url_prefix='/ap',
)


@setting_module_blueprint.route('/config')
def config_screen():
    data_tables = get_all_data_tables()
    data_tables = [DataTableForm(obj=dt) for dt in data_tables]

    data_sources = CfgDataSource.get_all()
    data_source_forms = [DataSourceForm(obj=ds) for ds in data_sources]

    ds_schema = DataSourceSchema(many=True)
    all_datasource = ds_schema.dumps(data_sources)
    import_err_dir = get_error_trace_path().replace('\\', '/')

    # get polling frequency
    polling_frequency = CfgConstant.get_value_by_type_first(CfgConstantType.POLLING_FREQUENCY.name)
    if polling_frequency is None:
        # set default polling freq.
        polling_frequency = DEFAULT_POLLING_FREQ
        with make_session() as session:
            CfgConstant.create_or_update_by_type(
                session,
                const_type=CfgConstantType.POLLING_FREQUENCY.name,
                const_value=polling_frequency,
            )

    all_procs = get_all_process()
    for proc in all_procs:
        data_table_dict = CfgDataTable.get_by_process_id(proc.id)
        data_source_ids = [e.get('data_source_id') for e in data_table_dict]
        proc.data_table_name = ' | '.join([e.get('name') for e in data_table_dict])
        proc.data_source_name = ' | '.join(
            [e.name for e in filter(lambda e: e.id in data_source_ids, data_sources)],
        )
    processes = get_all_process_no_nested()
    # generate english name for process
    for proc_data in processes:
        if not proc_data['name_en']:
            proc_data['name_en'] = to_romaji(proc_data['name'])
    procs = [(proc.get('id'), proc.get('shown_name'), proc.get('name_en')) for proc in processes]
    all_functions = get_all_functions()

    output_dict = {
        'page_title': _('Application Configuration'),
        'proc_list': all_procs,
        # 'data_table_list': [db_model_to_dict(data_table) for data_table in data_tables],
        'data_table_list': data_tables,
        'procs': procs,
        'import_err_dir': import_err_dir,
        'polling_frequency': int(polling_frequency),
        'data_sources': data_source_forms,
        # 'all_datasource': simple_json_dumps(all_datasource)
        # 'all_datasource': json_dumps_loads(all_datasource)
        'all_datasource': all_datasource,
        # 'ds_tables': ds_tables
        'log_path': get_log_path(),
        'data_path': get_data_path(),
        'all_function': json.dumps(all_functions),
    }

    # get R ETL wrap functions
    wrap_path = get_wrapr_path()
    func_etl_path = os.path.join(wrap_path, 'func', 'etl')
    try:
        # BRIDGE STATION - Refactor DN & OSS version
        if app_source == AppSource.DN.value:
            etl_scripts = get_files(directory=func_etl_path, depth_from=1, depth_to=1, file_name_only=True) or []
        else:
            etl_scripts = []
    except Exception:
        etl_scripts = []
    output_dict.update({'etl_scripts': etl_scripts})

    # Master Db Type drop down
    master_db_types = get_data_sourced_types_for_db()
    master_csv_types = get_data_sourced_types_for_csv()

    output_dict.update({'master_db_types': master_db_types, 'master_csv_types': master_csv_types})

    return render_template('config.html', is_json_dumps_loads=False, **output_dict)


@setting_module_blueprint.route('/config/filter')
def filter_config():
    processes = get_all_process_no_nested()
    # generate english name for process
    for proc_data in processes:
        if not proc_data['name_en']:
            proc_data['name_en'] = to_romaji(proc_data['name'])
    output_dict = {
        'procs': processes,
    }
    return render_template('filter_config.html', **output_dict)


@setting_module_blueprint.route('/config/master_cfg')
def master_cfg():
    processes = get_all_process()
    output_dict = {
        'procs': processes,
    }
    return render_template('master_config.html', **output_dict)


@setting_module_blueprint.route('/config/job', methods=['GET'])
def background_process():
    output_dict = {'page_title': _('Job List'), 'jobs': []}
    return render_template('background_job.html', **output_dict)


@setting_module_blueprint.route('/config/job/failed', methods=['GET'])
def failed_jobs():
    output_dict = {'page_title': _('Failed Job List'), 'jobs': []}
    return render_template('failed_jobs.html', **output_dict)


@setting_module_blueprint.route('/about', methods=['GET'])
def about():
    """
    about page
    """
    markdown_file_path = get_about_md_file()
    css, html = markdown_to_html(markdown_file_path)
    return render_template('about.html', css=css, content=html)


@setting_module_blueprint.route('/terms_of_use', methods=['GET'])
def term_of_use():
    """
    term of use page
    """
    current_locale = get_locale()
    markdown_file_path = get_terms_of_use_md_file(current_locale)
    css, html = markdown_to_html(markdown_file_path)
    return render_template('terms_of_use.html', css=css, content=html, do_not_send_ga=True)


@setting_module_blueprint.route('/config/master')
def master_config():
    processes = get_all_process_no_nested()
    # generate english name for process
    for proc_data in processes:
        if not proc_data['name_en']:
            proc_data['name_en'] = to_romaji(proc_data['name'])
    output_dict = {
        'procs': processes,
    }
    return render_template('master_cfg.html', **output_dict)


@setting_module_blueprint.route('/register_by_file')
def register_by_file_page():
    return render_template('register_by_file.html')
