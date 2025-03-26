import os

from flask import Blueprint
from flask_babel import gettext as _

from ap.common.flask_customize import render_template
from ap.setting_module.models import CfgDataTable
from ap.setting_module.schemas import ProcessFullSchema
from ap.setting_module.services.process_config import get_all_process

table_viewer_blueprint = Blueprint(
    'table_viewer',
    __name__,
    template_folder=os.path.join('..', 'templates', 'table_viewer'),
    static_folder=os.path.join('..', 'static', 'table_viewer'),
    static_url_path=os.path.join(os.sep, 'static', 'table_viewer'),
    url_prefix='/ap',
)


@table_viewer_blueprint.route('/table_viewer')
def index():
    all_procs = get_all_process()
    list_dict_all_process = []
    for proc in all_procs:
        data_table_dict = CfgDataTable.get_by_process_id(proc.id)
        data_table_ids = [e.get('id') for e in data_table_dict]
        cfg_data_table: CfgDataTable = CfgDataTable.get_by_id(data_table_ids[0])  # TODO: get all data table
        process_schema = ProcessFullSchema()
        proc.data_source = cfg_data_table.data_source
        proc.data_tables = [cfg_data_table]
        list_dict_all_process.append(process_schema.dump(proc))

    output_dict = {
        'page_title': _('Table Viewer'),
        'procs': list_dict_all_process,
    }
    return render_template('index.html', is_json_dumps_loads=False, **output_dict)
