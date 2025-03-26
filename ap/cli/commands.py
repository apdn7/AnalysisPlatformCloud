from flask.cli import AppGroup

from ap.setting_module.services.process_config import get_all_data_tables
from bridge.services.scan_data_type import scan_data_type

cli = AppGroup('cli')


@cli.command('gen_preview')
def gen_preview_data():
    print('Start to generate preview data')

    cfg_data_tables = get_all_data_tables()
    for cfg_data_table in cfg_data_tables:
        list(scan_data_type(data_table_id=cfg_data_table.id, is_user_approved_master=True))
