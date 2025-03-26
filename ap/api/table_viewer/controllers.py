import json

from flask import Blueprint, request

from ap.api.efa.services.etl import detect_file_path_delimiter
from ap.common.common_utils import get_csv_delimiter, get_latest_files
from ap.common.constants import DataType, DBType
from ap.common.pydn.dblib.db_common import db_instance_exec
from ap.common.pydn.dblib.db_proxy_readonly import ReadOnlyDbProxy
from ap.common.services.http_content import json_dumps
from ap.common.services.jp_to_romaji_utils import to_romaji
from ap.common.services.sse import MessageAnnouncer
from ap.setting_module.models import CfgDataSource
from bridge.services.etl_services.etl_controller import ETLController

api_table_viewer_blueprint = Blueprint('api_table_viewer', __name__, url_prefix='/ap/api/table_viewer')


@api_table_viewer_blueprint.route('/column_names', methods=['GET'])
def get_column_names():
    """[summary]
    show_column_names
    Returns:
        [type] -- [description]
    """
    database = request.args.get('database')
    table = request.args.get('table')

    blank_output = json_dumps({'cols': [], 'rows': []})

    data_source = CfgDataSource.query.get(database)
    if not data_source:
        return blank_output

    with ReadOnlyDbProxy(data_source) as db_instance:
        if not db_instance or not table:
            return blank_output

        cols = db_instance.list_table_columns(table)
        for col in cols:
            col['romaji'] = to_romaji(col['name'])

    content = {
        'cols': cols,
    }

    return json_dumps(content)


@api_table_viewer_blueprint.route('/table_records', methods=['POST'])
def get_table_records():
    """[summary]
    Show limited records
    Returns:
        [type] -- [description]
    """

    request_data = json.loads(request.data)
    db_code = request_data.get('database_code')
    table_name = request_data.get('table_name')
    sort_column = request_data.get('sort_column')
    sort_order = request_data.get('sort_order') or 'DESC'
    limit = request_data.get('limit') or 5

    blank_output = json_dumps({'cols': [], 'rows': []})

    if not db_code or sort_order not in ('ASC', 'DESC'):
        return blank_output

    data_source: CfgDataSource = CfgDataSource.query.get(db_code)
    if not data_source:
        return blank_output

    if data_source.type == DBType.CSV.name:
        csv_detail = data_source.csv_detail
        data_table = data_source.data_tables[0]  # TODO: check data source has many data table
        cols_with_types, rows = get_csv_data(csv_detail, data_table, sort_column, sort_order, int(limit))
    else:
        with ReadOnlyDbProxy(data_source) as db_instance:
            if not db_instance or not table_name:
                return blank_output

            cols_with_types = db_instance.list_table_columns(table_name)
            for col in cols_with_types:
                col['romaji'] = to_romaji(col['name'])

            cols, rows = query_data(db_instance, table_name, sort_column, sort_order, limit)

    result = {'cols': cols_with_types, 'rows': rows}
    return json_dumps(result)


@MessageAnnouncer.notify_progress(50)
def query_data(db_instance, table_name, sort_column, sort_order, limit):
    sort_statement = ''
    if sort_column and sort_order:
        sort_statement = 'order by "{}" {} '.format(sort_column, sort_order)

    cols, rows = db_instance_exec(
        db_instance,
        from_table=table_name,
        limit=limit,
        order=sort_statement,
        row_is_dict=True,
    )
    return cols, rows


@MessageAnnouncer.notify_progress(50)
def get_csv_data(csv_detail, data_table, sort_colum, sort_order, limit):
    latest_file = get_latest_files(csv_detail.directory)
    latest_file = latest_file[0:1][0]
    csv_delimiter = get_csv_delimiter(csv_detail.delimiter)
    # line_skip = '' if (csv_detail.skip_head == 0 and not csv_detail.dummy_header) else csv_detail.skip_head
    # etl_func = csv_detail.etl_func
    # delimiter check
    _, encoding = detect_file_path_delimiter(
        latest_file,
        csv_delimiter,
        with_encoding=True,
    )
    etl_service = ETLController.get_etl_service(data_table)
    dic_use_cols = {col.column_name: DataType.TEXT.name for col in data_table.columns}
    data_stream = etl_service.standard_csv(
        dic_use_cols,
        latest_file,
        encoding,
        csv_delimiter,
    )
    for df in data_stream:
        df_data = df
        # org_header, header_names, _, _, data_details, encoding, skip_tail, _ = get_csv_data_from_files(
        #     [latest_file],
        #     line_skip,
        #     etl_func,
        #     csv_delimiter,
        #     max_records=None,
        # )

        # df_data = pd.DataFrame(columns=org_header, data=data_details)

        # if sort_colum: # TODO: check duplicated column
        #   dict_column_name = dict(zip(org_header, header_names))
        #   sort_column_raw_name = dict_column_name[sort_colum]
        #   if sort_column_raw_name and sort_column_raw_name in df_data.columns:
        #       asc = sort_order == 'ASC'
        #       df_data.sort_values(by=[sort_column_raw_name], ascending=asc, inplace=True)

        if sort_colum and sort_colum in df_data.columns:
            asc = sort_order == 'ASC'
            df_data.sort_values(by=[sort_colum], ascending=asc, inplace=True)

        df_data = df_data.head(limit)
        cols = df_data.columns
        rows = [dict(zip(cols, vals)) for vals in df_data[0:limit][cols].to_records(index=False).tolist()]
        cols = [{'name': col} for col in cols]

        return cols, rows
