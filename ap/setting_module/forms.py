from flask_wtf import FlaskForm
from wtforms import BooleanField, FieldList, FormField, IntegerField, StringField


class DataSourceCsvForm(FlaskForm):
    id = StringField('dataSourceId')
    directory = StringField()
    skip_head = IntegerField()
    skip_tail = IntegerField()
    delimiter = StringField()
    etl_func = StringField()


class DataSourceForm(FlaskForm):
    id = StringField()
    name = StringField()
    type = StringField()
    master_type = StringField()
    comment = StringField()
    order = IntegerField()
    is_direct_import = BooleanField()


class DataTableForm(FlaskForm):
    id = StringField()

    name = StringField()
    data_source = FormField(DataSourceForm)
    partition_from = StringField()
    partition_to = StringField()
    table_name = StringField()
    comment = StringField()

    # mapping page related fields
    scan_done = BooleanField()
    mapping_page_enabled = BooleanField()
    has_new_master = BooleanField()


class ProcessColumnsForm(FlaskForm):
    id = StringField()
    process_id = StringField()
    column_name = StringField()
    english_name = StringField()
    name = StringField()
    data_type = StringField()
    operator = StringField()
    coef = StringField()
    column_type = IntegerField()
    is_serial_no = BooleanField()
    is_get_date = BooleanField()
    is_auto_increment = BooleanField()
    order = IntegerField()


class ProcessCfgForm(FlaskForm):
    id = StringField()
    name = StringField('proc_id')
    data_source_id = StringField()
    table_name = StringField()
    comment = StringField()
    order = IntegerField()
    columns = FieldList(FormField(ProcessColumnsForm))
