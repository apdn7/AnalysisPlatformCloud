from ap.common.constants import DEFAULT_POSTGRES_SCHEMA
from ap.common.pydn.dblib.db_common import add_single_quote
from ap.common.pydn.dblib.postgresql import PostgreSQL
from bridge.models.bridge_station import BridgeStationModel


def create_mapping_factory_machine_view(db_instance):
    sql_statement = '''
    CREATE OR REPLACE VIEW mapping_factory_machine_view AS
    select distinct mfm.t_location_abbr,
                    mfm.t_location_name,
                    mfm.t_factory_abbr,
                    mfm.t_factory_id,
                    mfm.t_factory_name,
                    mfm.t_plant_abbr,
                    mfm.t_plant_id,
                    mfm.t_plant_name,
                    mfm.t_dept_abbr,
                    mfm.t_dept_id,
                    mfm.t_dept_name,
                    mfm.t_sect_abbr,
                    mfm.t_sect_id,
                    mfm.t_sect_name,
                    mfm.t_prod_family_abbr,
                    mfm.t_prod_family_id,
                    mfm.t_prod_family_name,
                    mfm.t_line_id,
                    mfm.t_line_name,
                    mfm.t_line_no,
                    mfm.t_outsource,
                    mfm.t_equip_id,
                    mfm.t_equip_name,
                    mfm.t_equip_no,
                    mfm.t_equip_product_date,
                    mfm.t_equip_product_no,
                    mfm.t_station_no,
                    mfm.t_process_abbr,
                    mfm.t_process_id,
                    mfm.t_process_name,
                    mlc.location_name_jp,
                    mlc.location_abbr,
                    mf.factory_factid,
                    mf.factory_name_jp,
                    mf.factory_name_sys,
                    mf.factory_abbr_jp,
                    mpl.plant_factid,
                    mpl.plant_name_jp,
                    mpl.plant_name_sys,
                    mpl.plant_abbr_jp,
                    md.dept_factid,
                    md.dept_name_jp,
                    md.dept_name_sys,
                    md.dept_abbr_jp,
                    ms.sect_factid,
                    ms.sect_name_jp,
                    ms.sect_name_sys,
                    ms.sect_abbr_jp,
                    mpf.prod_family_factid,
                    mpf.prod_family_name_jp,
                    mpf.prod_family_name_sys,
                    mpf.prod_family_abbr_jp,
                    mp.process_factid,
                    mp.process_name_jp,
                    mp.process_name_sys,
                    mp.process_abbr_jp,
                    ml.line_factid,
                    ml.line_no,
                    ml.line_sign,
                    ml.outsourcing_flag,
                    ml.outsource,
                    mlg.line_name_jp,
                    mlg.line_name_sys,
                    me.equip_factid,
                    me.equip_no,
                    me.equip_sign,
                    me.equip_product_no,
                    me.equip_product_date,
                    meg.equip_name_jp,
                    meg.equip_name_sys,
                    rfm.equip_st,
                    mst.st_no,
                    mst.st_sign,
                    mlc.id            as location_id,
                    mf.id             as factory_id,
                    mpl.id            as plant_id,
                    md.id             as dept_id,
                    ms.id             as sect_id,
                    mpf.id            as prod_family_id,
                    mp.id             as process_id,
                    ml.id             as line_id,
                    mlg.id            as line_group_id,
                    me.id             as equip_id,
                    meg.id            as equip_group_id,
                    mst.id            as st_id
    from mapping_factory_machine mfm
             inner join r_factory_machine rfm on rfm.id = mfm.factory_machine_id
             inner join m_line ml on ml.id = rfm.line_id
             inner join m_line_group mlg on mlg.id = ml.line_group_id
             inner join m_plant mpl on mpl.id = ml.plant_id
             inner join m_factory mf on mf.id = mpl.factory_id
             inner join m_equip me on me.id = rfm.equip_id
             inner join m_equip_group meg on meg.id = me.equip_group_id
             inner join m_process mp on mp.id = rfm.process_id
             inner join m_prod_family mpf on mpf.id = mp.prod_family_id
             inner join m_sect ms on ms.id = rfm.sect_id
             inner join m_dept md on md.id = ms.dept_id
             inner join m_location mlc on mlc.id = mf.location_id
             inner join m_st mst on mst.id = rfm.st_id
    order by mp.process_name_jp;'''
    db_instance.execute_sql(sql_statement)


def create_mapping_part_view(db_instance):
    sql_statement = '''
    CREATE OR REPLACE VIEW mapping_part_view AS
    select distinct mpp.t_part_no,
                    mpp.t_part_abbr,
                    mpp.t_part_name,
                    mpp.t_part_no_full,
                    mpp.t_part_type,
                    mpt.part_name_jp,
                    mpt.part_abbr_jp,
                    mpt.assy_flag,
                    mpt.part_type_factid,
                    mp.part_factid,
                    mp.part_no,
                    ml.location_name_jp,
                    ml.location_name_en,
                    mpr.prod_name_jp,
                    mpr.prod_name_sys,
                    mprf.prod_family_name_jp,
                    mprf.prod_family_name_sys,
                    mp.id              as part_id,
                    mp.part_type_id,
                    mp.location_id,
                    mpr.id             as prod_id,
                    mpr.prod_family_id
    from mapping_part mpp
             inner join m_part mp on mp.id = mpp.part_id
             inner join m_part_type mpt on mpt.id = mp.part_type_id
             inner join r_prod_part rpp on rpp.part_id = mp.id
             inner join m_prod mpr on mpr.id = rpp.prod_id
             inner join m_prod_family mprf on mprf.id = mpr.prod_family_id
             inner join m_location ml on ml.id = mp.location_id
    order by mpt.part_type_factid;'''
    db_instance.execute_sql(sql_statement)


def create_mapping_process_data_view(db_instance):
    sql_statement = '''
    CREATE OR REPLACE VIEW mapping_process_data_view AS
    select distinct mpd.t_prod_family_id,
                    mpd.t_prod_family_name,
                    mpd.t_prod_family_abbr,
                    mpd.t_process_id,
                    mpd.t_process_name,
                    mpd.t_process_abbr,
                    mpd.t_data_id,
                    mpd.t_data_name,
                    mpd.t_data_abbr,
                    mpd.t_unit,
                    mpf.prod_family_factid,
                    mpf.prod_family_name_jp,
                    mpf.prod_family_name_sys,
                    mpf.prod_family_abbr_jp,
                    mp.process_factid,
                    mp.process_name_jp,
                    mp.process_name_sys,
                    mp.process_abbr_jp,
                    md.data_factid,
                    md.data_type,
                    mdg.data_name_jp,
                    mdg.data_name_sys,
                    mdg.data_abbr_jp,
                    mu.unit,
                    mp.prod_family_id,
                    mp.id             as process_id,
                    md.id             as data_id,
                    mdg.id            as data_group_id,
                    mu.id             as unit_id
    from mapping_process_data mpd
             inner join m_data md on md.id = mpd.data_id
             inner join m_data_group mdg on mdg.id = md.data_group_id
             inner join m_unit mu on mu.id = md.unit_id
             inner join m_process mp on mp.id = md.process_id
             inner join m_prod_family mpf on mpf.id = mp.prod_family_id
    order by mp.process_name_jp, mdg.data_name_jp;'''
    db_instance.execute_sql(sql_statement)


def create_mapping_factory_machine_process_data_view(db_instance):
    sql_statement = '''
    CREATE OR REPLACE VIEW mapping_factory_machine_process_data_view AS
    select distinct mfmv.t_location_abbr,
                    mfmv.t_location_name,
                    mfmv.t_factory_abbr,
                    mfmv.t_factory_id,
                    mfmv.t_factory_name,
                    mfmv.t_plant_abbr,
                    mfmv.t_plant_id,
                    mfmv.t_plant_name,
                    mfmv.t_dept_abbr,
                    mfmv.t_dept_id,
                    mfmv.t_dept_name,
                    mfmv.t_sect_abbr,
                    mfmv.t_sect_id,
                    mfmv.t_sect_name,
                    mfmv.t_prod_family_abbr,
                    mfmv.t_prod_family_id,
                    mfmv.t_prod_family_name,
                    mfmv.t_line_id,
                    mfmv.t_line_name,
                    mfmv.t_line_no,
                    mfmv.t_outsource,
                    mfmv.t_equip_id,
                    mfmv.t_equip_name,
                    mfmv.t_equip_no,
                    mfmv.t_equip_product_date,
                    mfmv.t_equip_product_no,
                    mfmv.t_station_no,
                    mfmv.t_process_abbr,
                    mfmv.t_process_id,
                    mfmv.t_process_name,
                    mpdv.t_data_id,
                    mpdv.t_data_name,
                    mpdv.t_data_abbr,
                    mfmv.location_name_jp,
                    mfmv.location_abbr,
                    mfmv.factory_factid,
                    mfmv.factory_name_jp,
                    mfmv.factory_name_sys,
                    mfmv.factory_abbr_jp,
                    mfmv.plant_factid,
                    mfmv.plant_name_jp,
                    mfmv.plant_name_sys,
                    mfmv.plant_abbr_jp,
                    mfmv.dept_factid,
                    mfmv.dept_name_jp,
                    mfmv.dept_name_sys,
                    mfmv.dept_abbr_jp,
                    mfmv.sect_factid,
                    mfmv.sect_name_jp,
                    mfmv.sect_name_sys,
                    mfmv.sect_abbr_jp,
                    mfmv.prod_family_factid,
                    mfmv.prod_family_name_jp,
                    mfmv.prod_family_name_sys,
                    mfmv.prod_family_abbr_jp,
                    mfmv.process_factid,
                    mfmv.process_name_jp,
                    mfmv.process_name_sys,
                    mfmv.process_abbr_jp,
                    mfmv.line_factid,
                    mfmv.line_no,
                    mfmv.line_sign,
                    mfmv.outsourcing_flag,
                    mfmv.outsource,
                    mfmv.line_name_jp,
                    mfmv.line_name_sys,
                    mfmv.equip_factid,
                    mfmv.equip_no,
                    mfmv.equip_sign,
                    mfmv.equip_product_no,
                    mfmv.equip_product_date,
                    mfmv.equip_name_jp,
                    mfmv.equip_name_sys,
                    mfmv.equip_st,
                    mfmv.st_no,
                    mfmv.st_sign,
                    mpdv.data_factid,
                    mpdv.data_type,
                    mpdv.data_name_jp,
                    mpdv.data_name_sys,
                    mpdv.data_abbr_jp,
                    mpdv.unit,
                    mfmv.location_id,
                    mfmv.factory_id,
                    mfmv.plant_id,
                    mfmv.dept_id,
                    mfmv.sect_id,
                    mfmv.prod_family_id,
                    mfmv.process_id,
                    mfmv.line_id,
                    mfmv.line_group_id,
                    mfmv.equip_id,
                    mfmv.equip_group_id,
                    mfmv.st_id,
                    mpdv.data_group_id,
                    mpdv.data_id,
                    mpdv.unit_id
    from mapping_factory_machine_view mfmv
             inner join mapping_process_data_view mpdv on mpdv.process_id = mfmv.process_id
    order by mfmv.process_name_jp, mpdv.data_name_jp;'''
    db_instance.execute_sql(sql_statement)


VIEW_TABLE_DICT = {
    'mapping_factory_machine_view': create_mapping_factory_machine_view,
    'mapping_part_view': create_mapping_part_view,
    'mapping_process_data_view': create_mapping_process_data_view,
    'mapping_factory_machine_process_data_view': create_mapping_factory_machine_process_data_view,
}


@BridgeStationModel.use_db_instance()
def gen_view_tables(db_instance: PostgreSQL = None):
    for view_name, create_func in VIEW_TABLE_DICT.items():
        try:
            create_func(db_instance)
            print(f'{view_name} view table is created')
        except Exception as e:
            raise e


@BridgeStationModel.use_db_instance()
def check_view_tables_exist(view_name, db_instance: PostgreSQL = None):
    sql_exist = (
        f"SELECT viewname FROM postgres.pg_catalog.pg_views WHERE viewname = '{view_name}' "
        f"AND schemaname = {add_single_quote(DEFAULT_POSTGRES_SCHEMA)}"
    )
    _, rows = db_instance.run_sql(sql_exist, row_is_dict=False)
    return bool(rows)


def drop_all_view_tables():
    with BridgeStationModel.get_db_proxy() as db_instance:
        for view_name, _ in VIEW_TABLE_DICT.items():
            sql_exist = f'DROP VIEW IF EXISTS {view_name} CASCADE;'
            _, rows = db_instance.run_sql(sql_exist)
