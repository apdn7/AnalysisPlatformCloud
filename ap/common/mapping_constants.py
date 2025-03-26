from ap.common.constants import BaseEnum
from ap.setting_module.models import (
    CfgDataTable,
    MappingFactoryMachine,
    MappingPart,
    MappingProcessData,
    MData,
    MDataGroup,
    MDept,
    MEquip,
    MEquipGroup,
    MFactory,
    MLine,
    MLineGroup,
    MLocation,
    MPart,
    MPartType,
    MPlant,
    MProcess,
    MProd,
    MProdFamily,
    MSect,
    MSt,
    MUnit,
    RFactoryMachine,
    RProdPart,
)


class DBColumnName(BaseEnum):
    id = 'id'
    created_at = 'created_at'
    updated_at = 'updated_at'
    source = 'source'
    data_table_name = 'data_table_name'
    source_row_index = 'source_row_index'
    data_source_name = 'data_source_name'
    original_id = 'original_id'
    is_file_row = 'is_file_row'
    is_mapped_id = 'is_mapped_id'
    is_new_registered = 'is_new_registered'
    is_new_added = 'is_new_added'
    group_id = 'group_id'
    sub_part_ids = 'sub_part_ids'
    number = 'number'
    master = 'Master'
    master_candidate = 'Master Candidate'
    state = 'state'
    data_source_id = CfgDataTable.data_source_id.name

    # mapping_factory_machine table
    t_location_name = MappingFactoryMachine.t_location_name.name
    t_location_abbr = MappingFactoryMachine.t_location_abbr.name
    t_line_id = MappingFactoryMachine.t_line_id.name
    t_line_name = MappingFactoryMachine.t_line_name.name
    t_line_no = MappingFactoryMachine.t_line_no.name
    t_outsource = MappingFactoryMachine.t_outsource.name
    t_equip_id = MappingFactoryMachine.t_equip_id.name
    t_equip_name = MappingFactoryMachine.t_equip_name.name
    t_equip_no = MappingFactoryMachine.t_equip_no.name
    t_equip_product_date = MappingFactoryMachine.t_equip_product_date.name
    t_equip_product_no = MappingFactoryMachine.t_equip_product_no.name
    t_station_no = MappingFactoryMachine.t_station_no.name
    t_prod_abbr = MappingFactoryMachine.t_prod_abbr.name
    t_prod_id = MappingFactoryMachine.t_prod_id.name
    t_prod_name = MappingFactoryMachine.t_prod_name.name
    t_process_id = MappingFactoryMachine.t_process_id.name
    t_process_name = MappingFactoryMachine.t_process_name.name
    t_process_abbr = MappingFactoryMachine.t_process_abbr.name
    t_dept_id = MappingFactoryMachine.t_dept_id.name
    t_dept_name = MappingFactoryMachine.t_dept_name.name
    t_dept_abbr = MappingFactoryMachine.t_dept_abbr.name
    t_sect_id = MappingFactoryMachine.t_sect_id.name
    t_sect_name = MappingFactoryMachine.t_sect_name.name
    t_sect_abbr = MappingFactoryMachine.t_sect_abbr.name
    t_factory_id = MappingFactoryMachine.t_factory_id.name
    t_factory_name = MappingFactoryMachine.t_factory_name.name
    t_factory_abbr = MappingFactoryMachine.t_factory_abbr.name
    t_plant_id = MappingFactoryMachine.t_plant_id.name
    t_plant_name = MappingFactoryMachine.t_plant_name.name
    t_plant_abbr = MappingFactoryMachine.t_plant_abbr.name
    t_prod_family_id = MappingFactoryMachine.t_prod_family_id.name
    t_prod_family_name = MappingFactoryMachine.t_prod_family_name.name
    t_prod_family_abbr = MappingFactoryMachine.t_prod_family_abbr.name
    factory_machine_id = MappingFactoryMachine.factory_machine_id.name
    data_table_id = MappingFactoryMachine.data_table_id.name

    # mapping_part table
    t_part_no = MappingPart.t_part_no.name
    t_part_abbr = MappingPart.t_part_abbr.name
    t_part_name = MappingPart.t_part_name.name
    t_part_no_full = MappingPart.t_part_no_full.name
    t_part_type = MappingPart.t_part_type.name
    part_id = MappingPart.part_id.name

    # mapping_process_data table
    t_data_id = MappingProcessData.t_data_id.name
    t_data_name = MappingProcessData.t_data_name.name
    t_data_abbr = MappingProcessData.t_data_abbr.name
    data_id = MappingProcessData.data_id.name

    # r_factory_machine table
    line_id = RFactoryMachine.line_id.name
    equip_id = RFactoryMachine.equip_id.name
    equip_st = RFactoryMachine.equip_st.name
    sect_id = RFactoryMachine.sect_id.name
    st_id = RFactoryMachine.st_id.name

    # r_prod_part table
    prod_id = RProdPart.prod_id.name

    # m_data table
    process_id = MData.process_id.name
    data_group_id = MData.data_group_id.name
    data_type = MData.data_type.name
    unit_id = MData.unit_id.name
    config_equation_id = MData.config_equation_id.name
    data_factid = MData.data_factid.name

    # m_data_group table
    data_name_jp = MDataGroup.data_name_jp.name
    data_name_en = MDataGroup.data_name_en.name
    data_name_sys = MDataGroup.data_name_sys.name
    data_name_local = MDataGroup.data_name_local.name
    data_name_all = 'data_name_all'
    data_abbr_jp = MDataGroup.data_abbr_jp.name
    data_abbr_en = MDataGroup.data_abbr_en.name
    data_abbr_local = MDataGroup.data_abbr_local.name
    data_group_type = MDataGroup.data_group_type.name

    # m_dept table
    dept_factid = MDept.dept_factid.name
    dept_name_jp = MDept.dept_name_jp.name
    dept_name_en = MDept.dept_name_en.name
    dept_name_sys = MDept.dept_name_sys.name
    dept_name_local = MDept.dept_name_local.name
    dept_name_all = 'dept_name_all'
    dept_abbr_jp = MDept.dept_abbr_jp.name
    dept_abbr_en = MDept.dept_abbr_en.name
    dept_abbr_local = MDept.dept_abbr_local.name

    # m_equip table
    equip_group_id = MEquip.equip_group_id.name
    equip_no = MEquip.equip_no.name
    equip_sign = MEquip.equip_sign.name
    equip_factid = MEquip.equip_factid.name
    equip_product_no = MEquip.equip_product_no.name
    equip_product_date = MEquip.equip_product_date.name

    # m_st table
    # equip_id = MSt.equip_id.name
    # st_id = 'st_id'
    st_no = MSt.st_no.name
    st_sign = MSt.st_sign.name

    # m_equip_group table
    equip = 'equip'
    equip_name_jp = MEquipGroup.equip_name_jp.name
    equip_name_en = MEquipGroup.equip_name_en.name
    equip_name_sys = MEquipGroup.equip_name_sys.name
    equip_name_local = MEquipGroup.equip_name_local.name
    equip_name_all = 'equip_name_all'

    # m_factory table
    factory_factid = MFactory.factory_factid.name
    factory_name_jp = MFactory.factory_name_jp.name
    factory_name_en = MFactory.factory_name_en.name
    factory_name_sys = MFactory.factory_name_sys.name
    factory_name_local = MFactory.factory_name_local.name
    factory_name_all = 'factory_name_all'
    location_id = MFactory.location_id.name
    factory_abbr_jp = MFactory.factory_abbr_jp.name
    factory_abbr_en = MFactory.factory_abbr_en.name
    factory_abbr_local = MFactory.factory_abbr_local.name

    # m_line table
    plant_id = MLine.plant_id.name
    prod_family_id = MLine.prod_family_id.name
    line_group_id = MLine.line_group_id.name
    line_factid = MLine.line_factid.name
    line_no = MLine.line_no.name
    outsourcing_flag = MLine.outsourcing_flag.name
    line_sign = MLine.line_sign.name
    outsource = MLine.outsource.name

    # m_line_group table
    line_name_jp = MLineGroup.line_name_jp.name
    line_name_en = MLineGroup.line_name_en.name
    line_name_sys = MLineGroup.line_name_sys.name
    line_name_local = MLineGroup.line_name_local.name
    line_name_all = 'line_name_all'

    # m_part table
    part_type_id = MPart.part_type_id.name
    part_factid = MPart.part_factid.name
    part_no = MPart.part_no.name
    part_use = MPart.part_use.name

    # m_parts_type table
    part_type_factid = MPartType.part_type_factid.name
    part_name_jp = MPartType.part_name_jp.name
    part_name_en = MPartType.part_name_en.name
    part_name_local = MPartType.part_name_local.name
    part_abbr_jp = MPartType.part_abbr_jp.name
    part_abbr_en = MPartType.part_abbr_en.name
    part_abbr_local = MPartType.part_abbr_local.name
    part_name_all = 'part_name_all'
    assy_flag = MPartType.assy_flag.name

    # m_plant table
    factory_id = MPlant.factory_id.name
    plant_factid = MPlant.plant_factid.name
    plant_name_jp = MPlant.plant_name_jp.name
    plant_name_en = MPlant.plant_name_en.name
    plant_name_sys = MPlant.plant_name_sys.name
    plant_name_local = MPlant.plant_name_local.name
    plant_name_all = 'plant_name_all'
    plant_abbr_jp = MPlant.plant_abbr_jp.name
    plant_abbr_en = MPlant.plant_abbr_en.name
    plant_abbr_local = MPlant.plant_abbr_local.name

    # m_process table
    process = 'process'
    process_factid = MProcess.process_factid.name
    process_name_jp = MProcess.process_name_jp.name
    process_name_en = MProcess.process_name_en.name
    process_name_sys = MProcess.process_name_sys.name
    process_name_local = MProcess.process_name_local.name
    process_name_all = 'process_name_all'
    process_abbr_jp = MProcess.process_abbr_jp.name
    process_abbr_en = MProcess.process_abbr_en.name
    process_abbr_local = MProcess.process_abbr_local.name

    # m_prod table table
    prod_factid = MProd.prod_factid.name
    prod_name_jp = MProd.prod_name_jp.name
    prod_name_en = MProd.prod_name_en.name
    prod_name_sys = MProd.prod_name_sys.name
    prod_name_local = MProd.prod_name_local.name
    prod_name_all = 'prod_name_all'
    prod_abbr_jp = MProd.prod_abbr_jp.name
    prod_abbr_en = MProd.prod_abbr_en.name
    prod_abbr_local = MProd.prod_abbr_local.name

    # m_prod_family table
    prod_family_factid = MProdFamily.prod_family_factid.name
    prod_family_name_jp = MProdFamily.prod_family_name_jp.name
    prod_family_name_en = MProdFamily.prod_family_name_en.name
    prod_family_name_sys = MProdFamily.prod_family_name_sys.name
    prod_family_name_local = MProdFamily.prod_family_name_local.name
    prod_family_name_all = 'prod_family_name_all'
    prod_family_abbr_jp = MProdFamily.prod_family_abbr_jp.name
    prod_family_abbr_en = MProdFamily.prod_family_abbr_en.name
    prod_family_abbr_local = MProdFamily.prod_family_abbr_local.name

    # m_sect table
    dept_id = MSect.dept_id.name
    sect_factid = MSect.sect_factid.name
    sect_name_jp = MSect.sect_name_jp.name
    sect_name_en = MSect.sect_name_en.name
    sect_name_sys = MSect.sect_name_sys.name
    sect_name_local = MSect.sect_name_local.name
    sect_name_all = 'sect_name_all'
    sect_abbr_jp = MSect.sect_abbr_jp.name
    sect_abbr_en = MSect.sect_abbr_en.name
    sect_abbr_local = MSect.sect_abbr_local.name

    # m_unit table
    quantity_jp = MUnit.quantity_jp.name
    quantity_en = MUnit.quantity_en.name
    unit = MUnit.unit.name
    type = MUnit.type.name
    base = MUnit.base.name
    conversion = MUnit.conversion.name
    denominator = MUnit.denominator.name
    offset = MUnit.offset.name

    # m_location
    location_name_jp = MLocation.location_name_jp.name
    location_name_en = MLocation.location_name_en.name
    location_name_sys = MLocation.location_name_sys.name
    location_abbr = MLocation.location_abbr.name
