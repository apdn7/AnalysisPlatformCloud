from ap.common.constants import DataGroupType
from ap.common.logger import log_execution_time
from bridge.models.m_data import MData
from bridge.models.m_data_group import MDataGroup
from bridge.models.m_dept import MDept
from bridge.models.m_equip import MEquip
from bridge.models.m_equip_group import MEquipGroup
from bridge.models.m_factory import MFactory
from bridge.models.m_line import MLine
from bridge.models.m_line_group import MLineGroup
from bridge.models.m_location import MLocation
from bridge.models.m_part import MPart
from bridge.models.m_part_type import MPartType
from bridge.models.m_plant import MPlant
from bridge.models.m_process import MProcess
from bridge.models.m_prod import MProd
from bridge.models.m_prod_family import MProdFamily
from bridge.models.m_sect import MSect
from bridge.models.m_st import MSt
from bridge.models.m_unit import MUnit
from bridge.services.master_data_base_transform_pattern import extract_master
from bridge.services.master_data_efa_v2_transform_pattern import DataNameRule


@log_execution_time()
def general_extract_master(master_name, **extend_args):
    return extract_master(
        master_name,
        dict_config_general_pattern,
        dict_config_general_transform_column,
        **extend_args,
    )


dict_config_general_pattern = {  # rule
    DataGroupType.LOCATION_NAME.name: [],
    DataGroupType.LOCATION_ABBR.name: [],
    DataGroupType.FACTORY_ID.name: [],
    DataGroupType.FACTORY_NAME.name: [],
    DataGroupType.FACTORY_ABBR.name: [],
    DataGroupType.PLANT_ID.name: [],
    DataGroupType.PLANT_NAME.name: [],
    DataGroupType.PLANT_ABBR.name: [],
    DataGroupType.DEPT_ID.name: [],
    DataGroupType.DEPT_NAME.name: [],
    DataGroupType.DEPT_ABBR.name: [],
    DataGroupType.SECT_ID.name: [],
    DataGroupType.SECT_NAME.name: [],
    DataGroupType.SECT_ABBR.name: [],
    DataGroupType.PROD_FAMILY_ID.name: [],
    DataGroupType.PROD_FAMILY_NAME.name: [],
    DataGroupType.PROD_FAMILY_ABBR.name: [],
    DataGroupType.PROD_ID.name: [],
    DataGroupType.PROD_NAME.name: [],
    DataGroupType.PROD_ABBR.name: [],
    DataGroupType.LINE_ID.name: [],
    DataGroupType.LINE_NAME.name: [],
    DataGroupType.LINE_NO.name: [],
    DataGroupType.OUTSOURCE.name: [],
    DataGroupType.EQUIP_ID.name: [],
    DataGroupType.EQUIP_NAME.name: [],
    DataGroupType.EQUIP_NO.name: [],
    DataGroupType.EQUIP_PRODUCT_NO.name: [],
    DataGroupType.EQUIP_PRODUCT_DATE.name: [],
    DataGroupType.STATION_NO.name: [],
    DataGroupType.PART_NO.name: [],
    DataGroupType.PART_NO_FULL.name: [],
    DataGroupType.PART_TYPE.name: [],
    DataGroupType.PART_NAME.name: [],
    DataGroupType.PART_ABBR.name: [],
    DataGroupType.PROCESS_ID.name: [],
    DataGroupType.PROCESS_NAME.name: [],
    DataGroupType.PROCESS_ABBR.name: [],
    DataGroupType.DATA_ID.name: [],
    DataGroupType.DATA_NAME.name: [DataNameRule.pattern_regexes, DataNameRule.pattern_99],
    DataGroupType.DATA_ABBR.name: [],
    DataGroupType.UNIT.name: [],
}

dict_config_general_transform_column = {  # transform from source column to BS column
    DataGroupType.LOCATION_NAME.name: [MLocation.get_default_name_column()],
    DataGroupType.LOCATION_ABBR.name: [MLocation.Columns.location_abbr.name],
    DataGroupType.FACTORY_ID.name: [MFactory.Columns.factory_factid.name],
    DataGroupType.FACTORY_NAME.name: [MFactory.get_default_name_column()],
    DataGroupType.FACTORY_ABBR.name: [MFactory.Columns.factory_abbr_jp.name],
    DataGroupType.PLANT_ID.name: [MPlant.Columns.plant_factid.name],
    DataGroupType.PLANT_NAME.name: [MPlant.get_default_name_column()],
    DataGroupType.PLANT_ABBR.name: [MPlant.Columns.plant_abbr_jp.name],
    DataGroupType.DEPT_ID.name: [MDept.Columns.dept_factid.name],
    DataGroupType.DEPT_NAME.name: [MDept.get_default_name_column()],
    DataGroupType.DEPT_ABBR.name: [MDept.Columns.dept_abbr_jp.name],
    DataGroupType.SECT_ID.name: [MSect.Columns.sect_factid.name],
    DataGroupType.SECT_NAME.name: [MSect.get_default_name_column()],
    DataGroupType.SECT_ABBR.name: [MSect.Columns.sect_abbr_jp.name],
    DataGroupType.PROD_FAMILY_ID.name: [MProdFamily.Columns.prod_family_factid.name],
    DataGroupType.PROD_FAMILY_NAME.name: [MProdFamily.get_default_name_column()],
    DataGroupType.PROD_FAMILY_ABBR.name: [MProdFamily.Columns.prod_family_abbr_jp.name],
    DataGroupType.PROD_ID.name: [MProd.Columns.prod_factid.name],
    DataGroupType.PROD_NAME.name: [MProd.get_default_name_column()],
    DataGroupType.PROD_ABBR.name: [MProd.Columns.prod_abbr_jp.name],
    DataGroupType.LINE_ID.name: [MLine.Columns.line_factid.name],
    DataGroupType.LINE_NAME.name: [MLineGroup.get_default_name_column()],
    DataGroupType.LINE_NO.name: [MLine.Columns.line_no.name],
    DataGroupType.OUTSOURCE.name: [MLine.Columns.outsource.name],
    DataGroupType.EQUIP_ID.name: [MEquip.Columns.equip_factid.name],
    DataGroupType.EQUIP_NAME.name: [MEquipGroup.get_default_name_column()],
    DataGroupType.EQUIP_NO.name: [MEquip.Columns.equip_no.name],
    DataGroupType.EQUIP_PRODUCT_NO.name: [MEquip.Columns.equip_product_no.name],
    DataGroupType.EQUIP_PRODUCT_DATE.name: [MEquip.Columns.equip_product_date.name],
    DataGroupType.STATION_NO.name: [MSt.Columns.st_no.name],
    DataGroupType.PART_NO.name: [MPart.Columns.part_no.name],
    DataGroupType.PART_NO_FULL.name: [MPart.Columns.part_factid.name],
    DataGroupType.PART_TYPE.name: [MPartType.Columns.part_type_factid.name],
    DataGroupType.PART_NAME.name: [MPartType.get_default_name_column()],
    DataGroupType.PART_ABBR.name: [MPartType.Columns.part_abbr_jp.name],
    DataGroupType.PROCESS_ID.name: [MProcess.Columns.process_factid.name],
    DataGroupType.PROCESS_NAME.name: [MProcess.get_default_name_column()],
    DataGroupType.PROCESS_ABBR.name: [MProcess.Columns.process_abbr_jp.name],
    DataGroupType.DATA_ID.name: [MData.Columns.data_factid.name],
    DataGroupType.DATA_NAME.name: [
        MDataGroup.get_default_name_column(),
        MDataGroup.Columns.data_group_type.name,
        MUnit.Columns.unit.name,
    ],
    DataGroupType.DATA_ABBR.name: [MDataGroup.Columns.data_abbr_jp.name],
}
