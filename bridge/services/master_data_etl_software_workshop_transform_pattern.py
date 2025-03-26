import re

from ap.common.constants import DEFAULT_LINE_SIGN, DEFAULT_NONE_VALUE, EMPTY_STRING, DataGroupType
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
from bridge.services.master_data_base_transform_pattern import (
    REMOVE_CHARACTERS_PATTERN,
    DataNameRule,
    RegexRule,
    extract_master,
)


class SoftwareWorkshopLineNameRule(RegexRule):
    """
    | ライン名                        	| line_name      	| line_no 	| line_sign | line_abbr_jp 	| outsourcing_flag |
    |-----------------------------------|----------------	|---------	|---------- |-----------	|------------      |
    | 調整検査6号                     	| 調整検査            | 6      	| L         | L6        	| FALSE            |
    | 直噴INJ ボデーCKD1号ライン      	    | ボデー         	| 1       	| L         | L1        	| TRUE             |
    | 直噴INJ ボデー研削 前加工ライン 	    | 前加工         	| 1       	| L         | L1        	| FALSE            |
    | 直噴INJ4号 バルブASSY組付ライン 	    | バルブASSY組付  	| 4       	| L         | L4        	| FALSE            |
    """  # noqa: E501

    regex_dict = {
        'pattern_1.1': ('group_0', r'.*(GD-P4|P4H)\s(.*\D)(\d*)号ライン$'),
        'pattern_1.2': ('group_0', r'.*(GD-P4|P4H)\s(.*)()ライン$'),
        'pattern_1.3': ('group_0', r'.*(GD-P4|P4H)\s(.*)()$'),
        'pattern_2': ('group_1', r'\s(.+\D)()(\d+)号ライン$'),
        'pattern_3': ('group_1', r'\s(.+\D)()(\d+)号$'),
        'pattern_6': ('group_1', r'\s(.+)(CKD)(\d+)号ライン$'),
        'pattern_4': ('group_2', r'(\d+)号\s(.+)ライン$'),
        'pattern_5': ('group_2', r'()\s(.+)ライン$'),
        'pattern_7': ('group_1', r'(.+\D)()(\d+)号$'),
        'pattern_8': ('group_1', r'(.+)(CKD)(\d+)号ライン$'),
    }

    @classmethod
    def extract_data(
        cls,
        data: str,
        regex: str,
        group: str,
    ):
        if cls.is_not_data(data):
            return None

        data = re.sub(REMOVE_CHARACTERS_PATTERN, EMPTY_STRING, data)
        match = re.search(regex, data)
        if not match:
            return None

        line_name_jp, line_no, outsourcing_flag = (
            DEFAULT_NONE_VALUE,
            DEFAULT_NONE_VALUE,
            DEFAULT_NONE_VALUE,
        )

        if group == 'group_0':
            return None

        elif group == 'group_1':
            line_name_jp, ckd, line_no = match.groups()
            outsourcing_flag = bool(ckd)

        elif group == 'group_2':
            line_no, line_name_jp = match.groups()
            outsourcing_flag = False

        line_name_jp = line_name_jp.strip()
        if not line_no:
            line_no = DEFAULT_NONE_VALUE

        line_sign = DEFAULT_LINE_SIGN

        return line_name_jp, line_no, line_sign, outsourcing_flag

    @classmethod
    def pattern_regexes(cls, data: str, **extend_args):
        for pattern, pair in cls.regex_dict.items():
            group, regex = pair
            result = cls.extract_data(data, regex, group, **extend_args)
            if result is None:
                continue
            return result
        return None

    @classmethod
    def pattern_99(
        cls,
        data: str,
        **_,
    ):
        line_name_jp = cls.default_pattern(data)
        line_no = DEFAULT_NONE_VALUE
        line_sign = DEFAULT_LINE_SIGN
        outsourcing_flag = False

        return line_name_jp, line_no, line_sign, outsourcing_flag


class SoftwareWorkshopUnitRule(RegexRule):
    @classmethod
    def pattern_99(cls, data: str, **_):
        unit = (
            re.sub(REMOVE_CHARACTERS_PATTERN, EMPTY_STRING, data).strip()
            if not cls.is_not_data(data)
            else DEFAULT_NONE_VALUE
        )
        return unit


@log_execution_time()
def etl_software_workshop_extract_master(master_name, **extend_args):
    return extract_master(
        master_name,
        dict_config_etl_software_workshop_pattern,
        dict_config_etl_software_workshop_transform_column,
        **extend_args,
    )


dict_config_etl_software_workshop_pattern = {  # rule
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
    DataGroupType.LINE_NAME.name: [
        SoftwareWorkshopLineNameRule.pattern_regexes,
        SoftwareWorkshopLineNameRule.pattern_99,
    ],
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
    DataGroupType.DATA_NAME.name: [
        DataNameRule.pattern_regexes,
        DataNameRule.pattern_99,
    ],
    DataGroupType.DATA_ABBR.name: [],
    DataGroupType.UNIT.name: [SoftwareWorkshopUnitRule.pattern_99],
}

dict_config_etl_software_workshop_transform_column = {  # transform from source column to BS column
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
    DataGroupType.LINE_NAME.name: [
        MLineGroup.get_default_name_column(),
        MLine.Columns.line_no.name,
        MLine.Columns.line_sign.name,
        MLine.Columns.outsourcing_flag.name,
    ],
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
    DataGroupType.UNIT.name: [MUnit.Columns.unit.name],
}
