import re

import pandas as pd
from pandas import DataFrame, Series

from ap.common.constants import (
    DEFAULT_EQUIP_SIGN,
    DEFAULT_LINE_SIGN,
    DEFAULT_NONE_VALUE,
    DEFAULT_ST_SIGN,
    DIRECT_STRING,
    EMPTY_STRING,
    HALF_WIDTH_SPACE,
    DataGroupType,
)
from ap.common.logger import log_execution_time
from bridge.models.m_data import MData
from bridge.models.m_data_group import MDataGroup
from bridge.models.m_equip import MEquip
from bridge.models.m_equip_group import MEquipGroup
from bridge.models.m_line import MLine
from bridge.models.m_line_group import MLineGroup
from bridge.models.m_location import MLocation
from bridge.models.m_part import MPart
from bridge.models.m_part_type import MPartType
from bridge.models.m_process import MProcess
from bridge.models.m_prod import MProd
from bridge.models.m_prod_family import MProdFamily
from bridge.models.m_st import MSt
from bridge.models.m_unit import MUnit
from bridge.models.r_factory_machine import RFactoryMachine
from bridge.services.extend_sessor_column_handler import ExtendSensorInfo
from bridge.services.master_data_base_transform_pattern import (
    REMOVE_CHARACTERS_PATTERN,
    DataNameRule,
    RegexRule,
    extract_master,
)


class LineNameRule(RegexRule):
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

    @staticmethod
    def _is_equal_(a: list, b: list):
        is_equal = True
        for a, b in zip(a, [DEFAULT_NONE_VALUE] * len(b)):
            if pd.isnull(a):
                if pd.isnull(b):
                    is_equal &= True
                else:
                    is_equal &= False
            elif pd.isnull(b):
                is_equal &= False
            else:
                is_equal &= a == b

            if not is_equal:
                break

        return is_equal

    @staticmethod
    def _get_prod_info_(
        series_condition: Series,
        m_prod_df: DataFrame,
        m_prod_family_df: DataFrame,
        prod_column_names,
        prod_family_column_names,
        is_direct_others=False,
        is_get_prod_abbr_jp=False,
    ):
        if is_direct_others:
            m_prod_target = m_prod_df[m_prod_df[MProd.Columns.prod_name_jp.name] == DIRECT_STRING]
        else:
            m_prod_target = m_prod_df[series_condition]

        if is_get_prod_abbr_jp:
            for i in range(len(prod_column_names)):
                if prod_column_names[i] == MProd.Columns.prod_name_jp.name:
                    prod_column_names[i] = MProd.Columns.prod_abbr_jp.name
        _prod_names = m_prod_target[prod_column_names].values.tolist()[0]

        m_prod_family_id = m_prod_target[MProd.Columns.prod_family_id.name].item()
        m_prod_family_target = m_prod_family_df[m_prod_family_df[MProd.Columns.prod_family_id.name] == m_prod_family_id]
        _prod_family_names = m_prod_family_target[prod_family_column_names].values.tolist()[0]
        return _prod_names, _prod_family_names

    @classmethod
    def extract_data(
        cls,
        data: str,
        regex: str,
        group: str,
        m_prod_df: DataFrame,
        m_prod_family_df: DataFrame,
        is_direct_others=False,
        is_get_prod_abbr_jp=False,
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
        prod_column_names = MProd.get_all_name_columns()
        prod_names = [DEFAULT_NONE_VALUE] * len(prod_column_names)
        prod_family_column_names = MProdFamily.get_all_name_columns()
        prod_family_names = [DEFAULT_NONE_VALUE] * len(prod_family_column_names)

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

        try:
            if LineNameRule._is_equal_(prod_names, [DEFAULT_NONE_VALUE] * len(prod_column_names)):
                series_condition = Series(m_prod_df[MProd.Columns.prod_name_jp.name].isnull())
                prod_names, prod_family_names = LineNameRule._get_prod_info_(
                    series_condition,
                    m_prod_df,
                    m_prod_family_df,
                    prod_column_names,
                    prod_family_column_names,
                    is_direct_others,
                    is_get_prod_abbr_jp,
                )
        except Exception as e:
            raise e

        return tuple([line_name_jp, line_no, line_sign, outsourcing_flag] + prod_names + prod_family_names)

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
        m_prod_df: DataFrame,
        m_prod_family_df: DataFrame,
        is_direct_others=False,
        is_get_prod_abbr_jp=False,
        **_,
    ):
        line_name_jp = cls.default_pattern(data)
        line_no = DEFAULT_NONE_VALUE
        line_sign = DEFAULT_LINE_SIGN
        outsourcing_flag = False
        prod_column_names = MProd.get_all_name_columns()
        prod_family_column_names = MProdFamily.get_all_name_columns()
        series_condition = Series(m_prod_df[MProd.Columns.prod_name_jp.name].isnull())
        prod_names, prod_family_names = LineNameRule._get_prod_info_(
            series_condition,
            m_prod_df,
            m_prod_family_df,
            prod_column_names,
            prod_family_column_names,
            is_direct_others,
            is_get_prod_abbr_jp,
        )

        return tuple([line_name_jp, line_no, line_sign, outsourcing_flag] + prod_names + prod_family_names)


class EquipNameRule(RegexRule):
    # | 子設備名                  | equip_name   	| equip_no  | st_no 	| equip_sign| st_sign  |
    # |-------------------------|------------------	|-------	|-------	|---------	|--------- |
    # | 噴形検査1号              	| 噴形検査         	| 1     	|        	| Eq     	|          |
    # | 噴形検査1号機            	| 噴形検査         	| 1     	|       	| Eq      	|          |
    # | 油密検査機1号1ST         	| 油密検査         	| 1     	| 1     	| Eq     	| St       |
    # | 動的流量調整検査2号2ST   	| 動的流量調整検査 	| 2     	| 2     	| Eq     	| St       |
    # | 動的流量調整検査2号機2ST 	| 動的流量調整検査 	| 2     	| 2     	| Eq     	| St       |
    # | 静的流量検査機           	| 静的流量検査     	|       	|       	|         	|          |

    regex_dict = {
        'pattern_1': r'^(.*[^\d機])機?(\d+)号機?()$',
        'pattern_2': r'^(.*[^\d機])機?(\d+)号機?(\d+)[sS][tT]$',
        'pattern_3': r'^(.+)()機()$',  # pattern : use 工程名 of v2 / PROCESS_NAME of eFA as process_name
        'pattern_4': r'^(.*[^\d機])機?()\s?(\d+)[sS][tT]$',
    }

    @classmethod
    def extract_data(cls, data: str, regex: str, pattern_no):
        if cls.is_not_data(data):
            return None

        data = re.sub(REMOVE_CHARACTERS_PATTERN, EMPTY_STRING, data)
        match = re.search(regex, data)
        if not match:
            return None

        equip_name_jp, equip_no, st_no = match.groups()
        if not equip_no:
            equip_no = DEFAULT_NONE_VALUE
        if not st_no:
            st_no = DEFAULT_NONE_VALUE

        equip_sign = DEFAULT_EQUIP_SIGN
        st_sign = DEFAULT_ST_SIGN

        return equip_name_jp, equip_no, st_no, equip_sign, st_sign

    @classmethod
    def pattern_99(cls, data: str):
        equip_name_jp = cls.default_pattern(data)
        equip_no = DEFAULT_NONE_VALUE
        st_no = DEFAULT_NONE_VALUE
        equip_sign = DEFAULT_EQUIP_SIGN
        st_sign = DEFAULT_ST_SIGN

        return equip_name_jp, equip_no, st_no, equip_sign, st_sign


class EquipFactIdRule(RegexRule):
    # | 子設備ID                             |    equip_factid    |    equip_st    |
    # |------------------EFA------------------------------------------------------|
    # | AS-9235                             |    ASXX-9235       |                |
    # | PCA-0036                            |    PCAX-0036       |                |
    # | IMB-4488-2                          |    IMBX-4488       |    2           |
    # | ASXX-9810                           |    ASXX-9810       |                |
    # | SMCX-51611                          |    SMCX-5161       |    1           |
    # | CTXX1212                            |    CTXX-1212       |                |
    # |------------------V2-------------------------------------------------------|
    # | 0303_DIAC5-JP-SCR02_IMB-4495        |    IMBX-4495       |                |
    # | 0303_DIAC5-JP-SCR02_IMB-4495-01     |    IMBX-4495       |    1           |
    # | MUXX3082_01                         |    MUXX-3082       |    1           |

    regex_dict = {
        'pattern_1': ('group_1', r'^([a-zA-Z]{2,4})[-_]{0,}([\d_]{4})[-_]{0,}(\d{1,2})?$'),
        'pattern_2': ('group_1', r'^.*_([a-zA-Z]{2,4})[-_]{0,}([\d_]{4})[-_]{0,}(\d{1,2})?$'),
    }

    @classmethod
    def extract_data(cls, data: str, regex: str, group: str, is_direct_others=False):
        if cls.is_not_data(data):
            return None

        data = re.sub(REMOVE_CHARACTERS_PATTERN, EMPTY_STRING, data)
        match = re.findall(regex, data)
        if not match:
            return None

        equip_factid = DEFAULT_NONE_VALUE
        equip_st = DEFAULT_NONE_VALUE
        if group == 'group_1':
            code_part, serial_part, st_part = match[0]

            # rule: if less than 4 letter, add 'X', if more than 4 letter
            code_part = code_part.ljust(4, 'X')
            serial_part = serial_part.rjust(4, '0')

            equip_factid = f'{code_part}-{serial_part}'
            if st_part:
                equip_st = int(st_part)

        return equip_factid, equip_st

    @classmethod
    def pattern_regexes(cls, data: str, **_):
        for pattern, pair in cls.regex_dict.items():
            group, regex = pair
            result = cls.extract_data(data, regex, group)
            if result is None:
                continue
            return result
        return None

    @classmethod
    def pattern_99(cls, data: str, is_direct_others=False):
        equip_factid = DIRECT_STRING if is_direct_others else cls.default_pattern(data)
        equip_st = DEFAULT_NONE_VALUE

        return equip_factid, equip_st


class PartNoRule(RegexRule):
    # | 品番                |   location_abbr_jp   |   parts_type_factid   |   品番の9→11文字  |
    # |-----------V2------------------------------------------------------------------------|
    # | JP2995003240       |   JP                 |   299500              |   324           |
    # | JP2995213081       |   JP                 |   299521              |   308           |
    # |-----------EFA-----------------------------------------------------------------------|
    # | 324                |   JP                 |   000000              |   324           |

    regex_dict = {
        'pattern_1': r'^([A-Z]{2})(\d{6})(\d{3})(\d{1})$',
        'pattern_2': r'^()()(\d{3})()$',
    }

    @classmethod
    def extract_data(cls, data: str, regex: str, pattern_no):
        if cls.is_not_data(data):
            return None

        data = re.sub(REMOVE_CHARACTERS_PATTERN, EMPTY_STRING, data)
        data = data.replace(HALF_WIDTH_SPACE, '0')

        match = re.search(regex, data)
        if not match:
            return None

        location_abbr, parts_type_factid, part_no, assy_flag = match.groups()
        if not location_abbr:
            location_abbr = 'JP'
        if not parts_type_factid:
            parts_type_factid = EMPTY_STRING.ljust(6, '0')
        if not assy_flag:
            assy_flag = '0'
        part_factid = f'{location_abbr}{parts_type_factid}{part_no}{assy_flag}'
        return part_factid, parts_type_factid, part_no, location_abbr

    @classmethod
    def pattern_99(cls, data: str):
        if not cls.is_not_data(data):
            data = re.sub(REMOVE_CHARACTERS_PATTERN, EMPTY_STRING, data)
            data = data.replace(HALF_WIDTH_SPACE, '0')
            if data.startswith('JP'):
                data = data.ljust(12, '0')
            else:
                data = data.rjust(9, '0')
                data = f'JP{data}0'
            return data, data[2:8], data[8:11], data[0:2]
        else:
            return DEFAULT_NONE_VALUE, DEFAULT_NONE_VALUE, DEFAULT_NONE_VALUE, DEFAULT_NONE_VALUE


class ProcessNameRule(RegexRule):
    regex_dict = {
        'pattern_1': r'^(.*[^\d機])機?(\d+)号機?$',  # remove 1号, 2号... from process name
        'pattern_2': r'^(.*)()機$',
    }

    @classmethod
    def extract_data(cls, data: str, regex: str, pattern_no):
        if cls.is_not_data(data):
            return None

        data = re.sub(REMOVE_CHARACTERS_PATTERN, EMPTY_STRING, data)
        match = re.search(regex, data)
        if not match:
            return None

        process_name, number = match.groups()
        return process_name

    @classmethod
    def pattern_99(cls, data: str):
        process_name = cls.default_pattern(data)
        return process_name


class DataIdRule(RegexRule):
    regex_dict = {
        'pattern_1': r'.*(.{5})$',  # get last 5 characters
    }

    @classmethod
    def extract_data(cls, data: str, regex: str, pattern_no, is_efa_history=False):
        if cls.is_not_data(data):
            return None

        if not is_efa_history:
            return data

        data = re.sub(REMOVE_CHARACTERS_PATTERN, EMPTY_STRING, data)
        if ExtendSensorInfo.ProdName.is_equal(data):  # Ignore extract data_id for __PROD_NAME__ column
            return DEFAULT_NONE_VALUE

        match = re.search(regex, data)
        if not match:
            return None

        data_factid, *_ = match.groups()
        return data_factid

    @classmethod
    def pattern_99(cls, data: str):
        data_factid = cls.default_pattern(data)
        return DEFAULT_NONE_VALUE if ExtendSensorInfo.ProdName.is_equal(data_factid) is not None else data_factid


@log_execution_time()
def efa_v2_extract_master(master_name, **extend_args):
    return extract_master(master_name, dict_config_efa_v2_pattern, dict_config_efa_v2_transform_column, **extend_args)


dict_config_efa_v2_pattern = {  # rule
    DataGroupType.LINE_NAME.name: [LineNameRule.pattern_regexes, LineNameRule.pattern_99],
    DataGroupType.PROCESS_NAME.name: [ProcessNameRule.pattern_regexes, ProcessNameRule.pattern_99],
    DataGroupType.EQUIP_NAME.name: [EquipNameRule.pattern_regexes, EquipNameRule.pattern_99],
    DataGroupType.EQUIP_ID.name: [EquipFactIdRule.pattern_regexes, EquipFactIdRule.pattern_99],
    DataGroupType.PART_NO.name: [PartNoRule.pattern_regexes, PartNoRule.pattern_99],
    DataGroupType.DATA_NAME.name: [DataNameRule.pattern_regexes, DataNameRule.pattern_99],
    DataGroupType.DATA_ID.name: [DataIdRule.pattern_regexes, DataIdRule.pattern_99],
}

dict_config_efa_v2_transform_column = {  # transform from source column to BS column
    DataGroupType.LINE_NAME.name: [
        MLineGroup.get_default_name_column(),
        MLine.Columns.line_no.name,
        MLine.Columns.line_sign.name,
        MLine.Columns.outsourcing_flag.name,
        *MProd.get_all_name_columns(),
        *MProdFamily.get_all_name_columns(),
    ],
    DataGroupType.PROCESS_NAME.name: [MProcess.get_default_name_column()],
    DataGroupType.EQUIP_NAME.name: [
        MEquipGroup.get_default_name_column(),
        MEquip.Columns.equip_no.name,
        MSt.Columns.st_no.name,
        MEquip.Columns.equip_sign.name,
        MSt.Columns.st_sign.name,
    ],
    DataGroupType.EQUIP_ID.name: [
        MEquip.Columns.equip_factid.name,
        RFactoryMachine.Columns.equip_st.name,
    ],
    DataGroupType.PART_NO.name: [
        MPart.Columns.part_factid.name,
        MPartType.Columns.part_type_factid.name,
        MPart.Columns.part_no.name,
        MLocation.Columns.location_abbr.name,
    ],
    DataGroupType.DATA_NAME.name: [
        MDataGroup.get_default_name_column(),
        MDataGroup.Columns.data_group_type.name,
        MUnit.Columns.unit.name,
    ],
    DataGroupType.DATA_ID.name: [MData.Columns.data_factid.name],
}
