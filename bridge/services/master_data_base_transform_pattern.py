import re
from typing import Any, Union

import pandas as pd
from pandas._libs.missing import NAType

from ap import log_execution_time
from ap.common.constants import DEFAULT_NONE_VALUE, EMPTY_STRING, HALF_WIDTH_SPACE, DataGroupType

REMOVE_CHARACTERS_PATTERN = r'\\+[nrt]'


class RegexRule:
    regex_dict = None

    @classmethod
    def extract_data(cls, *args, **kwargs) -> Union[tuple, str, None, NAType]:
        raise NotImplementedError('Method not implemented!')

    @classmethod
    def pattern_regexes(cls, data: str, **extend_args) -> Union[tuple, str, None, NAType]:
        for pattern_no, regex in cls.regex_dict.items():
            result = cls.extract_data(data, regex, pattern_no, **extend_args)
            if result is None:
                continue
            return result
        return None

    @classmethod
    def is_not_data(cls, data: str) -> bool:
        return pd.isnull(data) or data is None

    @classmethod
    def default_pattern(cls, data: Any, **_) -> str:
        if cls.is_not_data(data):
            return DEFAULT_NONE_VALUE

        if not isinstance(data, str):
            return data
        return re.sub(REMOVE_CHARACTERS_PATTERN, EMPTY_STRING, data)


@log_execution_time()
def extract_master(master_name: str, dict_config_pattern: dict, dict_config_transform_column: dict, **extend_args):
    patterns = dict_config_pattern.get(master_name, None)  # type: list[RegexRule.pattern_regexes]
    if not patterns:
        patterns = [RegexRule.default_pattern]

    def inner(name: str) -> Union[tuple, list]:
        for pattern in patterns:
            ret = pattern(name, **extend_args)
            if ret is not None:
                return ret
        expand_columns = dict_config_transform_column.get(master_name, None)
        default_val: list = [DEFAULT_NONE_VALUE] * len(expand_columns)
        default_val[0] = name if master_name != DataGroupType.DATA_ID.name else DEFAULT_NONE_VALUE

        return default_val

    return inner


class DataNameRule(RegexRule):
    # V2					                                eFA
    # 計測項目ID	          計測項目名				            DATA_ID	 DATA_NAME
    # LNSCXX4302_M00022	  △q/△SP荷重 [mm3/str/N]				DIC14028	 7孔Z軸補正量指令値
    # LNSCXX4302_M00042	  △q/△SP荷重 [mm3/str/N]				DIC14128	 7孔Z軸補正量指令値

    # In abnormal case
    # No    計測項目名	                              data name                     unit
    # ①    AJP圧入荷重[N](最終荷重)                ->   AJP圧入荷重 最終荷重              N
    # ②    SPバネ定数[N]]                         ->  SPバネ定数                        N
    #      切込み終了時荷重（ﾛｰﾗ①実測）[N            ->  切込み終了時荷重（ﾛｰﾗ①実測）         N
    # ③    ﾀｰﾐﾅﾙ位置寸法 Ｌ側 [m m ]               ->  ﾀｰﾐﾅﾙ位置寸法 Ｌ側                  m m
    # ④    調整荷重（調整終了点）（ﾛｰﾗ②実測）[N]     ->  調整荷重（調整終了点）（ﾛｰﾗ②実測）      N
    # ④    AJP圧入荷重[N][mm](最終荷重)             ->  AJP圧入荷重 最終荷重           N

    regex_dict = {
        'pattern_1': r'^([^\[\]]*)[\[\s]*([^\[\]]*)[\]\s]*([^\[\]]*)$',
        'pattern_2': r'^([^\[\]]+)[\[\s]*([^\[\]]*)[\]\s]*[\[\s]*([^\[\]]*)[\]\s]*([^\[\]]*)$',
    }

    @classmethod
    def extract_data(cls, data: str, regex: str, pattern_no, units: list[str]):
        if cls.is_not_data(data):
            return None

        data = re.sub(r'\\+[nrt]|\(?±\)?', EMPTY_STRING, data)
        match = re.search(regex, data)
        if not match:
            return None
        if pattern_no == 'pattern_1':
            data_name_jp, unit, suffix_data_name = match.groups()
        else:
            data_name_jp, unit, _, suffix_data_name = match.groups()
        data_name_jp = data_name_jp.strip()
        if len(suffix_data_name) != 0:
            data_name_jp += f'{HALF_WIDTH_SPACE}{suffix_data_name}'

        # 4. Replace any bracket.
        # 4-1. Replace "\s?[(\[]\s?" to "("
        data_name_jp = re.sub(r'\s?[(\[【「『〖〚〘｟〔]\s?', '(', data_name_jp)
        # 4-2. Replace "\s?[)\]" to ")"
        data_name_jp = re.sub(r'\s?[)\]】」』〗〛〙｠〕]\s?', ')', data_name_jp)

        # handle case 'μm' and 'µm'
        unit = unit.replace('μ', 'µ').replace('KPa', 'kPa').replace('cm^3/min', 'cm3/min').strip()
        if len(unit) == 0:
            # 3.1. In case cannot detect unit, try to compare string in () with units in DB.
            unit = DEFAULT_NONE_VALUE

            assume_unit_origins = [
                assume_unit.strip() if assume_unit else EMPTY_STRING
                for assume_unit in re.findall(r'\((.*?)\)', data_name_jp)
            ]

            for assume_unit_origin in assume_unit_origins:
                assume_unit = assume_unit_origin.replace('μ', 'µ').replace('KPa', 'kPa').replace('cm^3/min', 'cm3/min')
                if len(assume_unit) != 0 and assume_unit in units:
                    unit = assume_unit
                    # 3.2. Detected `unit`. If so, replace "(`unit`)" to ""
                    data_name_jp = re.sub(rf'\s?\(\s?{assume_unit_origin}\s?\)\s?', EMPTY_STRING, data_name_jp)
                    break

        # 4-3. Remove "()"
        data_name_jp = re.sub(r'[()]', HALF_WIDTH_SPACE, data_name_jp)
        data_name_jp = re.sub(r'\s+', HALF_WIDTH_SPACE, data_name_jp)

        # 5. Replace `No.`=> `No` by removing `.`
        data_name_jp = data_name_jp.replace('No.', 'No')
        data_name_jp = data_name_jp.replace(';', ':')  # Cover case 管理マスタ値1;指示値 & 管理マスタ値1:指示値

        # Remove 計測値:|measurement.
        measurement_removes = [
            '計測値:',
            '加工値:',
            '加工条件:',
            '加工条件値:',
            'その他:',
            'measurement.',
            '測定値:',
            'OK/NG情報:',
        ]
        data_name_jp = re.sub('|'.join(map(re.escape, measurement_removes)), EMPTY_STRING, data_name_jp)

        # 6. Strip
        data_name_jp = data_name_jp.strip()

        group_type = DataGroupType.GENERATED.value  # temp
        return data_name_jp, group_type, unit

    @classmethod
    def pattern_99(cls, data: str, **_):
        data_name_jp = (
            re.sub(r'\\+[nrt]|\(?±\)?', EMPTY_STRING, data) if not cls.is_not_data(data) else DEFAULT_NONE_VALUE
        )
        group_type = DataGroupType.GENERATED.value  # temp
        unit = DEFAULT_NONE_VALUE
        return data_name_jp, group_type, unit
