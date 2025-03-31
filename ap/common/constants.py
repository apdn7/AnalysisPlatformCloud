from __future__ import annotations

import os.path
from enum import Enum, auto
from typing import Optional

import numpy as np
import pandas as pd
from dateutil import tz

VALUE_COUNT_COL = '__count__'
DEFAULT_POSTGRES_SCHEMA = 'public'

MATCHED_FILTER_IDS = 'matched_filter_ids'
UNMATCHED_FILTER_IDS = 'unmatched_filter_ids'
NOT_EXACT_MATCH_FILTER_IDS = 'not_exact_match_filter_ids'
STRING_COL_IDS = 'string_col_ids'
DIC_STR_COLS = 'dic_str_cols'
SAMPLE_DATA = 'sample_data'
VAR_X = 'X'
VAR_Y = 'Y'
MULTIPLE_VALUES_CONNECTOR = '|'

SQL_IN_MAX = 900
FEATHER_MAX_RECORD = 5_000_000
DATABASE_LOGIN_TIMEOUT = 3  # seconds
VAR_X = 'X'
VAR_Y = 'Y'
DEFAULT_NONE_VALUE = pd.NA

# scheduler process size
SCHEDULER_PROCESS_POOL_SIZE = 5

# V2 and EFA import future from time
IMPORT_FUTURE_MONTH_AGO = 3

__NO_NAME__ = '__NO_NAME__'
PREFIX_TABLE_NAME = 't_process_'
MAX_NAME_LENGTH = 50
LIMIT_LEN_BS_COL_NAME = 15
SQL_COL_PREFIX = '__'
SQL_IN_MAX = 900
SQL_LIMIT = 5_000_000
# SQL_LIMIT = 1_000_000
SIMULATE_GLOBAL_ID_SQL_LIMIT = 500_000
ACTUAL_RECORD_NUMBER = 'actual_record_number'
ACTUAL_RECORD_NUMBER_TRAIN = 'actual_record_number_train'
ACTUAL_RECORD_NUMBER_TEST = 'actual_record_number_test'
REMOVED_OUTLIER_NAN_TRAIN = 'removed_outlier_nan_train'
REMOVED_OUTLIER_NAN_TEST = 'removed_outlier_nan_test'
CAST_INF_VALS = 'cast_inf_vals'

YAML_CONFIG_BASIC = 'basic'
YAML_START_UP = 'start_up'
YAML_CONFIG_DB = 'db'
YAML_CONFIG_PROC = 'proc'
YAML_CONFIG_AP = 'ap'
YAML_CONFIG_VERSION = 'version'
YAML_TILE_INTERFACE_DN7 = 'ti_dn7'
YAML_TILE_INTERFACE_AP = 'ti_analysis_platform'
TILE_RESOURCE_URL = '/ap/tile_interface/resources/'
DB_BACKUP_SUFFIX = '_old'
NORMAL_MODE_MAX_RECORD = 10000
MAX_COL_IN_TILES = 4
MAX_COL_IN_USAGE = 3

DEFAULT_WARNING_DISK_USAGE = 80
DEFAULT_ERROR_DISK_USAGE = 90

TRACING_KEY_DELIMITER_SYMBOL = '___'
SHOW_GRAPH_TEMP_TABLE_NAME = 't'
SHOW_GRAPH_TEMP_TABLE_COL = 'cycle_id'
NEXT_LINK_VAL_COL = 'next_link_value'

DATETIME_DUMMY = 'DatetimeDummy'
MAX_DATETIME_STEP_PER_DAY = 8640  # 10s/step -> 6steps * 60min * 24hrs

RESAMPLING_SIZE = 10_000

NROWS_FOR_SCAN_FILES = 10  # only needs 10 rows for scan file
PREVIEW_DATA_RECORDS = 10

LOG_LEVEL = 'log_level'

CSV_INDEX_COL = '__CSV_IDX__'
CSV_HORIZONTAL_ROW_INDEX_COL = '__CSV_HORIZONTAL_ROW_IDX__'

# fiscal year start month
FISCAL_YEAR_START_MONTH = 4

MAX_SAFE_INTEGER = 9007199254740991
DELIMITER_KW = 'sep'
ENCODING_KW = 'encoding'

MSP_CONTOUR_ADJUST = 0.4227
MSP_AS_HEATMAP_FROM = 10
AS_HEATMAP_MATRIX = 'as_heatmap_matrix'
HEATMAP_MATRIX = 'heatmap_matrix'
MAX_INT_CAT_VALUE = 128
COLORS_ENCODE = 'colors_encode'
FULL_DIV = 'full_div'

DUMMY_V2_PROCESS_NAME = 'DUMMY_V2_PROCESS_NAME'
MIN_DATETIME_LEN = 10

DATA_NAME_V2_SUFFIX = '01'
CONSTRAINT_RANGE = 'constraint_range'
SELECTED = 'selected'

SHUTDOWN_APP_TIMEOUT = 7200  # seconds
SHUTDOWN_APP_WAITING_INTERVAL = 5  # seconds

IDLE_MONITORING_INTERVAL = 5 * 60  # 5 minutes


class ApLogLevel(Enum):
    DEBUG = auto()
    INFO = auto()


INT16_MAX = np.iinfo(np.int16).max
INT16_MIN = np.iinfo(np.int16).min
INT32_MAX = np.iinfo(np.int32).max
INT32_MIN = np.iinfo(np.int32).min
INT64_MAX = np.iinfo(np.int64).max
INT64_MIN = np.iinfo(np.int64).min

NUMBER_OF_FILES_IN_A_SUBMISSION = 10


class BaseEnum(Enum):
    def __str__(self):
        return self.name

    @classmethod
    def get_items(cls):
        return tuple(cls.__members__.items())

    @classmethod
    def get_keys(cls):
        return tuple(cls.__members__.keys())

    @classmethod
    def get_values(cls):
        return tuple(cls.__members__.values())

    @classmethod
    def get_key(cls, value, default_key=None):
        for _key, _value in cls.__members__.items():
            if value == _value:
                return _key

        return default_key

    @classmethod
    def get_by_name(cls, name, default=None):
        for _key, _value in cls.__members__.items():
            if _key == name:
                return _key, _value

        return default

    @classmethod
    def get_by_enum_value(cls, enum_value, default=None):
        try:
            return cls(enum_value)
        except ValueError:
            return default

    @classmethod
    def to_dict(cls):
        return {i.name: i.value for i in cls}


class Environment(BaseEnum):
    DEV = auto()
    TEST = auto()
    PRODUCT = auto()


class DataType(BaseEnum):
    NULL = ''
    INTEGER = 'INTEGER'
    REAL = 'REAL'
    TEXT = 'TEXT'
    DATETIME = 'DATETIME'
    BOOLEAN = 'BOOLEAN'
    REAL_SEP = 'REAL_SEP'
    INTEGER_SEP = 'INTEGER_SEP'
    EU_REAL_SEP = 'EU_REAL_SEP'
    EU_INTEGER_SEP = 'EU_INTEGER_SEP'
    K_SEP_NULL = 'K_SEP_NULL'
    BIG_INT = 'BIG_INT'
    DATE = 'DATE'
    TIME = 'TIME'
    SMALL_INT = 'SMALL_INT'
    CATEGORY = 'CATEGORY'


class RawDataTypeDB(BaseEnum):  # data type user select
    NULL = DataType.NULL.value
    INTEGER = DataType.INTEGER.value
    REAL = DataType.REAL.value
    TEXT = DataType.TEXT.value
    DATETIME = DataType.DATETIME.value
    BOOLEAN = DataType.BOOLEAN.value
    DATE = DataType.DATE.value
    TIME = DataType.TIME.value

    SMALL_INT = DataType.SMALL_INT.value
    BIG_INT = DataType.BIG_INT.value
    CATEGORY = DataType.CATEGORY.value

    def __repr__(self) -> str:
        return self.value

    def __str__(self):
        return self.value

    @staticmethod
    def is_integer_data_type(data_type_db_value: str):
        return data_type_db_value in (
            RawDataTypeDB.INTEGER.value,
            RawDataTypeDB.SMALL_INT.value,
            RawDataTypeDB.BIG_INT.value,
        )

    @staticmethod
    def is_text_data_type(data_type_db_value: str):
        return data_type_db_value in (
            RawDataTypeDB.TEXT.value,
            RawDataTypeDB.CATEGORY.value,
        )

    @staticmethod
    def is_category_data_type(data_type_db_value: str):
        return data_type_db_value == RawDataTypeDB.CATEGORY.value

    @classmethod
    def get_pandas_dtype(cls, value: str, local_time: bool = True) -> pd.ExtensionDtype:
        if value is None:
            return object

        timezone = tz.tzlocal() if local_time else tz.tzutc()

        raw_data_type = RawDataTypeDB(value)
        if raw_data_type == RawDataTypeDB.NULL:
            raise ValueError

        if raw_data_type == RawDataTypeDB.INTEGER:
            return pd.Int32Dtype()

        if raw_data_type == RawDataTypeDB.REAL:
            return pd.Float64Dtype()

        if raw_data_type == RawDataTypeDB.TEXT:
            return pd.StringDtype()

        if raw_data_type == RawDataTypeDB.DATETIME:
            return pd.DatetimeTZDtype(tz=timezone)

        if raw_data_type == RawDataTypeDB.BOOLEAN:
            return pd.BooleanDtype()

        if raw_data_type == RawDataTypeDB.SMALL_INT:
            return pd.Int16Dtype()

        if raw_data_type == RawDataTypeDB.BIG_INT:
            return pd.Int64Dtype()

        if raw_data_type == RawDataTypeDB.CATEGORY:
            return pd.StringDtype()

        if raw_data_type == RawDataTypeDB.DATE:
            return pd.DatetimeTZDtype(tz=timezone)

        if raw_data_type == RawDataTypeDB.TIME:
            return pd.DatetimeTZDtype(tz=timezone)

        raise NotImplementedError('Invalid data type')

    @classmethod
    def get_data_type_for_function(cls, data_type: str | None, possible_data_types: list[str]) -> RawDataTypeDB | None:
        if data_type is None:
            return None

        # database only save `i` for integer, need to handle big int and small int as well
        if cls.is_integer_data_type(data_type) and cls.INTEGER.value in possible_data_types:
            return RawDataTypeDB.get_by_enum_value(data_type)

        # handle category as a text at the moment
        if data_type == cls.CATEGORY.value and cls.TEXT.value in possible_data_types:
            return RawDataTypeDB.get_by_enum_value(cls.TEXT.value)

        if data_type in possible_data_types:
            return RawDataTypeDB.get_by_enum_value(data_type)

        return None

    @classmethod
    def convert_raw_data_type_to_data_type(cls, raw_data_type_value: str) -> str | None:
        raw_data_type: RawDataTypeDB = RawDataTypeDB.get_by_enum_value(raw_data_type_value)
        if raw_data_type is None:
            return None

        data_type: DataType = DataType.get_by_name(raw_data_type.name)
        return data_type[1].value if data_type else None


class FunctionCastDataType(BaseEnum):
    SAME_AS_X = 'x'
    CAST = 'cast'


dict_convert_raw_data_type = {
    RawDataTypeDB.BOOLEAN.value: DataType.TEXT.value,
    RawDataTypeDB.CATEGORY.value: DataType.TEXT.value,
    RawDataTypeDB.SMALL_INT.value: DataType.INTEGER.value,
    RawDataTypeDB.BIG_INT.value: DataType.TEXT.value,
}

dict_data_type_db = {
    RawDataTypeDB.INTEGER.value: 'integer',
    RawDataTypeDB.REAL.value: 'real',
    RawDataTypeDB.TEXT.value: 'text',
    RawDataTypeDB.BOOLEAN.value: 'boolean',
    RawDataTypeDB.DATETIME.value: 'timestamp',
    RawDataTypeDB.SMALL_INT.value: 'smallint',
    RawDataTypeDB.BIG_INT.value: 'bigint',
    RawDataTypeDB.CATEGORY.value: 'smallint',
    RawDataTypeDB.DATE.value: 'date',
    RawDataTypeDB.TIME.value: 'time',
}

dict_invalid_data_type_regex = {
    RawDataTypeDB.INTEGER.value: r'[^-0-9]+',
    RawDataTypeDB.REAL.value: r'[^-0-9\.eg-]+',
    RawDataTypeDB.TEXT.value: '.*',
    RawDataTypeDB.BOOLEAN.value: '[^0-1]',
    RawDataTypeDB.DATETIME.value: '.*',
    RawDataTypeDB.SMALL_INT.value: r'[^-0-9]+',
    RawDataTypeDB.BIG_INT.value: r'[^-0-9]+',
    RawDataTypeDB.CATEGORY.value: '.*',
    RawDataTypeDB.DATE.value: '.*',
    RawDataTypeDB.TIME.value: '.*',
}

dict_numeric_type_ranges = {
    RawDataTypeDB.INTEGER.value: {'max': INT32_MAX, 'min': INT32_MIN},
    RawDataTypeDB.BOOLEAN.value: {'max': 1, 'min': 0},
    RawDataTypeDB.SMALL_INT.value: {'max': INT16_MAX, 'min': INT16_MIN},
    RawDataTypeDB.BIG_INT.value: {'max': INT64_MAX, 'min': INT64_MIN},
}


class FilterFunc(Enum):
    MATCHES = auto()
    ENDSWITH = auto()
    STARTSWITH = auto()
    CONTAINS = auto()
    REGEX = auto()
    SUBSTRING = auto()
    OR_SEARCH = auto()
    AND_SEARCH = auto()


class CsvDelimiter(Enum):
    CSV = ','
    TSV = '\t'
    DOT = '.'
    SMC = ';'
    Auto = None


class DirectoryNo(Enum):
    ROOT_DIRECTORY = 1
    SECOND_DIRECTORY = 2


class DBType(Enum):
    POSTGRESQL = 'postgresql'
    MSSQLSERVER = 'mssqlserver'
    SQLITE = 'sqlite'
    ORACLE = 'oracle'
    MYSQL = 'mysql'
    CSV = 'csv'
    V2 = 'v2'
    V2_MULTI = 'v2_multi'
    V2_HISTORY = 'v2_history'
    SOFTWARE_WORKSHOP = 'software_workshop'

    @classmethod
    def from_str(cls, s: str) -> Optional['DBType']:
        for e in DBType:
            if s == e.name:
                return e

    def is_db(self):
        return self in [
            DBType.POSTGRESQL,
            DBType.MSSQLSERVER,
            DBType.SQLITE,
            DBType.ORACLE,
            DBType.MYSQL,
            DBType.SOFTWARE_WORKSHOP,
        ]


class ErrorMsg(Enum):
    W_PCA_INTEGER = auto()
    E_PCA_NON_NUMERIC = auto()

    E_ALL_NA = auto()
    E_ZERO_VARIANCE = auto()
    E_EMPTY_DF = auto()


class ServerType(BaseEnum):
    EdgeServer = 1
    StandAlone = 2
    BridgeStationWeb = 3  # this number will become heading id number.
    BridgeStationGrpc = 4
    IntegrationServer = 5

    def is_postgres_db(self):
        if self in (
            ServerType.BridgeStationWeb,
            ServerType.BridgeStationGrpc,
            ServerType.StandAlone,
            ServerType.EdgeServer,
            ServerType.IntegrationServer,
        ):
            return True
        return False


ID_STR = 'id'

# YAML Keywords
YAML_INFO = 'info'
YAML_R_PATH = 'r-path'
YAML_PROC = 'proc'
YAML_SQL = 'sql'
YAML_FROM = 'from'
YAML_SELECT_OTHER_VALUES = 'select-other-values'
YAML_MASTER_NAME = 'master-name'
YAML_WHERE_OTHER_VALUES = 'where-other-values'
YAML_FILTER_TIME = 'filter-time'
YAML_FILTER_LINE_MACHINE_ID = 'filter-line-machine-id'
YAML_MACHINE_ID = 'machine-id'
YAML_DATE_COL = 'date-column'
YAML_AUTO_INCREMENT_COL = 'auto_increment_column'
YAML_SERIAL_COL = 'serial-column'
YAML_SELECT_PREFIX = 'select-prefix'
YAML_CHECKED_COLS = 'checked-columns'
YAML_COL_NAMES = 'column-names'
YAML_DATA_TYPES = 'data-types'
YAML_ALIASES = 'alias-names'
YAML_MASTER_NAMES = 'master-names'
YAML_OPERATORS = 'operators'
YAML_COEFS = 'coefs'
YAML_COL_NAME = 'column_name'
YAML_ORIG_COL_NAME = 'column_name'
YAML_VALUE_LIST = 'value_list'
YAML_VALUE_MASTER = 'value_masters'
YAML_SQL_STATEMENTS = 'sql_statements'
YAML_TRACE = 'trace'
YAML_TRACE_BACK = 'back'
YAML_TRACE_FORWARD = 'forward'
YAML_CHART_INFO = 'chart-info'
YAML_DEFAULT = 'default'
YAML_THRESH_H = 'thresh_high'
YAML_THRESH_L = 'thresh_low'
YAML_Y_MAX = 'y_max'
YAML_Y_MIN = 'y_min'
YAML_TRACE_SELF_COLS = 'self-alias-columns'
YAML_TRACE_TARGET_COLS = 'target-orig-columns'
YAML_TRACE_MATCH_SELF = 'self-substr'
YAML_TRACE_MATCH_TARGET = 'target-substr'
YAML_DB = 'db'
YAML_UNIVERSAL_DB = 'universal_db'
YAML_PROC_ID = 'proc_id'
# YAML_ETL_FUNC = 'etl_func'
YAML_PASSWORD = 'password'
YAML_HASHED = 'hashed'
# YAML_DELIMITER = 'delimiter'

# JSON Keywords
GET02_VALS_SELECT = 'GET02_VALS_SELECT'
ARRAY_FORMVAL = 'ARRAY_FORMVAL'
ARRAY_PLOTDATA = 'array_plotdata'
SERIAL_DATA = 'serial_data'
SERIAL_COLUMNS = 'serial_columns'
COMMON_INFO = 'common_info'
DATETIME_COL = 'datetime_col'
CYCLE_IDS = 'cycle_ids'
CYCLE_ID = 'cycle_id'
ARRAY_Y = 'array_y'
ARRAY_Z = 'array_z'
ORIG_ARRAY_Z = 'orig_array_z'
ARRAY_Y_MIN = 'array_y_min'
ARRAY_Y_MAX = 'array_y_max'
ARRAY_Y_TYPE = 'array_y_type'
SLOT_FROM = 'slot_from'
SLOT_TO = 'slot_to'
SLOT_COUNT = 'slot_count'
# IQR = 'iqr'
ARRAY_X = 'array_x'
Y_MAX = 'y-max'
Y_MIN = 'y-min'
# Y_MAX_ORG = 'y_max_org'
# Y_MIN_ORG = 'y_min_org'
# TIME_RANGE = 'time_range'
# TOTAL = 'total'
EMD_TYPE = 'emdType'
DUPLICATE_SERIAL_SHOW = 'duplicated_serial'
DUPLICATED_SERIALS_COUNT = 'dup_check'

UNLINKED_IDXS = 'unlinked_idxs'
NONE_IDXS = 'none_idxs'
ORG_NONE_IDXS = 'org_none_idxs'
INF_IDXS = 'inf_idxs'
NEG_INF_IDXS = 'neg_inf_idxs'
UPPER_OUTLIER_IDXS = 'upper_outlier_idxs'
LOWER_OUTLIER_IDXS = 'lower_outlier_idxs'

SCALE_SETTING = 'scale_setting'
SCALE_THRESHOLD = 'scale_threshold'
SCALE_AUTO = 'scale_auto'
SCALE_COMMON = 'scale_common'
SCALE_FULL = 'scale_full'
KDE_DATA = 'kde_data'
SCALE_Y = 'scale_y'
SCALE_X = 'scale_x'
SCALE_COLOR = 'scale_color'

CHART_INFOS = 'chart_infos'
CHART_INFOS_ORG = 'chart_infos_org'
COMMON = 'COMMON'
SELECT_ALL = 'All'
NO_FILTER = 'NO_FILTER'
START_PROC = 'start_proc'
START_DATE = 'START_DATE'
START_TM = 'START_TIME'
START_DT = 'start_dt'
COND_PROCS = 'cond_procs'
COND_PROC = 'cond_proc'
END_PROC = 'end_proc'
END_DATE = 'END_DATE'
END_TM = 'END_TIME'
END_DT = 'end_dt'
IS_REMOVE_OUTLIER = 'remove_outlier'
REMOVE_OUTLIER_OBJECTIVE_VAR = 'remove_outlier_objective_var'
REMOVE_OUTLIER_EXPLANATORY_VAR = 'remove_outlier_explanatory_var'
REMOVE_OUTLIER_TYPE = 'remove_outlier_type'
REMOVE_OUTLIER_REAL_ONLY = 'is_remove_outlier_real_only'
ABNORMAL_COUNT = 'abnormal_count'
FINE_SELECT = 'fine_select'
TBLS = 'TBLS'
FILTER_PARTNO = 'filter-partno'
FILTER_MACHINE = 'machine_id'
CATE_PROC = 'end_proc_cate'
GET02_CATE_SELECT = 'GET02_CATE_SELECT'
CATEGORY_DATA = 'category_data'
FILTER_DATA = 'filter_data'
CATE_PROCS = 'cate_procs'
TIMES = 'times'
TIME_NUMBERINGS = 'time_numberings'
ELAPSED_TIME = 'elapsed_time'
COLORS = 'colors'
COLOR = 'color_fmt'
H_LABEL = 'h_label'
V_LABEL = 'v_label'
X_USER_FORMAT = 'x_user_fmt'
Y_USER_FORMAT = 'y_user_fmt'
TIME_MIN = 'time_min'
TIME_MAX = 'time_max'
X_THRESHOLD = 'x_threshold'
Y_THRESHOLD = 'y_threshold'
X_SERIAL = 'x_serial'
Y_SERIAL = 'y_serial'
SORT_KEY = 'sort_key'
FILTER_ON_DEMAND = 'filter_on_demand'
DIV_FROM_TO = 'div_from_to'
X_ID = 'x_id'
Y_ID = 'y_id'
X_NAME = 'x_name'
Y_NAME = 'y_name'
X_LABEL = 'x_label'
Y_LABEL = 'y_label'
X_IS_MASTER = 'x_is_master'
Y_IS_MASTER = 'y_is_master'
ARRAY_X_MASTER = 'array_x_master'
ARRAY_Y_MASTER = 'array_y_master'
PROC_LINK_ORDER = 'proc_link_order'

UNIQUE_SERIAL = 'unique_serial'
UNIQUE_SERIAL_TRAIN = 'unique_serial_train'
UNIQUE_SERIAL_TEST = 'unique_serial_test'
WITH_IMPORT_OPTIONS = 'with_import'
GET_PARAM = 'get_param'
PROCS = 'procs'
CLIENT_TIMEZONE = 'client_timezone'
DATA_SIZE = 'data_size'
X_OPTION = 'xOption'
SERIAL_PROCESS = 'serialProcess'
SERIAL_COLUMN = 'serialColumn'
SERIAL_ORDER = 'serialOrder'
TEMP_X_OPTION = 'TermXOption'
TEMP_SERIAL_PROCESS = 'TermSerialProcess'
TEMP_SERIAL_COLUMN = 'TermSerialColumn'
TEMP_SERIAL_ORDER = 'TermSerialOrder'
THRESHOLD_BOX = 'thresholdBox'
SCATTER_CONTOUR = 'scatter_contour'
SHOW_ONLY_CONTOUR = 'is_show_contour_only'
ORDER_ARRAY_FORMVAL = 'order_array_formval'
DF_ALL_PROCS = 'dfProcs'
DF_ALL_COLUMNS = 'dfColumns'
CHART_TYPE = 'chartType'
EXPORT_FROM = 'export_from'
AVAILABLE_ORDERS = 'available_ordering_columns'
IS_NOMINAL_SCALE = 'is_nominal_scale'
NOMINAL_VARS = 'nominal_vars'

# CATEGORICAL PLOT
CATE_VARIABLE = 'categoryVariable'
CATE_VALUE_MULTI = 'categoryValueMulti'
PART_NO = 'PART_NO'
EQUIP_ID = 'EQUIP_ID'
COMPARE_TYPE = 'compareType'
CATEGORICAL = 'var'
TERM = 'term'
RL_CATEGORY = 'category'
RL_CYCLIC_TERM = 'cyclicTerm'
RL_DIRECT_TERM = 'directTerm'
TIME_CONDS = 'time_conds'
CATE_CONDS = 'cate_conds'
LINE_NO = 'LINE_NO'
YAML_LINE_LIST = 'line-list'
FILTER_OTHER = 'filter-other'
THRESH_HIGH = 'thresh-high'
THRESH_LOW = 'thresh-low'
PRC_MAX = 'prc-max'
PRC_MIN = 'prc-min'
ACT_FROM = 'act-from'
ACT_TO = 'act-to'
CYCLIC_DIV_NUM = 'cyclicTermDivNum'
CYCLIC_INTERVAL = 'cyclicTermInterval'
CYCLIC_WINDOW_LEN = 'cyclicTermWindowLength'
CYCLIC_TERMS = 'cyclic_terms'
END_PROC_ID = 'end_proc_id'
END_PROC_NAME = 'end_proc_name'
END_COL_ID = 'end_col_id'
END_COL_FORMAT = 'end_col_format'
CAT_EXP_FORMAT = 'cat_exp_format'
END_COL_NAME = 'end_col_name'
COLOR_COL_FORMAT = 'color_col_format'
DIV_COL_FORMAT = 'div_col_format'
END_COL_SHOW_NAME = 'end_col_show_name'
RANK_COL = 'before_rank_values'
SUMMARIES = 'summaries'
IS_RESAMPLING = 'is_resampling'
CAT_DISTRIBUTE = 'category_distributed'
CAT_SUMMARY = 'cat_summary'
FMT = 'fmt'
N = 'n'
N_PCTG = 'n_pctg'
N_NA = 'n_na'
N_NA_PCTG = 'n_na_pctg'
N_TOTAL = 'n_total'
UNIQUE_CATEGORIES = 'unique_categories'
UNIQUE_DIV = 'unique_div'
UNIQUE_COLOR = 'unique_color'
CAT_UNIQUE_LIMIT = 200
IS_OVER_UNIQUE_LIMIT = 'isOverUniqueLimit'
DIC_CAT_FILTERS = 'dic_cat_filters'
TEMP_CAT_EXP = 'temp_cat_exp'
TEMP_CAT_PROCS = 'temp_cat_procs'
DIV_BY_DATA_NUM = 'dataNumber'
DIV_BY_CAT = 'div'
DIV_BY_CAT_FORMAT = 'divFormat'
COLOR_VAR = 'colorVar'
IS_DATA_LIMITED = 'isDataLimited'
COL_DETAIL = 'col_detail'
RANK_VAL = 'rank_value'
COL_TYPE = 'type'
ORG_ARRAY_Y = 'org_array_y'
CAT_ON_DEMAND = 'cat_on_demand'
IS_MASTER_COL = 'is_master_col'
UNIT = 'unit'

# Cat Expansion
CAT_EXP_BOX = 'catExpBox'
CAT_EXP_BOX_NAME = 'catExpBoxName'
CAT_EXP_BOX_FORMAT = 'catExpBoxFormat'

# Order columns
INDEX_ORDER_COLS = 'indexOrderColumns'
THIN_DATA_GROUP_COUNT = 'thinDataGroupCounts'

# validate data flag
IS_VALIDATE_DATA = 'isValidateData'
# Substring column name in universal db
SUB_STRING_COL_NAME = '{}_From_{}_To_{}'
SUB_STRING_REGEX = r'^(.+)_From_(\d+)_To_(\d+)$'

# CHM & HMp
HM_STEP = 'step'
HM_MODE = 'mode'
HM_FUNCTION_REAL = 'function_real'
HM_FUNCTION_CATE = 'function_cate'
HM_TRIM = 'remove_outlier'
CELL_SUFFIX = '_cell'
AGG_COL = 'agg_col'
TIME_COL = 'time'
TIME_COL_LOCAL = 'time_local'
INDEX = 'index'
PROC_PART_ID_COL = 'prod_part_id'
DATA_SOURCE_ID_COL = 'data_source_id'
COLOR_BAR_TTTLE = 'color_bar_title'
UNIQUE_INT_DIV = 'unique_int_div'
IS_CAT_COLOR = 'is_cat_color'

REQUEST_THREAD_ID = 'thread_id'
SERIALS = 'serials'
DATETIME = 'datetime'

AGP_COLOR_VARS = 'aggColorVar'
DIVIDE_OFFSET = 'divideOffset'
DIVIDE_FMT = 'divideFormat'
DIVIDE_FMT_COL = 'divide_format'
COLOR_NAME = 'color_name'
DATA = 'data'
SHOWN_NAME = 'shown_name'
COL_DATA_TYPE = 'data_type'
DIVIDE_CALENDAR_DATES = 'divDates'
DIVIDE_CALENDAR_LABELS = 'divFormats'

# master data
ARRAY_MASTER_IDS = 'array_master_ids'
CATEGORY_MASTER_IDS = 'category_master_ids'
MASTER_INFO = 'master_info'
RANK_COLS_TO_MASTER_IDS = 'rank_cols_to_master_ids'
DIV_MASTER_IDS = 'div_master_ids'
DIV_MASTER_COLUMN_NAME = 'div_master_column_name'
COLOR_MASTER_ID = 'color_master_id'
COLOR_MASTER_COL_NAME = 'color_master_col_name'

#  data group type of column
DATA_GROUP_TYPE = 'data_group_type'
IS_SERIAL_NO = 'is_serial_no'
IS_INT_CATEGORY = 'is_int_category'


class HMFunction(Enum):
    max = auto()
    min = auto()
    mean = auto()
    std = auto()
    range = auto()
    median = auto()
    count = auto()
    count_per_hour = auto()
    count_per_min = auto()
    first = auto()
    last = auto()
    time_per_count = auto()
    iqr = auto()
    ratio = 'Ratio[%]'


class RelationShip(Enum):
    ONE = auto()
    MANY = auto()


class AbsPath(Enum):
    SHOW = auto()
    HIDE = auto()


class DataTypeEncode(Enum):
    NULL = ''
    INTEGER = 'Int'
    REAL = 'Real'
    TEXT = 'Str'
    CATEGORY = 'Cat'
    DATETIME = 'CT'
    BIG_INT = 'BigInt'


class JobStatus(Enum):
    def __str__(self):
        return str(self.name)

    PENDING = 0
    PROCESSING = 1
    DONE = 2
    KILLED = 3
    FAILED = 4
    FATAL = 5  # error when insert to db commit, file lock v...v ( we need re-run these files on the next job)
    SENT_TO_BRIDGE = 6  # TODO. temp. -> need to discuss

    @classmethod
    def failed_statuses(cls):
        return [
            cls.FAILED.name,
            cls.FATAL.name,
        ]


class Outliers(Enum):
    NOT_OUTLIER = 0
    IS_OUTLIER = 1


class FlaskGKey(Enum):
    TRACE_ERR = auto()
    YAML_CONFIG = auto()
    # APP_DB_SESSION = auto()
    DEBUG_SHOW_GRAPH = auto()
    MEMOIZE = auto()
    THREAD_ID = auto()


class DebugKey(Enum):
    IS_DEBUG_MODE = auto()
    GET_DATA_FROM_DB = auto()


class MemoizeKey(Enum):
    STOP_USING_CACHE = auto()


# error message for dangling jobs
FORCED_TO_BE_FAILED = 'DANGLING JOB. FORCED_TO_BE_FAILED'
DEFAULT_POLLING_FREQ = 180  # default is import every 3 minutes


class CfgConstantType(BaseEnum):
    def __str__(self):
        return str(self.name)

    # CHECKED_COLUMN = 0  # TODO define value
    # GUI_TYPE = 1
    # FILTER_REGEX = 2
    # PARTNO_LIKE = 3
    POLLING_FREQUENCY = auto()
    ETL_JSON = auto()
    UI_ORDER = auto()
    USE_OS_TIMEZONE = auto()
    TS_CARD_ORDER = auto()
    EFA_HEADER_EXISTS = auto()
    BRIDGE_STATION_CONFIG = auto()
    BRIDGE_STATION_PARTITION_CONFIG = auto()
    EDGE_SERVER_ID = auto()  # server id
    LAST_INCREMENT_ID = auto()
    DISK_USAGE_CONFIG = auto()  # See initialize_disk_usage_limit
    SYNC_MASTER_CONFIG = auto()
    SYNC_TRANSACTION = auto()
    SYNC_PROC_LINK = auto()
    BREAK_JOB = auto()
    CONVERTED_USER_SETTING_URL = auto()
    MAX_GRAPH_NUMBER = auto()


# UI order types
UI_ORDER_DB = 'tblDbConfig'
UI_ORDER_TABLE = 'tblDataTableConfig'
UI_ORDER_PROC = 'tblProcConfig'

# SQL
SQL_PERCENT = '%'
SQL_REGEX_PREFIX = 'RAINBOW7_REGEX:'
SQL_REGEXP_FUNC = 'REGEXP'

# Measurement Protocol Server
MPS = 'www.google-analytics.com'
R_PORTABLE = 'R-Portable'
R_LIB_VERSION = 'R_LIB_VERSION'

# Message
MSG_DB_CON_FAILED = 'Database connection failed! Please check your database connection information'
MSG_NOT_SUPPORT_DB = 'This application does not support this type of database!'

# encoding
ENCODING_SHIFT_JIS = 'cp932'
ENCODING_UTF_8 = 'utf-8'
ENCODING_UTF_8_BOM = 'utf-8-sig'
ENCODING_ASCII = 'ascii'

# Web socket
SOCKETIO = 'socketio'
PROC_LINK_DONE_PUBSUB = '/proc_link_done_pubsub'
PROC_LINK_DONE_SUBSCRIBE = 'proc_link_subscribe'
PROC_LINK_DONE_PUBLISH = 'proc_link_publish'
SHUTDOWN_APP_DONE_PUBSUB = '/shutdown_app_done_pubsub'
SHUTDOWN_APP_DONE_PUBLISH = 'shutdown_app_publish'
BACKGROUND_JOB_PUBSUB = '/job'
LISTEN_BACKGROUND_TIMEOUT = 10  # seconds
# JOB_STATUS_PUBLISH = 'job_status_publish'
# JOB_INFO_PUBLISH = 'res_background_job'

# Dictionary Key
HAS_RECORD = 'has_record'

# WRAPR keys
WR_CTGY = 'ctgy'
WR_HEAD = 'head'
WR_RPLC = 'rplc'
WR_VALUES = 'values'
WR_HEADER_NAMES = 'header_name'
WR_TYPES = 'types'
# RIDGELINE
RL_GROUPS = 'groups'
RL_EMD = 'emd'
RL_DATA = 'data'
RL_RIDGELINES = 'ridgelines'
SENSOR_ID = 'sensor_id'
RL_ARRAY_X = 'array_x'
RL_CATE_NAME = 'cate_name'
RL_PERIOD = 'From|To'
RL_SENSOR_NAME = 'sensor_name'
PROC_NAME = 'proc_name'
PROC_MASTER_NAME = 'proc_master_name'
RL_KDE = 'kde_data'
RL_DEN_VAL = 'kde'
RL_ORG_DEN = 'origin_kde'
RL_TRANS_VAL = 'transform_val'
RL_TRANS_DEN = 'trans_kde'
RL_XAXIS = 'rlp_xaxis'
RL_YAXIS = 'rlp_yaxis'
RL_HIST_LABELS = 'hist_labels'
RL_HIST_COUNTS = 'hist_counts'
RL_DATA_COUNTS = 'data_counts'
RL_CATES = 'categories'

# SkD
SKD_TARGET_PROC_CLR = '#65c5f1'

# tile interface
TILE_INTERFACE = 'tile_interface'
SECTIONS = 'sections'
DN7_TILE = 'dn7'
AP_TILE = 'analysis_platform'
SEARCH_USAGE = 'usage'
TILE_MASTER = 'tile_master'
TILE_JUMP_CFG = 'jump'
RCMDS = 'recommends'
UN_AVAILABLE = 'unavailable'
ALL_TILES = 'all'
TILES = 'tiles'
UNDER_SCORE = '_'
TITLE = 'title'
HOVER = 'hover'
DESCRIPTION = 'description'
EXAMPLE = 'example'
ICON_PATH = 'icon_path'
PAGE = 'page'
PNG_PATH = 'png_path'
LINK_ADD = 'link_address'
ROW = 'row'
COLUMN = 'column'
ENG = 'en'


# actions
class Action(Enum):
    def __str__(self):
        return str(self.name)

    SHUTDOWN_APP = auto()


class YType(Enum):
    NORMAL = 0
    INF = 1
    NEG_INF = -1
    NONE = 2
    OUTLIER = 3
    NEG_OUTLIER = -3
    UNLINKED = -4


class ProcessCfgConst(Enum):
    PROC_ID = 'id'
    PROC_COLUMNS = 'columns'


class EFAColumn(Enum):
    def __str__(self):
        return str(self.name)

    Line = auto()
    Process = auto()
    Machine = auto()


EFA_HEADER_FLAG = '1'


class Operator(Enum):
    def __str__(self):
        return str(self.name)

    PLUS = '+'
    MINUS = '-'
    PRODUCT = '*'
    DEVIDE = '/'
    REGEX = 'regex'


class AggregateBy(Enum):
    DAY = 'Day'
    HOUR = 'Hour'


# App Config keys
# SQLITE_CONFIG_DIR = 'SQLITE_CONFIG_DIR'
PARTITION_NUMBER = 'PARTITION_NUMBER'
# EDGE_DB_FILE = 'EDGE_DB_FILE'
DB_SECRET_KEY = 'DB_SECRET_KEY'
SQLITE_CONFIG_DIR = 'SQLITE_CONFIG_DIR'
UNIVERSAL_DB_FILE = 'UNIVERSAL_DB_FILE'
APP_DB_FILE = 'APP_DB_FILE'
INIT_APP_DB_FILE = 'INIT_APP_DB_FILE'
INIT_BASIC_CFG_FILE = 'INIT_BASIC_CFG_FILE'
TESTING = 'TESTING'

DATA_TYPE_ERROR_MSG = 'There is an error with the data type.'
DATA_TYPE_DUPLICATE_MSG = 'There are duplicate records in the data file.'
DATA_TYPE_ERROR_EMPTY_DATA = 'The data file is empty.'
CAST_DATA_TYPE_ERROR_MSG = 'Cast Data Type Error'


class DataImportErrorTypes(Enum):
    COL_NOT_FOUND = 1
    TABLE_NOT_FOUND = 2
    EMPTY_DATA_FILE = 3
    DB_LOCKED = 4

    UNKNOWN = 100


UNKNOWN_ERROR_TEXT = 'An unknown error occurred'

ErrorMsgText = {
    DataImportErrorTypes.COL_NOT_FOUND: 'It is possible that a column could not be found.',
    DataImportErrorTypes.TABLE_NOT_FOUND: 'The table could not be found.',
    DataImportErrorTypes.EMPTY_DATA_FILE: 'The data file is empty.',
    DataImportErrorTypes.DB_LOCKED: 'The database is locked and data cannot be written.',
}

ErrorMsgFromDB = {
    DataImportErrorTypes.DB_LOCKED: 'database is locked',
    DataImportErrorTypes.TABLE_NOT_FOUND: 'no such table',
}

AUTO_BACKUP = 'auto-backup-universal'
ANALYSIS_INTERFACE_ENV = 'ANALYSIS_INTERFACE_ENV'
APP_HOST_ENV = 'APP_HOST_ENV'
APP_FILE_MODE_ENV = 'APP_FILE_MODE_ENV'
APP_LANGUAGE_ENV = 'lang'
APP_SUBTITLE_ENV = 'subt'
APP_BROWSER_DEBUG_ENV = 'APP_BROWSER_DEBUG_ENV'
APP_TYPE_ENV = 'APP_TYPE_ENV'
DATABASE_NAME_ENV = 'DATABASE_NAME_ENV'
DATABASE_HOST_ENV = 'DATABASE_HOST_ENV'
DATABASE_PORT_ENV = 'DATABASE_PORT_ENV'
DATABASE_USERNAME_ENV = 'DATABASE_USERNAME_ENV'
DATABASE_PASSWORD_ENV = 'DATABASE_PASSWORD_ENV'


class AppEnv(Enum):
    PRODUCTION = 'prod'
    DEVELOPMENT = 'dev'
    TEST = 'test'


THIN_DATA_CHUNK = 4000
THIN_DATA_COUNT = THIN_DATA_CHUNK * 3

# variables correlation
CORRS = 'corrs'
CORR = 'corr'
PCORR = 'pcorr'
NTOTALS = 'ntotals'

# Heatmap
MAX_TICKS = 8
AGG_FUNC = 'agg_function'
CATE_VAL = 'cate_value'
END_COL = 'end_col'
X_TICKTEXT = 'x_ticktext'
X_TICKVAL = 'x_tickvals'
Y_TICKTEXT = 'y_ticktext'
Y_TICKVAL = 'y_tickvals'
ACT_CELLS = 'actual_num_cell'

OBJ_VAR = 'objectiveVar'
OBJ_FORMAT = 'objective_fmt'
JUMP_WITH_OBJ_ID = 'objective_var'
EXCLUDED_COLUMNS = 'excluded_columns'

CAT_TOTAL = 'cat_total'
IS_CAT_LIMITED = 'is_cat_limited'
IS_CATEGORY = 'is_category'
MAX_CATEGORY_SHOW = 30

# PCA
SHORT_NAMES = 'short_names'
DATAPOINT_INFO = 'data_point_info'
PLOTLY_JSON = 'plotly_jsons'
DIC_SENSOR_HEADER = 'dic_sensor_headers'


# chart type
class ChartType(Enum):
    HEATMAP = 'heatmap'
    SCATTER = 'scatter'
    VIOLIN = 'violin'
    HEATMAP_BY_INT = 'heatmap_by_int'


# Scp sub request params
MATRIX_COL = 'colNumber'
COLOR_ORDER = 'scpColorOrder'


# COLOR ORDER
class ColorOrder(Enum):
    DATA = 1
    TIME = 2
    ELAPSED_TIME = 3


# import export debug info
DIC_FORM_NAME = 'dic_form'
DF_NAME = 'df'
CONFIG_DB_NAME = 'config_db'
USER_SETTING_NAME = 'user_setting'
USER_SETTING_VERSION = 0
EN_DASH = '–'


# Disk usage warning level
class DiskUsageStatus(Enum):
    Normal = 0
    Warning = 1
    Full = 2


class MaxGraphNumber(Enum):
    AGP_MAX_GRAPH = auto()
    FPP_MAX_GRAPH = auto()
    RLP_MAX_GRAPH = auto()
    CHM_MAX_GRAPH = auto()
    SCP_MAX_GRAPH = auto()
    MSP_MAX_GRAPH = auto()
    STP_MAX_GRAPH = auto()


max_graph_number = {
    MaxGraphNumber.AGP_MAX_GRAPH.name: 18,
    MaxGraphNumber.FPP_MAX_GRAPH.name: 20,
    MaxGraphNumber.RLP_MAX_GRAPH.name: 20,
    MaxGraphNumber.CHM_MAX_GRAPH.name: 18,
    MaxGraphNumber.SCP_MAX_GRAPH.name: 49,
    MaxGraphNumber.MSP_MAX_GRAPH.name: 100,
    MaxGraphNumber.STP_MAX_GRAPH.name: 32,
}

# debug mode
IS_EXPORT_MODE = 'isExportMode'
IS_IMPORT_MODE = 'isImportMode'

# NA
NA_STR = 'NA'
INF_STR = 'Inf'
MINUS_INF_STR = '-Inf'

# Recent
VAR_TRACE_TIME = 'varTraceTime'
TERM_TRACE_TIME = 'termTraceTime'
CYCLIC_TRACE_TIME = 'cyclicTraceTime'
TRACE_TIME = 'traceTime'

# Limited graph flag
IS_GRAPH_LIMITED = 'isGraphLimited'

IMAGES = 'images'

# language
LANGUAGES = [
    'ja',
    'en',
    'it',
    'es',
    'vi',
    'pt',
    'hi',
    'th',
    'zh_CN',
    'zh_TW',
    'ar',
    'bg',
    'ca',
    'cs',
    'cy',
    'de',
    'el',
    'fa',
    'fi',
    'fr',
    'gd',
    'he',
    'hr',
    'hu',
    'id',
    'is',
    'km',
    'ko',
    'lb',
    'mi',
    'mk',
    'mn',
    'ms',
    'my',
    'ne',
    'nl',
    'no',
    'pa',
    'pl',
    'pt',
    'ro',
    'ru',
    'sd',
    'si',
    'sk',
    'sq',
    'sv',
    'te',
    'tl',
    'tr',
]

MAXIMUM_V2_PREVIEW_ZIP_FILES = 5
MAXIMUM_PROCESSES_ORDER_FILES = 3


class EMDType(Enum):
    drift = [False]
    diff = [True]
    both = [False, True]


# Bridge Info
BRIDGE_STATION_WEB_HOST = 'bridge_station_web_host'
BRIDGE_STATION_WEB_PORT = 'bridge_station_web_port'
BRIDGE_STATION_WEB_URL = 'bridge_station_web_url'
MODE = 'mode'


class CRUDType(Enum):
    INSERT = auto()
    UPDATE = auto()
    DELETE = auto()
    SELECT = auto()


TABLE_NAME = 'table_name'
DB_INSTANCE = 'db_instance'
CRUD_TYPE = 'crud_type'

ROWS = 'rows'
COLS = 'columns'

ARCHIVED_COLS = 'archived_columns'
ARCHIVED_ROWS = 'archived_rows'

REQUEST_MAX_TRIED = 3


class GRPCResponseStatus(BaseEnum):
    OK = auto()
    ERROR = auto()
    WARNING = auto()
    STARTED = auto()


class ErrorType(BaseEnum):
    DataError = 'Error'
    DuplicateError = 'Duplicate'


class ErrorOutputMsg(BaseEnum):
    DataError = 'Data Type Error'
    DuplicateError = 'Duplicate Record'


IGNORE_STRING = '__IGN0RE__'


class Suffixes:
    """
    See DataFrame.merge(df, suffixes)
    """

    KEEP_LEFT = (None, IGNORE_STRING)  # almost use this
    KEEP_RIGHT = (IGNORE_STRING, None)


NEW_COLUMN_PROCESS_IDS_KEY = 'new_column_process_ids'
DIRECT_STRING = 'Direct'
NULL_DEFAULT_STRING = 'Null'
DEFAULT_NONE_VALUE = pd.NA
HALF_WIDTH_SPACE = ' '
NORMALIZE_FORM = 'NFKC'
REPLACE_PAIRS = (('°C', '℃'), ('°F', '℉'))  # (from, to)
DIC_IGNORE_NORMALIZATION = {
    'cfg_data_source_csv': ['directory', 'etl_func'],
    'cfg_data_source_db': ['host', 'dbname', 'schema', 'username', 'password'],
    'cfg_csv_column': ['column_name'],
    'cfg_data_table_column': ['column_name'],
    'cfg_process_column': ['column_raw_name'],
}
DEFAULT_LINE_SIGN = 'L'
DEFAULT_EQUIP_SIGN = 'Eq'
DEFAULT_ST_SIGN = 'St'

DUMMY_FACTORY_ID = '__FACTORY_ID__'
DUMMY_FACTORY_NAME = '__FACTORY_NAME__'
DUMMY_FACTORY_ABBR = '__FACTORY_ABBR__'
DUMMY_PLANT_ID = '__PLANT_ID__'
DUMMY_PLANT_NAME = '__PLANT_NAME__'
DUMMY_PLANT_ABBR = '__PLANT_ABBR__'
DUMMY_PROD_FAMILY_ID = '__PROD_FAMILY_ID__'
DUMMY_PROD_FAMILY_NAME = '__PROD_FAMILY_NAME__'
DUMMY_PROD_FAMILY_ABBR = '__PROD_FAMILY_ABBR__'
DUMMY_OUTSOURCE = '__OUTSOURCE__'
DUMMY_DEPT_ID = '__DEPT_ID__'
DUMMY_DEPT_NAME = '__DEPT_NAME__'
DUMMY_DEPT_ABBR = '__DEPT_ABBR__'
DUMMY_SECT_ID = '__SECT_ID__'
DUMMY_SECT_NAME = '__SECT_NAME__'
DUMMY_SECT_ABBR = '__SECT_ABBR__'
DUMMY_PROD_ID = '__PROD_ID__'
DUMMY_PROD_NAME = '__PROD_NAME__'
DUMMY_PROD_ABBR = '__PROD_ABBR__'
DUMMY_PART_TYPE = '__PART_TYPE__'
DUMMY_PART_NAME = '__PART_NAME__'
DUMMY_PART_ABBR = '__PART_ABBR__'
DUMMY_PART_NO_FULL = '__PART_NO_FULL__'
DUMMY_EQUIP_PRODUCT_NO = '__EQUIP_PRODUCT_NO__'
DUMMY_EQUIP_PRODUCT_DATE = '__EQUIP_PRODUCT_DATE__'
DUMMY_STATION_NO = '__STATION_NO__'
DUMMY_PROCESS_ABBR = '__PROCESS_ABBR__'
DUMMY_DATA_ABBR = '__DATA_ABBR__'
DUMMY_UNIT = '__UNIT__'
DUMMY_LOCATION_NAME = '__LOCATION_NAME__'
DUMMY_LOCATION_ABBR = '__LOCATION_ABBR__'

DUMMY_LINE_ID = '__LINE_ID__'
DUMMY_LINE_NAME = '__LINE_NAME__'
DUMMY_LINE_NO = '__LINE_NO__'
DUMMY_PROCESS_ID = '__PROCESS_ID__'
DUMMY_PROCESS_NAME = '__PROCESS_NAME__'
DUMMY_PART_NO = '__PART_NO__'
DUMMY_DATA_ID = '__DATA_ID__'
DUMMY_DATA_NAME = '__DATA_NAME__'
DUMMY_DATA_VALUE = '__DATA_VALUE__'
DUMMY_EQUIP_ID = '__EQUIP_ID__'
DUMMY_EQUIP_NAME = '__EQUIP_NAME__'
DUMMY_EQUIP_NO = '__EQUIP_NO__'
DUMMY_SUB_PART_NO = '__SUB_PART_NO__'
DUMMY_SUB_LOT_NO = '__SUB_LOT_NO__'
DUMMY_SUB_TRAY_NO = '__SUB_TRAY_NO__'
DUMMY_SUB_SERIAL = '__SUB_SERIAL__'


class BaseMasterColumn:
    FACTORY_ID = DUMMY_FACTORY_ID
    FACTORY_NAME = DUMMY_FACTORY_NAME
    FACTORY_ABBR = DUMMY_FACTORY_ABBR

    PLANT_ID = DUMMY_PLANT_ID
    PLANT_NAME = DUMMY_PLANT_NAME
    PLANT_ABBR = DUMMY_PLANT_ABBR

    PROD_FAMILY_ID = DUMMY_PROD_FAMILY_ID
    PROD_FAMILY_NAME = DUMMY_PROD_FAMILY_NAME
    PROD_FAMILY_ABBR = DUMMY_PROD_FAMILY_ABBR

    DEPT_ID = DUMMY_DEPT_ID
    DEPT_NAME = DUMMY_DEPT_NAME
    DEPT_ABBR = DUMMY_DEPT_ABBR

    SECT_ID = DUMMY_SECT_ID
    SECT_NAME = DUMMY_SECT_NAME
    SECT_ABBR = DUMMY_SECT_ABBR

    PROD_ID = DUMMY_PROD_ID
    PROD_NAME = DUMMY_PROD_NAME
    PROD_ABBR = DUMMY_PROD_ABBR

    PART_TYPE = DUMMY_PART_TYPE
    PART_NAME = DUMMY_PART_NAME
    PART_ABBR = DUMMY_PART_ABBR
    PART_NO_FULL = DUMMY_PART_NO_FULL
    PART_NO = DUMMY_PART_NO

    LINE_ID = DUMMY_LINE_ID
    LINE_NAME = DUMMY_LINE_NAME
    LINE_NO = DUMMY_LINE_NO
    OUTSOURCE = DUMMY_OUTSOURCE
    STATION_NO = DUMMY_STATION_NO

    EQUIP_ID = DUMMY_EQUIP_ID
    EQUIP_NAME = DUMMY_EQUIP_NAME
    EQUIP_NO = DUMMY_EQUIP_NO
    EQUIP_PRODUCT_NO = DUMMY_EQUIP_PRODUCT_NO
    EQUIP_PRODUCT_DATE = DUMMY_EQUIP_PRODUCT_DATE

    PROCESS_ID = DUMMY_PROCESS_ID
    PROCESS_NAME = DUMMY_PROCESS_NAME
    PROCESS_ABBR = DUMMY_PROCESS_ABBR

    DATA_ID = DUMMY_DATA_ID
    DATA_NAME = DUMMY_DATA_NAME
    DATA_ABBR = DUMMY_DATA_ABBR
    DATA_VALUE = DUMMY_DATA_VALUE

    UNIT = DUMMY_UNIT
    LOCATION_NAME = DUMMY_LOCATION_NAME
    LOCATION_ABBR = DUMMY_LOCATION_ABBR

    SUB_PART_NO = DUMMY_SUB_PART_NO
    SUB_LOT_NO = DUMMY_SUB_LOT_NO
    SUB_TRAY_NO = DUMMY_SUB_TRAY_NO
    SUB_SERIAL = DUMMY_SUB_SERIAL

    @classmethod
    def get_default_column(cls, *args, **kwargs):
        return {
            cls.FACTORY_ID: DataGroupType.FACTORY_ID,
            cls.FACTORY_NAME: DataGroupType.FACTORY_NAME,
            cls.FACTORY_ABBR: DataGroupType.FACTORY_ABBR,
            cls.PLANT_ID: DataGroupType.PLANT_ID,
            cls.PLANT_NAME: DataGroupType.PLANT_NAME,
            cls.PLANT_ABBR: DataGroupType.PLANT_ABBR,
            cls.PROD_FAMILY_ID: DataGroupType.PROD_FAMILY_ID,
            cls.PROD_FAMILY_NAME: DataGroupType.PROD_FAMILY_NAME,
            cls.PROD_FAMILY_ABBR: DataGroupType.PROD_FAMILY_ABBR,
            cls.DEPT_ID: DataGroupType.DEPT_ID,
            cls.DEPT_NAME: DataGroupType.DEPT_NAME,
            cls.DEPT_ABBR: DataGroupType.DEPT_ABBR,
            cls.SECT_ID: DataGroupType.SECT_ID,
            cls.SECT_NAME: DataGroupType.SECT_NAME,
            cls.SECT_ABBR: DataGroupType.SECT_ABBR,
            cls.PROD_ID: DataGroupType.PROD_ID,
            cls.PROD_NAME: DataGroupType.PROD_NAME,
            cls.PROD_ABBR: DataGroupType.PROD_ABBR,
            cls.PART_TYPE: DataGroupType.PART_TYPE,
            cls.PART_NAME: DataGroupType.PART_NAME,
            cls.PART_ABBR: DataGroupType.PART_ABBR,
            cls.PART_NO_FULL: DataGroupType.PART_NO_FULL,
            cls.PART_NO: DataGroupType.PART_NO,
            cls.EQUIP_ID: DataGroupType.EQUIP_ID,
            cls.EQUIP_NAME: DataGroupType.EQUIP_NAME,
            cls.EQUIP_NO: DataGroupType.EQUIP_NO,
            cls.EQUIP_PRODUCT_NO: DataGroupType.EQUIP_PRODUCT_NO,
            cls.EQUIP_PRODUCT_DATE: DataGroupType.EQUIP_PRODUCT_DATE,
            cls.STATION_NO: DataGroupType.STATION_NO,
            cls.LINE_ID: DataGroupType.LINE_ID,
            cls.LINE_NAME: DataGroupType.LINE_NAME,
            cls.LINE_NO: DataGroupType.LINE_NO,
            cls.OUTSOURCE: DataGroupType.OUTSOURCE,
            cls.PROCESS_ID: DataGroupType.PROCESS_ID,
            cls.PROCESS_NAME: DataGroupType.PROCESS_NAME,
            cls.PROCESS_ABBR: DataGroupType.PROCESS_ABBR,
            cls.UNIT: DataGroupType.UNIT,
            cls.LOCATION_NAME: DataGroupType.LOCATION_NAME,
            cls.LOCATION_ABBR: DataGroupType.LOCATION_ABBR,
        }

    @classmethod
    def get_default_value(cls, *args, **kwargs):
        return {
            cls.FACTORY_ID: DEFAULT_NONE_VALUE,
            cls.FACTORY_NAME: DEFAULT_NONE_VALUE,
            cls.FACTORY_ABBR: DEFAULT_NONE_VALUE,
            cls.PLANT_ID: DEFAULT_NONE_VALUE,
            cls.PLANT_NAME: DEFAULT_NONE_VALUE,
            cls.PLANT_ABBR: DEFAULT_NONE_VALUE,
            cls.PROD_FAMILY_ID: DEFAULT_NONE_VALUE,
            cls.PROD_FAMILY_NAME: DEFAULT_NONE_VALUE,
            cls.PROD_FAMILY_ABBR: DEFAULT_NONE_VALUE,
            cls.DEPT_ID: DEFAULT_NONE_VALUE,
            cls.DEPT_NAME: DEFAULT_NONE_VALUE,
            cls.DEPT_ABBR: DEFAULT_NONE_VALUE,
            cls.SECT_ID: DEFAULT_NONE_VALUE,
            cls.SECT_NAME: DEFAULT_NONE_VALUE,
            cls.SECT_ABBR: DEFAULT_NONE_VALUE,
            cls.PROD_ID: DEFAULT_NONE_VALUE,
            cls.PROD_NAME: DEFAULT_NONE_VALUE,
            cls.PROD_ABBR: DEFAULT_NONE_VALUE,
            cls.PART_TYPE: DEFAULT_NONE_VALUE,
            cls.PART_NAME: DEFAULT_NONE_VALUE,
            cls.PART_ABBR: DEFAULT_NONE_VALUE,
            cls.PART_NO_FULL: DEFAULT_NONE_VALUE,
            cls.PART_NO: DEFAULT_NONE_VALUE,
            cls.EQUIP_ID: DEFAULT_NONE_VALUE,
            cls.EQUIP_NAME: DEFAULT_NONE_VALUE,
            cls.EQUIP_NO: DEFAULT_NONE_VALUE,
            cls.EQUIP_PRODUCT_NO: DEFAULT_NONE_VALUE,
            cls.EQUIP_PRODUCT_DATE: DEFAULT_NONE_VALUE,
            cls.STATION_NO: DEFAULT_NONE_VALUE,
            cls.LINE_ID: DEFAULT_NONE_VALUE,
            cls.LINE_NAME: DEFAULT_NONE_VALUE,
            cls.LINE_NO: DEFAULT_NONE_VALUE,
            cls.OUTSOURCE: DEFAULT_NONE_VALUE,
            cls.PROCESS_ID: DEFAULT_NONE_VALUE,
            cls.PROCESS_NAME: DEFAULT_NONE_VALUE,
            cls.PROCESS_ABBR: DEFAULT_NONE_VALUE,
            cls.UNIT: DEFAULT_NONE_VALUE,
            cls.LOCATION_NAME: DEFAULT_NONE_VALUE,
            cls.LOCATION_ABBR: DEFAULT_NONE_VALUE,
        }

    @classmethod
    def get_dummy_column_name(cls):
        dict_default_column = cls.get_default_column()
        return list(dict_default_column.keys())

    @classmethod
    def _switch_key_value(cls, dic: dict):
        return {data_group_type: name for name, data_group_type in dic.items()}

    @classmethod
    def is_dummy_column(cls, column_name):
        return isinstance(column_name, str) and column_name in vars(cls).values()


class OtherMasterColumn(BaseMasterColumn):
    @classmethod
    def get_default_value(cls, is_direct_import: bool = False):
        default_values = super().get_default_value()

        default_values[cls.LINE_ID] = DIRECT_STRING if is_direct_import else DEFAULT_NONE_VALUE
        default_values[cls.LINE_NAME] = DIRECT_STRING if is_direct_import else DEFAULT_NONE_VALUE

        default_values[cls.EQUIP_ID] = DIRECT_STRING if is_direct_import else DEFAULT_NONE_VALUE
        default_values[cls.EQUIP_NAME] = DIRECT_STRING if is_direct_import else DEFAULT_NONE_VALUE

        default_values[cls.PROD_FAMILY_ID] = DIRECT_STRING if is_direct_import else DEFAULT_NONE_VALUE
        default_values[cls.PROD_FAMILY_NAME] = DIRECT_STRING if is_direct_import else DEFAULT_NONE_VALUE

        default_values[cls.PROD_ID] = DIRECT_STRING if is_direct_import else DEFAULT_NONE_VALUE
        default_values[cls.PROD_NAME] = DIRECT_STRING if is_direct_import else DEFAULT_NONE_VALUE

        return default_values


class EFAMasterColumn(BaseMasterColumn):
    @classmethod
    def get_default_column(cls, is_key_name: bool = True):
        default_columns = super().get_default_column()
        default_columns[cls.DATA_ID] = DataGroupType.DATA_ID
        default_columns[cls.DATA_VALUE] = DataGroupType.DATA_VALUE
        return default_columns if is_key_name else cls._switch_key_value(default_columns)

    @classmethod
    def get_default_value(cls):
        default_values = super().get_default_value()
        default_values[cls.DATA_ID] = DEFAULT_NONE_VALUE
        default_values[cls.DATA_VALUE] = DEFAULT_NONE_VALUE
        return default_values


class V2MasterColumn(BaseMasterColumn):
    @classmethod
    def get_default_column(cls, is_key_name: bool = True):
        default_columns = super().get_default_column()
        default_columns[cls.DATA_ID] = DataGroupType.DATA_ID
        default_columns[cls.DATA_VALUE] = DataGroupType.DATA_VALUE
        return default_columns if is_key_name else cls._switch_key_value(default_columns)

    @classmethod
    def get_default_value(cls):
        default_values = super().get_default_value()
        default_values[cls.DATA_ID] = DEFAULT_NONE_VALUE
        default_values[cls.DATA_VALUE] = DEFAULT_NONE_VALUE
        return default_values


class V2HistoryMasterColumn(BaseMasterColumn):
    @classmethod
    def get_default_column(cls, is_key_name: bool = True):
        dic = {
            cls.SUB_PART_NO: DataGroupType.SUB_PART_NO,
            cls.SUB_LOT_NO: DataGroupType.SUB_LOT_NO,
            cls.SUB_TRAY_NO: DataGroupType.SUB_TRAY_NO,
            cls.SUB_SERIAL: DataGroupType.SUB_SERIAL,
        }
        default_dic = super().get_default_column()
        dic.update(default_dic)

        return dic if is_key_name else cls._switch_key_value(dic)

    @classmethod
    def get_default_value(cls):
        default_values = super().get_default_value()
        dict_v2_history = {
            cls.SUB_PART_NO: DEFAULT_NONE_VALUE,
            cls.SUB_LOT_NO: DEFAULT_NONE_VALUE,
            cls.SUB_TRAY_NO: DEFAULT_NONE_VALUE,
            cls.SUB_SERIAL: DEFAULT_NONE_VALUE,
        }
        default_values.update(dict_v2_history)

        return default_values


class SoftwareWorkshopMasterColumn(BaseMasterColumn):
    @classmethod
    def get_default_column(cls, is_key_name: bool = True):
        default_columns = super().get_default_column()
        default_columns[cls.DATA_ID] = DataGroupType.DATA_ID
        default_columns[cls.DATA_VALUE] = DataGroupType.DATA_VALUE
        return default_columns if is_key_name else cls._switch_key_value(default_columns)

    @classmethod
    def get_default_value(cls):
        default_values = super().get_default_value()
        default_values[cls.DATA_ID] = DEFAULT_NONE_VALUE
        default_values[cls.DATA_VALUE] = DEFAULT_NONE_VALUE
        return default_values


class FileExtension(BaseEnum):
    Feather = 'ftr'
    Parquet = 'parquet'
    Pickle = 'pkl'
    Csv = 'csv'
    Tsv = 'tsv'


USE_CONTOUR = 'use_contour'
USE_HEATMAP = 'use_heatmap'
COL_ID = 'column_id'
COL_NAME = 'column_name'
COL_MASTER_NAME = 'column_master_name'
PROC_ID = 'proc_id'
DATA_TABLE_ID = 'data_table_id'
DATA_TABLE_NAME = 'data_table_name'
COL_DETAIL_NAME = 'name'
NAME = 'name'
INT_AS_CAT = 'int_as_cat'
HAS_NEW_MASTER = 'has_new_master'


class DataCountType(Enum):
    YEAR = 'year'
    MONTH = 'month'
    WEEK = 'week'


class DuplicateSerialShow(Enum):
    SHOW_BOTH = 'all'
    SHOW_FIRST = 'first'
    SHOW_LAST = 'last'


class DuplicateSerialCount(Enum):
    AUTO = 'auto'
    CHECK = 'check'
    SILENT = 'silent'


class RemoveOutlierType(Enum):
    OP1 = 'Op1'  # p1-p99
    OP5 = 'Op5'  # p5-p95
    O6M = 'O6m'  # q3 + 2.5iqr Majority
    O6I = 'O6i'  # q3 + 2.5iqr Minority
    O6U = 'O6u'  # q3 + 2.5iqr Upper
    O6L = 'O6l'  # q3 + 2.5iqr Lower
    O4M = 'O4m'  # q3 + 1.5iqr Majority
    O4I = 'O4i'  # q3 + 1.5iqr Minority
    O4U = 'O4u'  # q3 + 1.5iqr Upper
    O4L = 'O4l'  # q3 + 1.5iqr Lower
    Majority = 'majority'
    Minority = 'minority'
    Upper = 'upper'
    Lower = 'lower'


ID = 'id'
ROWID = 'id'  # This row id is actually id in bridge station
IS_USE_DUMMY_DATETIME = 'is_use_dummy_datetime'
ENG_NAME = 'en_name'
IS_GET_DATE = 'is_get_date'
IS_DUMMY_DATETIME = 'is_dummy_datetime'
LIST_PROCS = 'list_procs'
GRAPH_FILTER_DETAILS = 'graph_filter_detail_ids'

EMPTY_STRING = ''


class DataGroupType(BaseEnum):
    """
    Enum supports for handling in system.
    Because user can map this type with some other column name. Should use get_primary_groups instead.
    """

    FACTORY = 1
    FACTORY_ID = 2
    FACTORY_NAME = 3
    FACTORY_ABBR = 4
    PLANT_ID = 5
    PLANT = 6
    PLANT_NAME = 7
    PLANT_ABBR = 8
    PROD_FAMILY = 9
    PROD_FAMILY_ID = 10
    PROD_FAMILY_NAME = 11
    PROD_FAMILY_ABBR = 12
    LINE = 13
    LINE_ID = 14
    LINE_NAME = 15
    LINE_NO = 16
    OUTSOURCE = 17
    DEPT = 18
    DEPT_ID = 19
    DEPT_NAME = 20
    DEPT_ABBR = 21
    SECT = 22
    SECT_ID = 23
    SECT_NAME = 24
    SECT_ABBR = 25
    PROD_ID = 26
    PRODUCT = 27
    PROD_NAME = 28
    PROD_ABBR = 29
    PARTTYPE = 30
    PART_TYPE = 31
    PART_NAME = 32
    PART_ABBR = 33
    PART = 34
    PART_NO_FULL = 35
    PART_NO = 36
    EQUIP = 37
    EQUIP_ID = 38
    EQUIP_NAME = 39
    EQUIP_PRODUCT_NO = 40
    EQUIP_PRODUCT_DATE = 41
    STATION = 42
    STATION_NO = 43
    EQUIP_NO = 44
    PROCESS = 45
    PROCESS_ID = 46
    PROCESS_NAME = 47
    PROCESS_ABBR = 48
    DATA_ID = 49
    DATA_NAME = 50
    DATA_ABBR = 51
    DATA_VALUE = 52
    UNIT = 53
    LOCATION_NAME = 54
    LOCATION_ABBR = 55
    DATA_TIME = 56
    DATA_SERIAL = 57
    SUB_PART_NO = 62
    SUB_SERIAL = 63
    SUB_LOT_NO = 64
    SUB_TRAY_NO = 65
    AUTO_INCREMENTAL = 66
    HORIZONTAL_DATA = 67
    FileName = 97
    DATA_SOURCE_NAME = 98
    JUDGE = 99
    LOTNO = 99
    CARRIERNO = 99
    TRAY_NO = 99
    WORK_TYPE = 99
    QUALITY = 99
    LOT_NO = 99
    OK = 99
    NG = 99
    # V2_HISTORY
    Femto_Date = 68
    Femto_Mach = 69
    Femto_Order = 70
    Line = 71
    Datetime = 72
    Milling = 73
    MAIN_DATE = 74
    MAIN_TIME = 75
    # V2_HISTORY
    GENERATED = 99
    GENERATED_EQUATION = 100  # unused

    @classmethod
    def get_transaction_data_groups(cls):  # columns were stored in t_master_data
        return [cls.DATA_SERIAL, cls.GENERATED]

    @classmethod
    def get_master_group_values(cls):  # column values were stored in t_master_data
        return [
            cls.LINE_ID.value,
            cls.PART_NO.value,
            cls.EQUIP_ID.value,
            cls.DATA_TIME.value,
            cls.DATA_SERIAL.value,
            cls.LINE_NAME.value,
            cls.EQUIP_NAME.value,
            cls.FACTORY_ID.value,
            cls.FACTORY_NAME.value,
            cls.PLANT_ID.value,
            cls.DEPT_ID.value,
            cls.DEPT_NAME.value,
            cls.PART_NO_FULL.value,
            cls.EQUIP_ID.value,
        ]

    @classmethod
    def get_column_type_show_graph(cls):  # column show in graph
        show_graph_column_types = [
            cls.FACTORY.value,
            cls.PLANT.value,
            cls.PROD_FAMILY.value,
            cls.DATA_TIME.value,
            cls.DATA_SERIAL.value,
            cls.LINE.value,
            cls.DEPT.value,
            cls.SECT.value,
            cls.PRODUCT.value,
            cls.PARTTYPE.value,
            cls.PART.value,
            cls.EQUIP.value,
            cls.STATION.value,
            cls.DATA_SOURCE_NAME.value,
            cls.GENERATED.value,
            cls.MAIN_DATE.value,
            cls.MAIN_TIME.value,
        ]

        return show_graph_column_types

    @classmethod
    def get_represent_column_values(cls):
        return [
            cls.FACTORY.value,
            cls.PLANT.value,
            cls.PROD_FAMILY.value,
            cls.LINE.value,
            cls.DEPT.value,
            cls.SECT.value,
            cls.PRODUCT.value,
            cls.PARTTYPE.value,
            cls.PART.value,
            cls.EQUIP.value,
            cls.STATION.value,
            cls.DATA_SOURCE_NAME.value,
        ]

    @classmethod
    def get_physical_column_types(cls):
        return [
            cls.GENERATED.value,
            cls.DATA_SERIAL.value,
            cls.AUTO_INCREMENTAL.value,
            cls.DATA_TIME.value,
            cls.MAIN_DATE.value,
            cls.MAIN_TIME.value,
        ]

    @classmethod
    def get_hide_column_type_cfg_proces_columns(cls):
        return cls.get_represent_column_values() + [cls.GENERATED_EQUATION.value]

    @classmethod
    def get_all_reserved_groups(cls):
        return tuple(cls.__members__.keys())

    @classmethod
    def not_master_data_column(cls):
        return [
            DataGroupType.DATA_SERIAL.value,
            DataGroupType.DATA_TIME.value,
            DataGroupType.AUTO_INCREMENTAL.value,
            DataGroupType.HORIZONTAL_DATA.value,
            DataGroupType.GENERATED.value,
            DataGroupType.DATA_SOURCE_NAME.value,
            DataGroupType.MAIN_DATE.value,
            DataGroupType.MAIN_TIME.value,
        ]

    @classmethod
    def is_master_data_column(cls, column_type: int):
        return column_type not in cls.not_master_data_column()

    @classmethod
    def is_data_source_name(cls, column_type: int) -> bool:
        return column_type == cls.DATA_SOURCE_NAME.value

    @classmethod
    def get_v2_history_generated_columns(cls):
        return [
            cls.Femto_Date,
            cls.Femto_Mach,
            cls.Femto_Order,
            cls.Line,
            cls.Datetime,
            cls.Milling,
        ]


def get_others_well_known_columns():
    file_name = 'data_files/config/well_know_general.csv'
    if not os.path.exists(file_name):
        return {}

    df = pd.read_csv(
        file_name,
        usecols=[1, 2],
    )

    dict_others = df.replace({np.nan: None}).set_index('data_group_type')['pattern_regex'].to_dict()
    return dict_others


WELL_KNOWN_COLUMNS = {
    'OTHERS': get_others_well_known_columns(),
    'EFA': {
        'LINE_NO': DataGroupType.LINE_ID.value,
        'PROCESS_NO': DataGroupType.PROCESS_ID.value,
        'PART_NO': DataGroupType.PART_NO.value,
        'EQUIP_NO': DataGroupType.EQUIP_ID.value,
        'CHECK_CODE': DataGroupType.DATA_ID.value,
        'LOT_NO1': DataGroupType.DATA_SERIAL.value,
        'LOT_NO2': DataGroupType.DATA_SERIAL.value,
        'LOT_NO3': DataGroupType.DATA_SERIAL.value,
        'LOT_NO4': DataGroupType.DATA_SERIAL.value,
        'LOT_NO5': DataGroupType.DATA_SERIAL.value,
        'LOT_NO6': DataGroupType.DATA_SERIAL.value,
        'LOT_NO7': DataGroupType.DATA_SERIAL.value,
        'LOT_NO8': DataGroupType.DATA_SERIAL.value,
        'LOT_NO9': DataGroupType.DATA_SERIAL.value,
        'LOT_NO10': DataGroupType.DATA_SERIAL.value,
        'LOT_NO': DataGroupType.DATA_SERIAL.value,
        'CHECK_DATE1': DataGroupType.DATA_TIME.value,
        'CHECK_DATE2': DataGroupType.DATA_TIME.value,
        'CHECK_DATE3': DataGroupType.DATA_TIME.value,
        'CHECK_DATE4': DataGroupType.DATA_TIME.value,
        'CHECK_DATE5': DataGroupType.DATA_TIME.value,
        'CHECK_DATE6': DataGroupType.DATA_TIME.value,
        'CHECK_DATE7': DataGroupType.DATA_TIME.value,
        'CHECK_DATE8': DataGroupType.DATA_TIME.value,
        'CHECK_DATE9': DataGroupType.DATA_TIME.value,
        'CHECK_DATE10': DataGroupType.DATA_TIME.value,
        'SET_DATE': DataGroupType.DATA_TIME.value,
        'CHECK_DATA1': DataGroupType.DATA_VALUE.value,
    },
    'EFA_HISTORY': {
        'LINE_NO': DataGroupType.LINE_ID.value,
        'PROCESS_NO': DataGroupType.PROCESS_ID.value,
        'PART_NO': DataGroupType.PART_NO.value,
        'EQUIP_NO': DataGroupType.EQUIP_ID.value,
        'PART_TYPE_CODE': DataGroupType.DATA_ID.value,
        'LOT_NO': DataGroupType.DATA_SERIAL.value,
        'SET_DATE': DataGroupType.DATA_TIME.value,
        'CHILD_LOT': DataGroupType.DATA_VALUE.value,
    },
    'V2': {
        'ラインID': DataGroupType.LINE_ID.value,
        'ライン名': DataGroupType.LINE_NAME.value,
        '工程ID': DataGroupType.PROCESS_ID.value,
        '工程名': DataGroupType.PROCESS_NAME.value,
        '子設備ID': DataGroupType.EQUIP_ID.value,
        '子設備名': DataGroupType.EQUIP_NAME.value,
        '品番': DataGroupType.PART_NO.value,
        # TODO(209-210): confirm PO. #5_E
        'ワーク種別': DataGroupType.HORIZONTAL_DATA.value,
        '良否': DataGroupType.HORIZONTAL_DATA.value,
        'ロットNo': DataGroupType.HORIZONTAL_DATA.value,
        'ロット番号': DataGroupType.HORIZONTAL_DATA.value,
        'トレイNo': DataGroupType.HORIZONTAL_DATA.value,
        'トレイ番号': DataGroupType.HORIZONTAL_DATA.value,
        'シリアルNo': DataGroupType.DATA_SERIAL.value,
        'シリアル番号': DataGroupType.DATA_SERIAL.value,
        '計測日時': DataGroupType.DATA_TIME.value,
        '計測項目ID': DataGroupType.DATA_ID.value,
        '計測項目名': DataGroupType.DATA_NAME.value,
        '計測値': DataGroupType.DATA_VALUE.value,
    },
    'V2_MULTI': {
        'ラインID': DataGroupType.LINE_ID.value,
        'ライン': DataGroupType.LINE_NAME.value,
        '工程ID': DataGroupType.PROCESS_ID.value,
        '工程': DataGroupType.PROCESS_NAME.value,
        '子設備ID': DataGroupType.EQUIP_ID.value,
        '子設備': DataGroupType.EQUIP_NAME.value,
        '品番': DataGroupType.PART_NO.value,
        # TODO(209-210): confirm PO. #5_E
        'ワーク種別': DataGroupType.HORIZONTAL_DATA.value,
        '良否': DataGroupType.HORIZONTAL_DATA.value,
        'ロットNo': DataGroupType.HORIZONTAL_DATA.value,
        'トレイNo': DataGroupType.HORIZONTAL_DATA.value,
        'シリアルNo': DataGroupType.DATA_SERIAL.value,
        '加工日時': DataGroupType.DATA_TIME.value,
        '測定項目名': DataGroupType.DATA_NAME.value,
        '測定値': DataGroupType.DATA_VALUE.value,
        'line_id': DataGroupType.LINE_ID.value,
        'line': DataGroupType.LINE_NAME.value,
        'process_id': DataGroupType.PROCESS_ID.value,
        'process': DataGroupType.PROCESS_NAME.value,
        'child_equipment_id': DataGroupType.EQUIP_ID.value,
        'child_equipment': DataGroupType.EQUIP_NAME.value,
        'part_number': DataGroupType.PART_NO.value,
        # TODO(209-210): confirm PO. #5_E
        'work_type': DataGroupType.HORIZONTAL_DATA.value,
        'quality': DataGroupType.HORIZONTAL_DATA.value,
        'lot_no': DataGroupType.HORIZONTAL_DATA.value,
        'tray_no': DataGroupType.HORIZONTAL_DATA.value,
        'serial_no': DataGroupType.DATA_SERIAL.value,
        'processed_date_time': DataGroupType.DATA_TIME.value,
        'measurement_item_name': DataGroupType.DATA_NAME.value,
        'measured_value': DataGroupType.DATA_VALUE.value,
    },
    'V2_HISTORY': {
        'ラインID': DataGroupType.LINE_ID.value,
        'ライン名': DataGroupType.LINE_NAME.value,
        '工程ID': DataGroupType.PROCESS_ID.value,
        '工程名': DataGroupType.PROCESS_NAME.value,
        '子設備ID': DataGroupType.EQUIP_ID.value,
        '子設備名': DataGroupType.EQUIP_NAME.value,
        '品番': DataGroupType.PART_NO.value,
        # TODO(209-210): confirm PO. #5_E
        'ワーク種別': DataGroupType.HORIZONTAL_DATA.value,
        '良否': DataGroupType.HORIZONTAL_DATA.value,
        'ロットNo': DataGroupType.HORIZONTAL_DATA.value,
        'トレイNo': DataGroupType.HORIZONTAL_DATA.value,
        'トレーNo': DataGroupType.HORIZONTAL_DATA.value,
        'シリアルNo': DataGroupType.DATA_SERIAL.value,
        '計測日時': DataGroupType.DATA_TIME.value,
        '子部品品番': DataGroupType.SUB_PART_NO.value,
        '子部品ロットNo': DataGroupType.SUB_LOT_NO.value,
        '子部品トレイNo': DataGroupType.SUB_TRAY_NO.value,
        '子部品トレーNo': DataGroupType.SUB_TRAY_NO.value,
        '子部品シリアルNo': DataGroupType.SUB_SERIAL.value,
    },
    'V2_MULTI_HISTORY': {
        'ラインID': DataGroupType.LINE_ID.value,
        'ライン': DataGroupType.LINE_NAME.value,
        '工程ID': DataGroupType.PROCESS_ID.value,
        '工程': DataGroupType.PROCESS_NAME.value,
        '子設備ID': DataGroupType.EQUIP_ID.value,
        '子設備': DataGroupType.EQUIP_NAME.value,
        '品番': DataGroupType.PART_NO.value,
        # TODO(209-210): confirm PO. #5_E
        'ワーク種別': DataGroupType.HORIZONTAL_DATA.value,
        '良否': DataGroupType.HORIZONTAL_DATA.value,
        'ロットNo': DataGroupType.HORIZONTAL_DATA.value,
        'トレイNo': DataGroupType.HORIZONTAL_DATA.value,
        'シリアルNo': DataGroupType.DATA_SERIAL.value,
        '加工日時': DataGroupType.DATA_TIME.value,
        '子部品品番': DataGroupType.SUB_PART_NO.value,
        '子部品ロットNo': DataGroupType.SUB_LOT_NO.value,
        '子部品トレイNo': DataGroupType.SUB_TRAY_NO.value,
        '子部品シリアルNo': DataGroupType.SUB_SERIAL.value,
    },
    'SOFTWARE_WORKSHOP': {
        'fctry_id': DataGroupType.FACTORY_ID.value,
        'fctry_name': DataGroupType.FACTORY_NAME.value,
        'line_id': DataGroupType.LINE_ID.value,
        'line_name': DataGroupType.LINE_NAME.value,
        'child_equip_id': DataGroupType.PROCESS_ID.value,
        'child_equip_name': DataGroupType.PROCESS_NAME.value,
        'code': DataGroupType.DATA_ID.value,
        'meas_item_name': DataGroupType.DATA_NAME.value,
        'unit': DataGroupType.UNIT.value,
        'value': DataGroupType.DATA_VALUE.value,
        'event_time': DataGroupType.DATA_TIME.value,
        'serial_no': DataGroupType.DATA_SERIAL.value,
        'part_no': DataGroupType.PART_NO_FULL.value,
        'lot_no': DataGroupType.HORIZONTAL_DATA.value,
        'tray_no': DataGroupType.HORIZONTAL_DATA.value,
    },
}

REVERSED_WELL_KNOWN_COLUMNS = {
    'V2_MULTI': {
        DataGroupType.LINE_ID.value: 'ラインID',
        DataGroupType.LINE_NAME.value: 'ライン',
        DataGroupType.PROCESS_ID.value: '工程ID',
        DataGroupType.PROCESS_NAME.value: '工程',
        DataGroupType.EQUIP_ID.value: '子設備ID',
        DataGroupType.EQUIP_NAME.value: '子設備',
        DataGroupType.PART_NO.value: '品番',
        # TODO(209-210): confirm PO. #5_E
        # DataGroupType.WORK_TYPE.value: 'ワーク種別',
        # DataGroupType.QUALITY.value: '良否',
        # DataGroupType.LOT_NO.value: 'ロットNo',
        # DataGroupType.TRAY_NO.value: 'トレイNo',
        DataGroupType.DATA_SERIAL.value: 'シリアルNo',
        DataGroupType.DATA_TIME.value: '加工日時',
        DataGroupType.DATA_NAME.value: '測定項目名',
        DataGroupType.DATA_VALUE.value: '測定値',
    },
    'V2_MULTI_HISTORY': {
        DataGroupType.LINE_ID.value: 'ラインID',
        DataGroupType.LINE_NAME.value: 'ライン',
        DataGroupType.PROCESS_ID.value: '工程ID',
        DataGroupType.PROCESS_NAME.value: '工程',
        DataGroupType.EQUIP_ID.value: '子設備ID',
        DataGroupType.EQUIP_NAME.value: '子設備',
        DataGroupType.PART_NO.value: '品番',
        # TODO(209-210): confirm PO. #5_E
        # DataGroupType.WORK_TYPE.value: 'ワーク種別',
        # DataGroupType.LOT_NO.value: 'ロットNo',
        # DataGroupType.TRAY_NO.value: 'トレイNo',
        DataGroupType.DATA_SERIAL.value: 'シリアルNo',
        DataGroupType.DATA_TIME.value: '加工日時',
        DataGroupType.SUB_PART_NO.value: '子部品品番',
        DataGroupType.SUB_LOT_NO.value: '子部品ロットNo',
        DataGroupType.SUB_TRAY_NO.value: '子部品トレイNo',
        DataGroupType.SUB_SERIAL.value: '子部品シリアルNo',
    },
}

# for en column name from v2 files
WELL_KNOWN_EN_COLUMNS = {
    DBType.V2_MULTI.name: {
        'line_id': DataGroupType.LINE_ID.value,
        'line': DataGroupType.LINE_NAME.value,
        'process_id': DataGroupType.PROCESS_ID.value,
        'process': DataGroupType.PROCESS_NAME.value,
        'equipment_id': DataGroupType.EQUIP_ID.value,
        'equipment': DataGroupType.EQUIP_NAME.value,
        'part_number': DataGroupType.PART_NO.value,
        # TODO(209-210): confirm PO. #5_E
        # 'work_type': DataGroupType.WORK_TYPE.value,
        # 'quality': DataGroupType.QUALITY.value,
        # 'lot_no': DataGroupType.LOT_NO.value,
        # 'tray_no': DataGroupType.TRAY_NO.value,
        'serial_no': DataGroupType.DATA_SERIAL.value,
        'processed_date_time': DataGroupType.DATA_TIME.value,
        'measurement_item_name': DataGroupType.DATA_NAME.value,
        'measured_value': DataGroupType.DATA_VALUE.value,
    },
}
REVERSED_WELL_KNOWN_EN_COLUMNS = {
    DBType.V2_MULTI.name: {
        DataGroupType.LINE_ID.value: 'line_id',
        DataGroupType.LINE_NAME.value: 'line',
        DataGroupType.PROCESS_ID.value: 'process_id',
        DataGroupType.PROCESS_NAME.value: 'process',
        DataGroupType.EQUIP_ID.value: 'equipment_id',
        DataGroupType.EQUIP_NAME.value: 'equipment',
        DataGroupType.PART_NO.value: 'part_number',
        # TODO(209-210): confirm PO. #5_E
        # DataGroupType.WORK_TYPE.value: 'work_type',
        # DataGroupType.QUALITY.value: 'quality',
        # DataGroupType.LOT_NO.value: 'lot_no',
        # DataGroupType.TRAY_NO.value: 'tray_no',
        DataGroupType.DATA_SERIAL.value: 'serial_no',
        DataGroupType.DATA_TIME.value: 'processed_date_time',
        DataGroupType.DATA_NAME.value: 'measurement_item_name',
        DataGroupType.DATA_VALUE.value: 'measured_value',
    },
}


class JobType(Enum):
    def __str__(self):
        return str(self.name)

    DEL_PROCESS = auto()
    CSV_IMPORT = auto()
    FACTORY_IMPORT = auto()
    GEN_GLOBAL = auto()
    CLEAN_DATA = auto()
    CLEAN_CACHE = auto()
    CLEAN_LOG = auto()
    FACTORY_PAST_IMPORT = auto()
    IDLE_MONITORING = auto()
    SHUTDOWN_APP = auto()
    BACKUP_DATABASE = auto()
    RESTORE_DATABASE = auto()
    BRIDGE_CHANGED_LISTEN = auto()
    MASTER_IMPORT = auto()
    PULL_DB_DATA = auto()
    PULL_FEATHER_DATA = auto()
    PULL_PAST_DB_DATA = auto()
    PULL_CSV_DATA = auto()  # TODO: change to DUMP IMPORT TARGET DATA
    PULL_PAST_CSV_DATA = auto()
    DUPLICATE_DATA_HANDLE = auto()
    SYNC_ETL_MAPPING = auto()
    SCAN_MASTER = auto()
    SCAN_DATA_TYPE = auto()
    SCAN_UNKNOWN_MASTER = auto()
    SCAN_UNKNOWN_DATA_TYPE = auto()
    ZIP_FILE = auto()
    ZIP_FILE_THREAD = auto()
    ZIP_SCAN_MASTER_FILE = auto()
    ZIP_SCAN_MASTER_FILE_THREAD = auto()
    SYNC_TRANSACTION = auto()
    GEN_CONFIG_PROCESS = auto()
    USER_APPROVED_MASTER = auto()
    SYNC_PROC_LINK = auto()
    SYNC_MASTER = auto()
    SYNC_CONFIG = auto()
    TRANSACTION_IMPORT = auto()
    TRANSACTION_PAST_IMPORT = auto()
    SCAN_FILE = auto()
    SCAN_FILE_THREAD = auto()
    PROC_LINK_COUNT = auto()
    TRANSACTION_CLEAN = auto()
    PROCESS_COMMUNICATE = auto()
    DATABASE_MAINTENANCE = auto()
    RESTRUCTURE_INDEXES = auto()
    ZIP_LOG = auto()
    CLEAN_ZIP = auto()
    CLEAN_EXPIRED_REQUEST = auto()
    PULL_FOR_AUTO_LINK = auto()

    @classmethod
    def transaction_import_job_id(cls, process_id: int, is_past: bool = True) -> str:
        job_type = cls.TRANSACTION_IMPORT if not is_past else cls.TRANSACTION_PAST_IMPORT
        return f'{job_type.name}_{process_id}'


class BridgeChannelResponseMsg:
    def __init__(self, dict_plain_msg):
        self.type = dict_plain_msg.get(BridgeResponseKey.Type.value)
        self.channel = dict_plain_msg.get(BridgeResponseKey.Type.value)
        self.pattern = dict_plain_msg.get(BridgeResponseKey.Pattern.value)
        self.data = dict_plain_msg.get(BridgeResponseKey.Data.value)


class BridgeResponseKey(Enum):
    Type = 'type'
    Channel = 'channel'
    Pattern = 'pattern'
    Data = 'data'


class BridgeMessageType(Enum):
    Subscribe = 'subscribe'
    Unsubscribe = 'unsubscribe'
    PSubscribe = 'psubscribe'
    PUnsubscribe = 'punsubscribe'
    Message = 'message'
    PMessage = 'pmessage'


INDEX_COL = '__INDEX__'
MAX_RECORD = 1_000_000
SQL_FACTORY_LIMIT = 5_000_000
MAX_IMPORT_CYCLE = 2_000_000
JOB_DONE = 100  # percent
PAST_YEARS_BACKWARD = -3
FETCH_MANY_SIZE = 1_000_000
TIME_ANCHORS = ('030000', '090000', '120000', '150000', '190000')  # hhmmss
DAY_ANCHORS = ('02', '06', '10', '14', '18', '22', '26')  # day
EFA_LIMIT_SCAN_MASTER = 500  # sql limit when select datetime by time range
DB_LIMIT_SCAN_MASTER = 1_000_000
CSV_LIMIT_SCAN_MASTER = 100_000
SQL_LIMIT_SCAN_DATA_TYPE = 10_000
SOFTWARE_WORKSHOP_LIMIT_SCAN_MASTER = 10_000
SOFTWARE_WORKSHOP_LIMIT_PULL_DB = 50_000
CATEGORY_RATIO = 0.01
CATEGORY_COUNT = 256
NUMBER_RECORD_FOR_SCAN_DATA = 2_000
MAX_VALUE_SMALL_INT = 32_767
MAX_VALUE_INT = 2_147_483_647
CATEGORY_TYPES = [RawDataTypeDB.CATEGORY.value]
NON_CATEGORY_TYPES = [
    RawDataTypeDB.TEXT.value,
    RawDataTypeDB.INTEGER.value,
    RawDataTypeDB.DATETIME.value,
    RawDataTypeDB.REAL.value,
]
JOB_ID = 'job_id'
JOB_NAME = 'job_name'
JOB_TYPE = 'job_type'
DB_CODE = 'db_code'
PROC_CODE = 'proc_code'
DB_MASTER_NAME = 'db_master_name'
DONE_PERCENT = 'done_percent'
STATUS = 'status'
PROCESS_MASTER_NAME = 'process_master_name'
DETAIL = 'detail'
DATA_TYPE_ERR = 'data_type_error'
DURATION = 'duration'
JOB_START_TM = 'start_tm'
JOB_END_TM = 'end_tm'

CATEGORY_TEXT_SHORTEST = 5  # Work-around for seihinmei
DF_CHUNK_SIZE = 200_000

SUB_PART_NO_DEFAULT_SUFFIX = '.'
SUB_PART_NO_NAMES = '部品'
SUB_PART_NO_DEFAULT_NO = 'No'
v2_PART_NO_REGEX = r'JP\d{10}$'
SUB_PART_NO_SUFFIX = 'Part'
SUB_PART_NO_PREFIX = 'Sub'

# PCP categorized real
CATEGORIZED_SUFFIX = '__CATEGORIZED__'
# timeout 10 seconds for preview data
PREVIEW_DATA_TIMEOUT = 10
NULL_PERCENT = 'null_percent'
ZERO_VARIANCE = 'zero_variance'
SELECTED_VARS = 'selected_vars'

# format
LEFT_Z_TILL_SYMBOL = '<'

START_TIME_COL = 'start_time_col'
END_TIME_COL = 'end_time_col'
LATEST_RECORDS_SQL_LIMIT = 2000

ZERO_FILL_PATTERN = r'^{\:(0)([<>]?)([1-9]\d*)d?\}$'  # {:010}, {:010d}
ZERO_FILL_PATTERN_2 = r'^{\:([1-9])([<>])(\d+)d?\}$'  # {:1>10}, {:1>10}, {:1<10d}

OSERR = {22: 'Access denied', 2: 'Folder not found', 20: 'Not a folder'}


class MasterDBType(BaseEnum):
    EFA = auto()
    EFA_HISTORY = auto()
    V2 = auto()
    V2_MULTI = auto()
    V2_HISTORY = auto()
    OTHERS = auto()
    V2_MULTI_HISTORY = auto()
    SOFTWARE_WORKSHOP = auto()

    @classmethod
    def is_v2_group(cls, master_type: str):
        return master_type in [cls.V2.name, cls.V2_MULTI.name, cls.V2_HISTORY.name, cls.V2_MULTI_HISTORY.name]

    @classmethod
    def is_efa_group(cls, master_type: str):
        return master_type in [cls.EFA.name, cls.EFA_HISTORY.name]

    @classmethod
    def is_long_db(cls, master_type: str):
        return master_type in [cls.EFA.name, cls.EFA_HISTORY.name, cls.SOFTWARE_WORKSHOP.name]


class TransactionForPurpose(Enum):
    FOR_SCAN_MASTER = auto()
    FOR_SCAN_DATA_TYPE = auto()
    FOR_AUTO_LINK = auto()


# Browser support
SAFARI_SUPPORT_VER = 15.4

UTF8_WITH_BOM = 'utf-8-sig'
UTF8_WITHOUT_BOM = 'utf-8'

MAX_RESERVED_NAME_ID = 10_000_000  # reserved name 1-> 10_000_000, others: 10_000_001 ->

SENSOR_NAMES = 'sensor_names'
SENSOR_IDS = 'sensor_ids'
COEF = 'coef'
BAR_COLORS = 'bar_colors'

# rlp NG rate
JUDGE_VAR = 'judgeVar'
NG_CONDITION = 'NGCondition'
NG_CONDITION_VALUE = 'NGConditionValue'
X = 'x'
Y = 'y'
X_NAME = 'x_name'
Y_NAME = 'y_name'
NG_RATES = 'ng_rates'
GROUP = 'group'
COUNT = 'count'
TRUE_MATCH = 'true'
RATE = 'rate'
IS_PROC_LINKED = 'is_proc_linked'
EXPORT_TERM_FROM = 'From'
EXPORT_TERM_TO = 'To'
EXPORT_NG_RATE = 'NG Rate'
JUDGE_LABEL = 'judge_label'


class NGCondition(Enum):
    LESS_THAN = '<'
    LESS_THAN_OR_EQUAL = '<='
    GREATER_THAN = '>'
    GREATER_THAN_OR_EQUAL = '>='
    EQUAL = '='
    NOT_EQUAL_TO = '!='


class CacheType(Enum):
    CONFIG_DATA = 1
    TRANSACTION_DATA = 2
    JUMP_FUNC = 3
    OTHER = 4


FACET_PER_ROW = 8
FACET_ROW = 'facet_row'
SENSORS = 'sensors'

CREATED_AT = 'created_at'
UPDATED_AT = 'updated_at'
SQL_PARAM_SYMBOL = '?'

# encoding
SHIFT_JIS_NAME = 'Shift-JIS'
SHIFT_JIS = 'shift_jis'
WINDOWS_31J = 'windows_31j'
CP932 = 'cp932'
UTF8_WITH_BOM_NAME = 'utf-8-bom'

LAST_REQUEST_TIME = 'last_request_time'

# nominal selection modal
CATEGORY_COLS = 'category_cols'

HTML_CODE_304 = 304
PCA_SAMPLE_DATA = 3000
CUM_RATIO_VALUE = 0.8
RL_MIN_DATA_COUNT = 8
MAX_GRAPH_COUNT = 20
HM_WEEK_MODE = 7
HM_WEEK_MODE_DAYS = 140
COMPLETED_PERCENT = 100
ALMOST_COMPLETE_PERCENT = 99
PAST_IMPORT_LIMIT_DATA_COUNT = 2_000_000

CLEAN_REQUEST_INTERVAL = 24  # 1 day interval

# External API request params
REQ_ID = 'req_id'
BOOKMARK_ID = 'bookmark_id'
PROCESS_ID = 'process_id'
COLUMNS = 'columns'
START_DATETIME = 'start_datetime'
END_DATETIME = 'end_datetime'
LATEST = 'latest'
OPTION_ID = 'option_id'
OD_FILTER = 'od_filter'
OBJECTIVE = 'objective'
FUNCTION = 'function'
BOOKMARKS = 'bookmarks'
PROCESSES = 'processes'
EXTERNAL_API = 'external_api'
FACET = 'facet'
FILTER = 'filter'
DIV = 'div'
REQUEST_PARAMS = 'params'  # for external API
BOOKMARK_TITLE = 'title'
CREATED_BY = 'created_by'
PRIORITY = 'priority'
BOOKMARK_DESCRIPTION = 'description'
SAVE_DATETIME = 'save_datetime'
SAVE_GRAPH_SETTINGS = 'save_graph_settings'
SAVE_LATEST = 'save_latest'
OD_FILTERS = 'od_filters'

# External API elements
TRACE_DATA_FORM = 'traceDataForm'
RADIO_DEFAULT_INTERVAL = 'radioDefaultInterval'
RADIO_RECENT_INTERVAL = 'radioRecentInterval'
CHECKED = 'checked'
FIRST_END_PROC = 'end-proc-process-1'
VALUE = 'value'
START_DATE_ID = 'startDate'
START_TIME_ID = 'startTime'
END_DATE_ID = 'endDate'
END_TIME_ID = 'endTime'

EXAMPLE_VALUE = 3


class PagePath(Enum):
    FPP = 'ap/fpp'
    STP = 'ap/stp'
    RLP = 'ap/rlp'
    CHM = 'ap/chm'
    MSP = 'ap/msp'
    SCP = 'ap/scp'
    AGP = 'ap/agp'
    SKD = 'ap/skd'
    PCP = 'ap/pcp'
    PCA = 'ap/analyze/anomaly_detection/pca'
    GL = 'ap/analyze/structure_learning/gl'
    REGISTER_DATA_FILE = 'ap/register_by_file'


PROCESS_QUEUE = '_queue'
SHUTDOWN = 'SHUTDOWN'
PORT = 'PORT'


# class DicConfig(BaseEnum):
#     PROCESS_QUEUE = auto()
#     DB_SECRET_KEY = auto()
#     SQLITE_CONFIG_DIR = auto()
#     APP_DB_FILE = auto()
#     UNIVERSAL_DB_FILE = auto()
#     TESTING = auto()


class ListenNotifyType(BaseEnum):
    JOB_PROGRESS = auto()
    ADD_JOB = auto()
    RESCHEDULE_JOB = auto()
    CLEAR_CACHE = auto()
    SHUTDOWN = auto()
    CATEGORY_ERROR = auto()
    RUNNING_JOB = auto()


SEQUENCE_CACHE = 1000


class DuplicateMode(BaseEnum):
    FIRST = auto()
    LAST = auto()
    BOTH = auto()


class CSVExtTypes(Enum):
    CSV = 'csv'
    TSV = 'tsv'
    SSV = 'ssv'
    ZIP = 'zip'


class DataColumnType(BaseEnum):
    DATETIME = 1
    MAIN_SERIAL = 2
    SERIAL = 3
    DATETIME_KEY = 4
    DATE = 5
    TIME = 6
    MAIN_DATE = 7  # main::date
    MAIN_TIME = 8  # main::time

    INT_CATE = 10

    LINE_NAME = 20
    LINE_NO = 21
    EQ_NAME = 22
    EQ_NO = 23
    PART_NAME = 24
    PART_NO = 25
    ST_NO = 26

    GENERATED = 99
    GENERATED_EQUATION = 100

    @classmethod
    def category_int_types(cls):
        return [
            cls.INT_CATE.value,
            cls.MAIN_SERIAL.value,
            cls.SERIAL.value,
            cls.LINE_NO.value,
            cls.EQ_NO.value,
            cls.PART_NO.value,
            cls.ST_NO.value,
        ]


NOTIFY_DELAY_TIME = 3  # seconds


class ColumnDTypeToSQLiteDType(BaseEnum):
    NULL = 'null'
    INTEGER = 'integer'
    REAL = 'real'
    TEXT = 'text'
    DATETIME = 'text'
    DATE = 'text'
    TIME = 'text'
    REAL_SEP = 'real'
    INTEGER_SEP = 'integer'
    EU_REAL_SEP = 'real'
    EU_INTEGER_SEP = 'integer'
    K_SEP_NULL = 'null'
    BIG_INT = 'string'


# if nchar(header) > 90%: generate column name
NUM_CHARS_THRESHOLD = 90


# Những column bị đôi tên trong cùng một thư mục csv
COLUMN_CONVERSION = {
    'シリアル番号': 'シリアル',
    'ロット番号': 'ロットNo',
    'トレイ番号': 'トレイNo',
    'トレー番号': 'トレイNo',
    '子部品ロット番号': '子部品ロットNo',
    '子部品トレイ番号': '子部品トレイNo',
    '子部品トレー番号': '子部品トレイNo',
    '子部品シリアル番号': '子部品シリアルNo',
}


class AnnounceEvent(Enum):
    JOB_RUN = auto()
    PROC_LINK = auto()
    SHUT_DOWN = auto()
    DATA_TYPE_ERR = auto()
    EMPTY_FILE = auto()
    PCA_SENSOR = auto()
    SHOW_GRAPH = auto()
    DISK_USAGE = auto()
    CLEAR_TRANSACTION_DATA = auto()
    PROC_ID = auto()
    IMPORT_CONFIG = auto()
    MAPPING_CONFIG_DONE = auto()
    CATEGORY_ERROR = auto()
    DEL_PROCESS = auto()
    DATA_REGISTER = auto()
    BACKUP_DATA_FINISHED = auto()
    RESTORE_DATA_FINISHED = auto()


class CategoryErrorType(Enum):
    OLD_UNIQUE_VALUE_EXCEED = auto()
    NEW_UNIQUE_VALUE_EXCEED = auto()

    @classmethod
    def get_error_type_from_count(cls, unique_count: int, df_unique_count: int) -> CategoryErrorType | None:
        if unique_count >= CATEGORY_COUNT:
            return cls.OLD_UNIQUE_VALUE_EXCEED
        if df_unique_count >= CATEGORY_COUNT:
            if unique_count == 0:
                return cls.NEW_UNIQUE_VALUE_EXCEED
            else:
                return cls.OLD_UNIQUE_VALUE_EXCEED
        return None


LOCK = 'LOCK'
MAPPING_DATA_LOCK = 'MAPPING_DATA_LOCK'
IGNORE_MULTIPROCESSING_LOCK_KEY = 'ignore_multiprocessing_lock'
CATEGORY_ERROR_RESCHEDULE_TIME = 80  # seconds
PROCESS_QUEUE_FILE_NAME = f'process_queue.{FileExtension.Pickle.value}'
DATE_TYPE_REGEX = (
    '^'
    r'(?P<year>\d{2}|\d{4})'
    r'(?:[\-\.\s\/\\年]?(?P<month>\d|0\d|1[0-2])[月]?)'
    r'(?:[\-\.\s\/\\]?(?P<day>\d|[0-2]\d|3[0-1])[日]?)?'
    '$'
)
TIME_TYPE_REGEX = (
    '^'
    r'(?P<hour>\d|[01]\d|2[0-3])'
    r'(?:[:\-\.\s時]?(?P<minute>\d|[0-5]\d)[分]?)'
    r'(?:[:\-\.\s]?(?P<second>\d|[0-5]\d)[秒]?)?'
    '$'
)


# register data from file
class RegisterDatasourceType(BaseEnum):
    DIRECTORY = 'directory'
    FILE = 'file'
    REFERENCE_FILE = 'reference_file'


SERVER_ADDR = [
    'localhost',
    '127.0.0.1',
]


class DataRegisterStage(BaseEnum):
    FIRST_IMPORTED = 'first_imported'
    DURING = 'during'
    FINISHED = 'finished'


# Time for announcing and blinking icon
ANNOUNCE_UPDATE_TIME: int = 60  # unit: seconds

# Limit range time to check new version
LIMIT_CHECKING_NEWER_VERSION_TIME: int = 60  # unit: seconds


class BooleanStringDefinition(BaseEnum):
    true = 1
    false = 0
