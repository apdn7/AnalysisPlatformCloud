import glob
import os.path
import re
from typing import Iterable, Iterator, Optional, Tuple, Type, Union

import numpy as np
import pandas as pd
from anyascii import anyascii
from pandas import DataFrame, Series
from pandas.core.dtypes.common import is_string_dtype

from ap import multiprocessing_lock
from ap.api.setting_module.services.filter_settings import insert_default_filter_config_raw_sql
from ap.common.common_utils import (
    camel_to_snake,
    check_exist,
    convert_nan_to_none,
    convert_type_base_df,
    detect_language_str,
    format_df,
    get_current_timestamp,
    get_dummy_data_path,
    get_nayose_path,
    read_feather_file,
    write_feather_file,
)
from ap.common.constants import (
    DEFAULT_NONE_VALUE,
    EMPTY_STRING,
    HALF_WIDTH_SPACE,
    ID,
    INDEX_COL,
    JOB_ID,
    MAPPING_DATA_LOCK,
    NEW_COLUMN_PROCESS_IDS_KEY,
    NULL_DEFAULT_STRING,
    BaseMasterColumn,
    CRUDType,
    DataGroupType,
    DataType,
    FileExtension,
    JobType,
    MasterDBType,
    RawDataTypeDB,
    Suffixes,
)
from ap.common.logger import log_execution_time, logger
from ap.common.memoize import memoize
from ap.common.pydn.dblib.db_common import SqlComparisonOperator
from ap.common.pydn.dblib.postgresql import PostgreSQL
from ap.common.services.jp_to_romaji_utils import to_romaji
from ap.common.services.normalization import normalize_series
from ap.setting_module.models import (
    CfgDataTable,
    CfgDataTableColumn,
    insert_or_update_config,
    make_session,
)
from ap.setting_module.services.background_process import JobInfo, send_processing_info
from bridge.common.bridge_station_config_utils import PostgresSequence
from bridge.common.dummy_data_utils import dump_from_csv, read_data
from bridge.models.bridge_station import BridgeStationModel, MasterModel
from bridge.models.cfg_data_source import CfgDataSource as BSCfgDataSource
from bridge.models.cfg_data_table import CfgDataTable as BSCfgDataTable
from bridge.models.cfg_process_column import CfgProcessColumn
from bridge.models.cfg_process_function_column import CfgProcessFunctionColumn
from bridge.models.m_data import MData
from bridge.models.m_data_group import (
    MDataGroup,
)
from bridge.models.m_dept import MDept
from bridge.models.m_equip import MEquip
from bridge.models.m_equip_group import MEquipGroup
from bridge.models.m_factory import MFactory
from bridge.models.m_function import MFunction
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
from bridge.models.mapping_factory_machine import MappingFactoryMachine
from bridge.models.mapping_part import MappingPart
from bridge.models.mapping_process_data import MappingProcessData
from bridge.models.r_factory_machine import RFactoryMachine
from bridge.models.r_prod_part import RProdPart
from bridge.redis_utils.db_changed import publish_master_config_changed
from bridge.services.data_import import NA_VALUES, PANDAS_DEFAULT_NA
from bridge.services.etl_services.etl_controller import ETLController
from bridge.services.etl_services.etl_service import ETLService
from bridge.services.master_catalog import MasterColumnMetaCatalog
from bridge.services.master_data_efa_v2_transform_pattern import (
    dict_config_efa_v2_transform_column,
    efa_v2_extract_master,
)
from bridge.services.master_data_etl_software_workshop_transform_pattern import (
    dict_config_etl_software_workshop_transform_column,
    etl_software_workshop_extract_master,
)
from bridge.services.master_data_general_transform_pattern import (
    dict_config_general_transform_column,
    general_extract_master,
)
from bridge.services.nayose_handler import ALL_DATA_RELATION, NAYOSE_FILE_NAMES
from bridge.services.scan_data_type import set_scan_data_type_status_done
from bridge.services.word_prediction import get_word_prediction

global_file_name = None
global_df = pd.DataFrame()  # todo remove

dict_config_bs_name = {  # todo: how to common this config
    DataGroupType.LOCATION_NAME.name: MappingFactoryMachine.Columns.t_location_name.name,
    DataGroupType.LOCATION_ABBR.name: MappingFactoryMachine.Columns.t_location_abbr.name,
    DataGroupType.FACTORY_ID.name: MappingFactoryMachine.Columns.t_factory_id.name,
    DataGroupType.FACTORY_NAME.name: MappingFactoryMachine.Columns.t_factory_name.name,
    DataGroupType.FACTORY_ABBR.name: MappingFactoryMachine.Columns.t_factory_abbr.name,
    DataGroupType.PLANT_ID.name: MappingFactoryMachine.Columns.t_plant_id.name,
    DataGroupType.PLANT_NAME.name: MappingFactoryMachine.Columns.t_plant_name.name,
    DataGroupType.PLANT_ABBR.name: MappingFactoryMachine.Columns.t_plant_abbr.name,
    DataGroupType.DEPT_ID.name: MappingFactoryMachine.Columns.t_dept_id.name,
    DataGroupType.DEPT_NAME.name: MappingFactoryMachine.Columns.t_dept_name.name,
    DataGroupType.DEPT_ABBR.name: MappingFactoryMachine.Columns.t_dept_abbr.name,
    DataGroupType.SECT_ID.name: MappingFactoryMachine.Columns.t_sect_id.name,
    DataGroupType.SECT_NAME.name: MappingFactoryMachine.Columns.t_sect_name.name,
    DataGroupType.SECT_ABBR.name: MappingFactoryMachine.Columns.t_sect_abbr.name,
    DataGroupType.LINE_ID.name: MappingFactoryMachine.Columns.t_line_id.name,
    DataGroupType.LINE_NO.name: MappingFactoryMachine.Columns.t_line_no.name,
    DataGroupType.LINE_NAME.name: MappingFactoryMachine.Columns.t_line_name.name,
    DataGroupType.OUTSOURCE.name: MappingFactoryMachine.Columns.t_outsource.name,
    DataGroupType.EQUIP_ID.name: MappingFactoryMachine.Columns.t_equip_id.name,
    DataGroupType.EQUIP_NAME.name: MappingFactoryMachine.Columns.t_equip_name.name,
    DataGroupType.EQUIP_PRODUCT_NO.name: MappingFactoryMachine.Columns.t_equip_product_no.name,
    DataGroupType.EQUIP_PRODUCT_DATE.name: MappingFactoryMachine.Columns.t_equip_product_date.name,
    DataGroupType.EQUIP_NO.name: MappingFactoryMachine.Columns.t_equip_no.name,
    DataGroupType.STATION_NO.name: MappingFactoryMachine.Columns.t_station_no.name,
    DataGroupType.PROD_FAMILY_ID.name: MappingFactoryMachine.Columns.t_prod_family_id.name,
    DataGroupType.PROD_FAMILY_NAME.name: MappingFactoryMachine.Columns.t_prod_family_name.name,
    DataGroupType.PROD_FAMILY_ABBR.name: MappingFactoryMachine.Columns.t_prod_family_abbr.name,
    DataGroupType.PROD_ID.name: MappingFactoryMachine.Columns.t_prod_id.name,
    DataGroupType.PROD_NAME.name: MappingFactoryMachine.Columns.t_prod_name.name,
    DataGroupType.PROD_ABBR.name: MappingFactoryMachine.Columns.t_prod_abbr.name,
    DataGroupType.PROCESS_ID.name: MappingFactoryMachine.Columns.t_process_id.name,
    DataGroupType.PROCESS_NAME.name: MappingFactoryMachine.Columns.t_process_name.name,
    DataGroupType.PROCESS_ABBR.name: MappingFactoryMachine.Columns.t_process_abbr.name,
    DataGroupType.PART_TYPE.name: MappingPart.Columns.t_part_type.name,
    DataGroupType.PART_NAME.name: MappingPart.Columns.t_part_name.name,
    DataGroupType.PART_ABBR.name: MappingPart.Columns.t_part_abbr.name,
    DataGroupType.PART_NO_FULL.name: MappingPart.Columns.t_part_no_full.name,
    DataGroupType.PART_NO.name: MappingPart.Columns.t_part_no.name,
    DataGroupType.DATA_ID.name: MappingProcessData.Columns.t_data_id.name,
    DataGroupType.DATA_NAME.name: MappingProcessData.Columns.t_data_name.name,
    DataGroupType.DATA_ABBR.name: MappingProcessData.Columns.t_data_abbr.name,
    DataGroupType.UNIT.name: MappingProcessData.Columns.t_unit.name,
}

dict_config_bs_unique_key = {
    MProdFamily.get_table_name(): [
        MProdFamily.Columns.prod_family_factid.name,
        *MProdFamily.get_all_name_columns(),
        *MProdFamily.get_all_abbr_columns(),
    ],
    MProd.get_table_name(): [
        MProd.Columns.prod_factid.name,
        MProd.Columns.prod_family_id.name,
        *MProd.get_all_name_columns(),
        *MProd.get_all_abbr_columns(),
    ],
    MFunction.get_table_name(): [MFunction.Columns.function_type.name],
    MFactory.get_table_name(): [
        MFactory.Columns.factory_factid.name,
        *MFactory.get_all_name_columns(),
        *MFactory.get_all_abbr_columns(),
    ],
    MLocation.get_table_name(): [
        MLocation.Columns.location_abbr.name,
    ],  # [*MLocation.get_all_name_columns(), *MLocation.get_all_abbr_columns()],
    MPlant.get_table_name(): [
        MPlant.Columns.factory_id.name,
        *MPlant.get_all_name_columns(),
        *MPlant.get_all_abbr_columns(),
    ],
    MSect.get_table_name(): [
        MSect.Columns.dept_id.name,
        MSect.Columns.sect_factid.name,
        *MSect.get_all_name_columns(),
        *MSect.get_all_abbr_columns(),
    ],
    MDept.get_table_name(): [
        MDept.Columns.dept_factid.name,
        *MDept.get_all_name_columns(),
        *MDept.get_all_abbr_columns(),
    ],
    MLineGroup.get_table_name(): [
        *MLineGroup.get_all_name_columns(),
        *MLineGroup.get_all_abbr_columns(),
    ],
    MLine.get_table_name(): [
        MLine.Columns.plant_id.name,
        MLine.Columns.prod_family_id.name,
        MLine.Columns.line_group_id.name,
        MLine.Columns.line_factid.name,
        MLine.Columns.line_no.name,
    ],
    MProcess.get_table_name(): [
        MProcess.Columns.process_factid.name,
        MProcess.Columns.prod_family_id.name,
        *MProcess.get_all_name_columns(),
        *MProcess.get_all_abbr_columns(),
    ],
    MEquipGroup.get_table_name(): [
        *MEquipGroup.get_all_name_columns(),
        *MEquipGroup.get_all_abbr_columns(),
    ],
    MEquip.get_table_name(): [
        MEquip.Columns.equip_group_id.name,
        MEquip.Columns.equip_no.name,
        MEquip.Columns.equip_factid.name,
    ],
    MSt.get_table_name(): [
        MSt.Columns.equip_id.name,
        MSt.Columns.st_no.name,
        MSt.Columns.st_sign.name,
    ],
    MPart.get_table_name(): [
        MPart.Columns.part_type_id.name,
        MPart.Columns.part_factid.name,
        MPart.Columns.part_no.name,
    ],
    MPartType.get_table_name(): [
        MPartType.Columns.part_type_factid.name,
        *MPartType.get_all_name_columns(),
        *MPartType.get_all_abbr_columns(),
    ],
    MData.get_table_name(): [
        MData.Columns.data_factid.name,
        MData.Columns.process_id.name,
        MData.Columns.data_group_id.name,
        MData.Columns.unit_id.name,
    ],
    MUnit.get_table_name(): [MUnit.Columns.unit.name],
    MDataGroup.get_table_name(): [
        *MDataGroup.get_all_name_columns(),
        *MDataGroup.get_all_abbr_columns(),
    ],
    RFactoryMachine.get_table_name(): [
        RFactoryMachine.Columns.line_id.name,
        RFactoryMachine.Columns.process_id.name,
        RFactoryMachine.Columns.equip_id.name,
        RFactoryMachine.Columns.equip_st.name,
        RFactoryMachine.Columns.sect_id.name,
        RFactoryMachine.Columns.st_id.name,
    ],
    MappingFactoryMachine.get_table_name(): [
        MappingFactoryMachine.Columns.t_location_name.name,
        MappingFactoryMachine.Columns.t_location_abbr.name,
        MappingFactoryMachine.Columns.t_factory_id.name,
        MappingFactoryMachine.Columns.t_factory_name.name,
        MappingFactoryMachine.Columns.t_factory_abbr.name,
        MappingFactoryMachine.Columns.t_plant_id.name,
        MappingFactoryMachine.Columns.t_plant_name.name,
        MappingFactoryMachine.Columns.t_plant_abbr.name,
        MappingFactoryMachine.Columns.t_dept_id.name,
        MappingFactoryMachine.Columns.t_dept_name.name,
        MappingFactoryMachine.Columns.t_dept_abbr.name,
        MappingFactoryMachine.Columns.t_sect_id.name,
        MappingFactoryMachine.Columns.t_sect_name.name,
        MappingFactoryMachine.Columns.t_sect_abbr.name,
        MappingFactoryMachine.Columns.t_line_id.name,
        MappingFactoryMachine.Columns.t_line_no.name,
        MappingFactoryMachine.Columns.t_line_name.name,
        MappingFactoryMachine.Columns.t_outsource.name,
        MappingFactoryMachine.Columns.t_equip_id.name,
        MappingFactoryMachine.Columns.t_equip_name.name,
        MappingFactoryMachine.Columns.t_equip_product_no.name,
        MappingFactoryMachine.Columns.t_equip_product_date.name,
        MappingFactoryMachine.Columns.t_equip_no.name,
        MappingFactoryMachine.Columns.t_station_no.name,
        MappingFactoryMachine.Columns.t_prod_family_id.name,
        MappingFactoryMachine.Columns.t_prod_family_name.name,
        MappingFactoryMachine.Columns.t_prod_family_abbr.name,
        MappingFactoryMachine.Columns.t_prod_id.name,
        MappingFactoryMachine.Columns.t_prod_name.name,
        MappingFactoryMachine.Columns.t_prod_abbr.name,
        MappingFactoryMachine.Columns.t_process_id.name,
        MappingFactoryMachine.Columns.t_process_name.name,
        MappingFactoryMachine.Columns.t_process_abbr.name,
        MappingFactoryMachine.Columns.data_table_id.name,
        MappingFactoryMachine.Columns.factory_machine_id.name,
    ],
    MappingPart.get_table_name(): [
        MappingPart.Columns.t_part_type.name,
        MappingPart.Columns.t_part_name.name,
        MappingPart.Columns.t_part_abbr.name,
        MappingPart.Columns.t_part_no_full.name,
        MappingPart.Columns.t_part_no.name,
        MappingPart.Columns.data_table_id.name,
    ],
    MappingProcessData.get_table_name(): [
        MappingProcessData.Columns.t_process_id.name,
        MappingProcessData.Columns.t_process_name.name,
        MappingProcessData.Columns.t_process_abbr.name,
        MappingProcessData.Columns.t_data_id.name,
        MappingProcessData.Columns.t_data_name.name,
        MappingProcessData.Columns.t_data_abbr.name,
        MappingProcessData.Columns.t_prod_family_id.name,
        MappingProcessData.Columns.t_prod_family_name.name,
        MappingProcessData.Columns.t_prod_family_abbr.name,
        MappingProcessData.Columns.t_unit.name,
        MappingProcessData.Columns.data_table_id.name,
    ],
    RProdPart.get_table_name(): [
        RProdPart.Columns.prod_id.name,
        RProdPart.Columns.part_id.name,
    ],
    CfgProcessFunctionColumn.get_table_name(): [
        CfgProcessFunctionColumn.Columns.function_id.name,
        CfgProcessFunctionColumn.Columns.var_x.name,
        CfgProcessFunctionColumn.Columns.var_y.name,
        CfgProcessFunctionColumn.Columns.a.name,
        CfgProcessFunctionColumn.Columns.b.name,
        CfgProcessFunctionColumn.Columns.c.name,
        CfgProcessFunctionColumn.Columns.n.name,
        CfgProcessFunctionColumn.Columns.k.name,
        CfgProcessFunctionColumn.Columns.s.name,
        CfgProcessFunctionColumn.Columns.t.name,
        CfgProcessFunctionColumn.Columns.return_type.name,
    ],
    ETLService.THE_ALL: [
        MappingFactoryMachine.Columns.t_location_name.name,
        MappingFactoryMachine.Columns.t_location_abbr.name,
        MappingFactoryMachine.Columns.t_factory_id.name,
        MappingFactoryMachine.Columns.t_factory_name.name,
        MappingFactoryMachine.Columns.t_factory_abbr.name,
        MappingFactoryMachine.Columns.t_plant_id.name,
        MappingFactoryMachine.Columns.t_plant_name.name,
        MappingFactoryMachine.Columns.t_plant_abbr.name,
        MappingFactoryMachine.Columns.t_dept_id.name,
        MappingFactoryMachine.Columns.t_dept_name.name,
        MappingFactoryMachine.Columns.t_dept_abbr.name,
        MappingFactoryMachine.Columns.t_sect_id.name,
        MappingFactoryMachine.Columns.t_sect_name.name,
        MappingFactoryMachine.Columns.t_sect_abbr.name,
        MappingFactoryMachine.Columns.t_line_id.name,
        MappingFactoryMachine.Columns.t_line_no.name,
        MappingFactoryMachine.Columns.t_line_name.name,
        MappingFactoryMachine.Columns.t_outsource.name,
        MappingFactoryMachine.Columns.t_equip_id.name,
        MappingFactoryMachine.Columns.t_equip_name.name,
        MappingFactoryMachine.Columns.t_equip_product_no.name,
        MappingFactoryMachine.Columns.t_equip_product_date.name,
        MappingFactoryMachine.Columns.t_equip_no.name,
        MappingFactoryMachine.Columns.t_station_no.name,
        MappingFactoryMachine.Columns.t_prod_family_id.name,
        MappingFactoryMachine.Columns.t_prod_family_name.name,
        MappingFactoryMachine.Columns.t_prod_family_abbr.name,
        MappingFactoryMachine.Columns.t_prod_id.name,
        MappingFactoryMachine.Columns.t_prod_name.name,
        MappingFactoryMachine.Columns.t_prod_abbr.name,
        MappingFactoryMachine.Columns.t_process_id.name,
        MappingFactoryMachine.Columns.t_process_name.name,
        MappingFactoryMachine.Columns.t_process_abbr.name,
        MappingPart.Columns.t_part_type.name,
        MappingPart.Columns.t_part_name.name,
        MappingPart.Columns.t_part_abbr.name,
        MappingPart.Columns.t_part_no_full.name,
        MappingPart.Columns.t_part_no.name,
        MappingProcessData.Columns.t_data_id.name,
        MappingProcessData.Columns.t_data_name.name,
        MappingProcessData.Columns.t_data_abbr.name,
        MappingProcessData.Columns.t_unit.name,
    ],
}

BRIDGE_STATION_RELATION = {
    MappingPart: (MProdFamily, MProd, MLocation, MPartType, MPart, RProdPart),
    MappingProcessData: (MProdFamily, MProcess, MUnit, MDataGroup, MData),
    MappingFactoryMachine: (
        MLocation,
        MFactory,
        MPlant,
        MDept,
        MSect,
        MProdFamily,
        MProd,
        MLineGroup,
        MLine,
        MEquipGroup,
        MEquip,
        MSt,
        MProcess,
        RFactoryMachine,
    ),
}

# columns to copy to cfg process
BS_COMMON_PROCESS_COLUMNS = {  # generate cfg process columns
    DataGroupType.DATA_SERIAL: (DataGroupType.DATA_SERIAL.value, RawDataTypeDB.TEXT),
    DataGroupType.DATA_TIME: (DataGroupType.DATA_TIME.value, RawDataTypeDB.DATETIME),
    DataGroupType.LINE_ID: (DataGroupType.LINE_ID.value, MLine.Columns.line_factid.data_type),
    DataGroupType.PROCESS_ID: (
        DataGroupType.PROCESS_ID.value,
        MProcess.Columns.process_factid.data_type,
    ),
    DataGroupType.PART_NO: (DataGroupType.PART_NO.value, MPart.Columns.part_no.data_type),
    DataGroupType.EQUIP_ID: (DataGroupType.EQUIP_ID.value, MEquip.Columns.equip_factid.data_type),
    DataGroupType.DATA_ID: (DataGroupType.DATA_ID.value, RawDataTypeDB.TEXT),
    DataGroupType.DATA_VALUE: (DataGroupType.DATA_VALUE.value, RawDataTypeDB.TEXT),
    DataGroupType.LINE_NAME: (
        DataGroupType.LINE_NAME.value,
        MLineGroup.Columns.line_name_en.data_type,
    ),
    DataGroupType.PROCESS_NAME: (
        DataGroupType.PROCESS_NAME.value,
        MProcess.Columns.process_name_en.data_type,
    ),
    DataGroupType.EQUIP_NAME: (
        DataGroupType.EQUIP_NAME.value,
        MEquipGroup.Columns.equip_name_en.data_type,
    ),
    DataGroupType.DATA_NAME: (
        DataGroupType.DATA_NAME.value,
        MDataGroup.Columns.data_name_en.data_type,
    ),
    DataGroupType.FACTORY_ID: (
        DataGroupType.FACTORY_ID.value,
        MFactory.Columns.factory_factid.data_type,
    ),
    DataGroupType.FACTORY_NAME: (
        DataGroupType.FACTORY_NAME.value,
        MFactory.Columns.factory_name_en.data_type,
    ),
    DataGroupType.PLANT_ID: (DataGroupType.PLANT_ID.value, MPlant.Columns.plant_factid.data_type),
    DataGroupType.DEPT_ID: (DataGroupType.DEPT_ID.value, MDept.Columns.dept_factid.data_type),
    DataGroupType.DEPT_NAME: (DataGroupType.DEPT_NAME.value, MDept.Columns.dept_name_en.data_type),
    DataGroupType.PART_NO_FULL: (
        DataGroupType.PART_NO_FULL.value,
        MPart.Columns.part_factid.data_type,
    ),
    DataGroupType.EQUIP_NO: (DataGroupType.EQUIP_NO.value, MEquip.Columns.equip_no.data_type),
    DataGroupType.FACTORY_ABBR: (
        DataGroupType.FACTORY_ABBR.value,
        MFactory.Columns.factory_abbr_en.data_type,
    ),
    DataGroupType.PLANT_NAME: (
        DataGroupType.PLANT_NAME.value,
        MPlant.Columns.plant_name_en.data_type,
    ),
    DataGroupType.PLANT_ABBR: (
        DataGroupType.PLANT_ABBR.value,
        MPlant.Columns.plant_abbr_en.data_type,
    ),
    DataGroupType.PROD_FAMILY_ID: (
        DataGroupType.PROD_FAMILY_ID.value,
        MProdFamily.Columns.prod_family_factid.data_type,
    ),
    DataGroupType.PROD_FAMILY_NAME: (
        DataGroupType.PROD_FAMILY_NAME.value,
        MProdFamily.Columns.prod_family_name_en.data_type,
    ),
    DataGroupType.PROD_FAMILY_ABBR: (
        DataGroupType.PROD_FAMILY_ABBR.value,
        MProdFamily.Columns.prod_family_abbr_en.data_type,
    ),
    DataGroupType.OUTSOURCE: (DataGroupType.OUTSOURCE.value, MLine.Columns.outsource.data_type),
    DataGroupType.DEPT_ABBR: (DataGroupType.DEPT_ABBR.value, MDept.Columns.dept_abbr_en.data_type),
    DataGroupType.SECT_ID: (DataGroupType.SECT_ID.value, MSect.Columns.sect_factid.data_type),
    DataGroupType.SECT_NAME: (DataGroupType.SECT_NAME.value, MSect.Columns.sect_name_en.data_type),
    DataGroupType.SECT_ABBR: (DataGroupType.SECT_ABBR.value, MSect.Columns.sect_abbr_en.data_type),
    DataGroupType.PROD_ID: (DataGroupType.PROD_ID.value, MProd.Columns.prod_factid.data_type),
    DataGroupType.PROD_NAME: (DataGroupType.PROD_NAME.value, MProd.Columns.prod_name_en.data_type),
    DataGroupType.PROD_ABBR: (DataGroupType.PROD_ABBR.value, MProd.Columns.prod_abbr_en.data_type),
    DataGroupType.PART_TYPE: (
        DataGroupType.PART_TYPE.value,
        MPartType.Columns.part_type_factid.data_type,
    ),
    DataGroupType.PART_NAME: (
        DataGroupType.PART_NAME.value,
        MPartType.Columns.part_name_en.data_type,
    ),
    DataGroupType.PART_ABBR: (
        DataGroupType.PART_ABBR.value,
        MPartType.Columns.part_abbr_en.data_type,
    ),
    DataGroupType.EQUIP_PRODUCT_NO: (
        DataGroupType.EQUIP_PRODUCT_NO.value,
        MEquip.Columns.equip_product_no.data_type,
    ),
    DataGroupType.EQUIP_PRODUCT_DATE: (
        DataGroupType.EQUIP_PRODUCT_DATE.value,
        MEquip.Columns.equip_product_date.data_type,
    ),
    DataGroupType.STATION_NO: (DataGroupType.STATION_NO.value, MSt.Columns.st_no.data_type),
    DataGroupType.PROCESS_ABBR: (
        DataGroupType.PROCESS_ABBR.value,
        MProcess.Columns.process_abbr_en.data_type,
    ),
    DataGroupType.DATA_ABBR: (
        DataGroupType.DATA_ABBR.value,
        MDataGroup.Columns.data_abbr_en.data_type,
    ),
    DataGroupType.UNIT: (DataGroupType.UNIT.value, MUnit.Columns.unit.data_type),
    DataGroupType.LOCATION_NAME: (
        DataGroupType.LOCATION_NAME.value,
        MLocation.Columns.location_name_en.data_type,
    ),
    DataGroupType.LOCATION_ABBR: (
        DataGroupType.LOCATION_ABBR.value,
        MLocation.Columns.location_abbr.data_type,
    ),
    DataGroupType.LINE_NO: (DataGroupType.LINE_NO.value, MLine.Columns.line_no.data_type),
    DataGroupType.SUB_PART_NO: (DataGroupType.SUB_PART_NO.value, RawDataTypeDB.TEXT),
    DataGroupType.SUB_LOT_NO: (DataGroupType.SUB_LOT_NO.value, RawDataTypeDB.TEXT),
    DataGroupType.SUB_TRAY_NO: (DataGroupType.SUB_TRAY_NO.value, RawDataTypeDB.TEXT),
    DataGroupType.SUB_SERIAL: (DataGroupType.SUB_SERIAL.value, RawDataTypeDB.TEXT),
    DataGroupType.DATA_SOURCE_NAME: (DataGroupType.DATA_SOURCE_NAME.value, RawDataTypeDB.TEXT),
}

# ↓==== Split only exist masters of same data source ====↓
DUMMY_MASTER_DATA = {}


# ↑==== Split only exist masters of same data source ====↑


@log_execution_time()
@BridgeStationModel.use_db_instance_generator()
def scan_master(
    data_table_id: int,
    data_stream: Iterable[Tuple[Optional[DataFrame], Optional[list], Union[int, float]]] = None,
    return_new_master_dict: dict = None,
    db_instance: PostgreSQL = None,
    is_unknown_master: bool = False,
):
    yield 0

    cfg_data_table: BSCfgDataTable = BSCfgDataTable.get_by_id(db_instance, data_table_id, is_cascade=True)

    is_export_to_pickle_files = cfg_data_table.is_export_file()
    etl_service = ETLController.get_etl_service(cfg_data_table, db_instance=db_instance)
    _data_stream = (
        etl_service.get_master_data(db_instance=db_instance)
        if data_stream is None
        else etl_service.split_master_data(data_stream)
    )

    # support log
    dict_records_count = {}
    job_info = JobInfo()
    job_info_dict = {}
    yield job_info_dict  # yield to get job id from send_processing_info function
    job_info.job_id = job_info_dict.get(JOB_ID)

    # II. Do import collected data
    for (
        dict_target_and_df,
        percentage,
    ) in _data_stream:
        yield from extract_master_data_and_export_files(
            db_instance,
            cfg_data_table,
            job_info,
            dict_target_and_df,
            jump_percent=percentage,
            is_export_to_pickle_files=is_export_to_pickle_files,
            dict_records_count=dict_records_count,
            etl_service=etl_service,
            finish_percent=80,
            return_new_master_dict=return_new_master_dict,
        )

    if not is_export_to_pickle_files:
        dum_represent_master_column_name_raw_sql(db_instance)
        insert_default_filter_config_raw_sql(db_instance, data_table_id)
        yield 95
    else:
        from bridge.services.scan_data_type import scan_data_type

        generator = scan_data_type(data_table_id, data_steam=data_stream, db_instance=db_instance)
        send_processing_info(
            generator,
            JobType.SCAN_UNKNOWN_DATA_TYPE if is_unknown_master else JobType.SCAN_DATA_TYPE,
            data_table_id=data_table_id,
            is_check_disk=False,
            is_run_one_time=True,
        )
        dict_records_count.clear()
        db_instance.connection.rollback()
        # because register in mapping page run scan data type
        set_scan_data_type_status_done(db_instance, data_table_id)
        yield 95

    yield 100

    return dict_records_count, is_export_to_pickle_files


@log_execution_time()
def extract_master_data_and_export_files(
    db_instance: PostgreSQL,
    cfg_data_table: CfgDataTable,
    job_info: JobInfo,
    dict_target_and_df: dict[Type[Union[MappingProcessData, MappingPart, MappingFactoryMachine, str]], DataFrame],
    jump_percent: Union[float, int] = 100,
    is_export_to_pickle_files: bool = True,
    dict_records_count: dict[MasterModel, float] = None,
    etl_service: ETLService = None,
    finish_percent: Union[float, int] = 100,
    return_new_master_dict: dict = None,
) -> Iterator[JobInfo]:
    yield 1

    if dict_records_count is None:
        dict_records_count = {}

    # get master type
    master_type = cfg_data_table.get_master_type()

    is_direct_others = (
        cfg_data_table.data_source.is_direct_import
        and cfg_data_table.data_source.master_type == MasterDBType.OTHERS.name
    )
    folder_path = os.path.join(get_nayose_path(), str(cfg_data_table.id))
    df_word = get_word_prediction()
    has_data = False
    len_target = len(dict_target_and_df) + 1
    for idx, (mapping_model_cls, _df) in enumerate(
        dict_target_and_df.items(),
        start=1,
    ):  # type: int, (Type[Union[MappingProcessData, MappingPart, MappingFactoryMachine, str]], DataFrame)
        df = _df
        if df is None or df.empty or mapping_model_cls == ETLService.THE_ALL:
            continue

        copy_physical_column(df)

        if not DUMMY_MASTER_DATA:
            load_dict_dummy_master_data()

        if not is_export_to_pickle_files:
            # ignore rows that existing in mapping tables
            df = remove_existing_mapping(
                db_instance,
                cfg_data_table.id,
                df,
                mapping_model_cls,
                is_export_to_pickle_files,
            ).reset_index(drop=True)

        if df.empty:
            continue

        # 直交化
        if master_type == MasterDBType.OTHERS.name:
            general_transform_column(db_instance, df)
        elif master_type == MasterDBType.SOFTWARE_WORKSHOP.name:
            software_workshop_transform_column(db_instance, df)
        else:
            efa_v2_transform_column(db_instance, df, master_type, is_direct_others, is_export_to_pickle_files)

        # mapping_model_cls is mapping_xxx table. children_model_cls_s is m_xxx tables
        # ↓ ----- ↓
        children_model_cls_s = BRIDGE_STATION_RELATION[mapping_model_cls]
        visited = set()
        for master_model_cls in [*children_model_cls_s, mapping_model_cls]:
            if master_model_cls in visited:
                continue
            visited.add(master_model_cls)

            # Skip replace_word for case OTHER DIRECT
            if cfg_data_table.data_source.master_type != MasterDBType.OTHERS.name:
                # replace word to test v2-eFA same process, same column data
                replace_word(df, df_word, master_model_cls)

            # TODO : set language here to make sure use 1 language in a dataset
            #  ( currently, some column name is not in the csv file, so it can not detect ja well
            pre_processing(db_instance, df, master_model_cls)

            if df.empty:
                continue

            if master_model_cls is MData:
                df = insert_reserved_m_data(db_instance, cfg_data_table, df).dropna(
                    subset=[MData.Columns.data_group_id.name],
                )

            # change camel_to_snake for gen t_process_<process_system_name>
            if master_model_cls is MProcess:
                df[MProcess.Columns.process_name_sys.name] = df[MProcess.Columns.process_name_sys.name].apply(
                    camel_to_snake,
                )

            write_master_data(
                db_instance,
                df,
                master_model_cls,
                etl_service=etl_service,
                is_thoroughly_map_data=True,
                return_new_master_dict=return_new_master_dict,
                is_export_to_pickle_files=is_export_to_pickle_files,
            )

            if master_model_cls is MData:
                update_data_ids = []
                update_data_ids.extend(add_suffix_to_duplicate_data_name(db_instance, df, etl_service))
                if update_data_ids:
                    dict_cond = {
                        CfgProcessColumn.Columns.id.name: [(SqlComparisonOperator.IN, tuple(set(update_data_ids)))],
                    }
                    CfgProcessColumn.delete_by_condition(db_instance, dic_conditions=dict_cond, mode=0)
        # ↑ ----- ↑

        dict_records_count[mapping_model_cls] = len(df)

        # Keep 3 mapping df in dict_target_and_df to export to files later
        if is_export_to_pickle_files:
            dict_target_and_df[mapping_model_cls] = df
            has_data = True

        try:
            job_info.percent = round((idx * 100 / len_target) * (jump_percent / 100) * (finish_percent / 100), 2)
            yield job_info
        except Exception as e:
            logger.warning(e)

    # Export all relation mapping file
    if is_export_to_pickle_files and has_data and ETLService.THE_ALL in dict_target_and_df:
        export_mapping_data_to_files(dict_target_and_df, folder_path)

    job_info.percent = round(jump_percent * finish_percent / 100, 2)
    yield job_info


@multiprocessing_lock(MAPPING_DATA_LOCK)
def export_mapping_data_to_files(dict_target_and_df: dict, folder_path: str):
    for mapping_model_cls, _df in dict_target_and_df.items():
        df = _df.copy()
        key_file = mapping_model_cls.__name__ if mapping_model_cls is not ETLService.THE_ALL else ALL_DATA_RELATION
        file_path = os.path.join(
            folder_path,
            f'{NAYOSE_FILE_NAMES.get(key_file)}.{FileExtension.Feather.value}',
        )

        if mapping_model_cls is not ETLService.THE_ALL:
            if mapping_model_cls is MappingProcessData:
                df.dropna(subset=[f'{mapping_model_cls.__name__}_INDEX'], inplace=True)

            correct_type_cols = [col for col in df.columns if re.match('^[^t_].*_(id|INDEX)$|^data_group_type$', col)]
            df[correct_type_cols] = format_df(df[correct_type_cols])

        df = append_with_data_in_exist_file(file_path, df)
        write_nayose_data_to_file(df, file_path)
        logger.debug(f'[SCAN_MASTER] Export file: {file_path}')


@log_execution_time()
def rename_column_for_history_processes(db_instance: PostgreSQL, df: DataFrame, etl_service: ETLService) -> list:
    __BASE_NAME__ = '__BASE_NAME__'
    __NEW_NAME__ = '__NEW_NAME__'
    __PREFIX__ = '__PREFIX__'
    __MIDDLE__ = '__MIDDLE__'
    __SUFFIX__ = '__SUFFIX__'
    replace_name_dict = {
        DataGroupType.SUB_LOT_NO.name: 'ロット',
        DataGroupType.SUB_TRAY_NO.name: 'トレイ',
        DataGroupType.SUB_SERIAL.name: 'シリアル',
    }

    # Check duplicate data_name in one process
    df.reset_index(drop=True, inplace=True)
    check_duplicate_df = df[
        [
            MData.Columns.process_id.name,
            MDataGroup.Columns.data_name_jp.name,
            MDataGroup.Columns.data_name_sys.name,
            MData.Columns.data_group_id.name,
            MData.Columns.unit_id.name,
            MappingProcessData.Columns.data_id.name,
        ]
    ].drop_duplicates()

    exist_m_datas: list[MData] = MData.get_in_process_ids(
        db_instance,
        process_ids=check_duplicate_df.process_id.unique(),
        is_cascade=True,
    )
    exist_m_data_df = pd.DataFrame(
        [
            {
                MData.Columns.process_id.name: m.process_id,
                MDataGroup.Columns.data_name_jp.name: m.m_data_group.data_name_jp,
                MDataGroup.Columns.data_name_sys.name: m.m_data_group.data_name_sys,
                MData.Columns.data_group_id.name: m.data_group_id,
                MData.Columns.unit_id.name: m.unit_id,
                MappingProcessData.Columns.data_id.name: m.id,
            }
            for m in exist_m_datas
        ],
    )
    check_duplicate_df = check_duplicate_df.append(exist_m_data_df).drop_duplicates(
        subset=[MappingProcessData.Columns.data_id.name],
    )
    check_duplicate_df = check_duplicate_df[check_duplicate_df.data_group_id >= DataGroupType.HORIZONTAL_DATA.value]
    check_duplicate_df.reset_index(drop=True, inplace=True)

    check_duplicate_df[
        [__BASE_NAME__, MPartType.Columns.part_type_factid.name]
    ] = check_duplicate_df.data_name_sys.str.extract(r'^(.*)_(\d{6})$')
    check_duplicate_df = check_duplicate_df[check_duplicate_df[__BASE_NAME__].isin(replace_name_dict)]
    check_duplicate_df.sort_values(
        [MData.Columns.process_id.name, MappingProcessData.Columns.data_id.name],
        inplace=True,
    )
    check_duplicate_df[__PREFIX__] = '子'
    check_duplicate_df[__SUFFIX__] = check_duplicate_df[__BASE_NAME__].replace(replace_name_dict)
    check_duplicate_df[__MIDDLE__] = pd.NA
    for idx, df_process in check_duplicate_df.groupby(by=[MData.Columns.process_id.name]):
        check_duplicate_df[__MIDDLE__].update(
            (df_process.groupby(by=[MPartType.Columns.part_type_factid.name]).ngroup() + 1).astype(str),
        )
    check_duplicate_df[__NEW_NAME__] = (
        check_duplicate_df[__PREFIX__] + check_duplicate_df[__MIDDLE__] + check_duplicate_df[__SUFFIX__]
    )

    check_duplicate_df = check_duplicate_df[check_duplicate_df.data_name_jp != check_duplicate_df[__NEW_NAME__]]
    if check_duplicate_df.empty:
        return []

    check_duplicate_df.data_name_jp = check_duplicate_df[__NEW_NAME__]
    check_duplicate_df.data_group_id = pd.NA
    check_duplicate_df.drop(columns=[__BASE_NAME__, __NEW_NAME__, __PREFIX__, __MIDDLE__, __SUFFIX__], inplace=True)
    is_generate_data_series = df.data_group_id >= DataGroupType.HORIZONTAL_DATA.value
    merged_df = (
        df.reset_index()
        .merge(
            check_duplicate_df,
            on=[MappingProcessData.Columns.data_id.name],
            how='left',
            suffixes=Suffixes.KEEP_RIGHT,
        )
        .set_index('index')
    )
    # insert new m_data_group to get new data_group_id for the new names
    df_insert = merged_df[merged_df.data_group_id.isnull() & is_generate_data_series]
    if not df_insert.empty:
        write_master_data(db_instance, df_insert, MDataGroup, etl_service=etl_service, is_thoroughly_map_data=True)
        df.data_group_id.update(df_insert.data_group_id)

    # insert new m_data to get new data_id for the new names
    remove_data_ids = df_insert.data_id.drop_duplicates().astype(np.int64).to_list()
    if remove_data_ids:
        MData.delete_by_condition(
            db_instance,
            {MData.Columns.id.name: [(SqlComparisonOperator.IN, tuple(remove_data_ids))]},
            mode=0,
        )
        df_insert.drop(columns=[MappingProcessData.Columns.data_id.name], inplace=True)
        write_master_data(db_instance, df_insert, MData, etl_service=etl_service, is_thoroughly_map_data=True)
        df[MappingProcessData.Columns.data_id.name] = df[MappingProcessData.Columns.data_id.name].astype(
            pd.Int64Dtype(),
        )
        df[MappingProcessData.Columns.data_id.name].update(df_insert[MappingProcessData.Columns.data_id.name])

    return df_insert.data_id.drop_duplicates().astype(np.int64).to_list()


@log_execution_time()
def add_suffix_to_duplicate_data_name(db_instance: PostgreSQL, df: DataFrame, etl_service: ETLService) -> list:
    __ORIGIN_VALUE__ = '__ORIGIN_VALUE__'
    __SUFFIX__ = '__SUFFIX__'
    __NEW_SUFFIX__ = '__NEW_SUFFIX__'
    __NEW__ = '__NEW__'
    __NEW_SYSTEM__ = '__NEW_SYSTEM__'
    __IS_UPDATE_SYS__ = '__IS_UPDATE_SYS__'

    # Check duplicate data_name in one process
    df.reset_index(drop=True, inplace=True)
    check_duplicate_df = df[
        [
            MData.Columns.process_id.name,
            MDataGroup.Columns.data_name_jp.name,
            MDataGroup.Columns.data_name_sys.name,
            MData.Columns.data_group_id.name,
            MData.Columns.unit_id.name,
            MappingProcessData.Columns.data_id.name,
        ]
    ].drop_duplicates()

    exist_m_datas: list[MData] = MData.get_in_process_ids(
        db_instance,
        process_ids=check_duplicate_df[MData.Columns.process_id.name].unique(),
        is_cascade=True,
    )
    exist_m_data_df = pd.DataFrame(
        [
            {
                MData.Columns.process_id.name: m.process_id,
                MDataGroup.Columns.data_name_jp.name: m.m_data_group.data_name_jp,
                MDataGroup.Columns.data_name_sys.name: m.m_data_group.data_name_sys,
                MData.Columns.data_group_id.name: m.data_group_id,
                MData.Columns.unit_id.name: m.unit_id,
                MappingProcessData.Columns.data_id.name: m.id,
            }
            for m in exist_m_datas
        ],
    )
    check_duplicate_df = check_duplicate_df.append(exist_m_data_df).drop_duplicates(
        subset=[MappingProcessData.Columns.data_id.name],
    )
    check_duplicate_df = check_duplicate_df[
        check_duplicate_df[MData.Columns.data_group_id.name] >= DataGroupType.HORIZONTAL_DATA.value
    ]
    check_duplicate_df.reset_index(drop=True, inplace=True)

    # Check duplicate name jp
    grouped_df = check_duplicate_df.groupby(by=[MData.Columns.process_id.name, MData.Columns.data_group_id.name])
    duplicate_sys_group_series = grouped_df.transform('count')
    check_duplicate_series = duplicate_sys_group_series[MappingProcessData.Columns.data_id.name] > 1

    def _lambda_func(v):
        if pd.isnull(v):
            return np.nan, np.nan
        return (re.search(r'(.*)_(\d{2})$', v) or re.search(r'(.*)()$', v)).groups()

    if check_duplicate_series.any():
        split_series = check_duplicate_df[MDataGroup.Columns.data_name_jp.name].map(_lambda_func)
        check_duplicate_df[[__ORIGIN_VALUE__, __SUFFIX__]] = split_series.apply(
            pd.Series,
            index=[__ORIGIN_VALUE__, __SUFFIX__],
        )
        check_duplicate_df[__SUFFIX__] = pd.to_numeric(check_duplicate_df[__SUFFIX__], errors='coerce').convert_dtypes()
        for idx, grouped_df in check_duplicate_df.groupby(by=[MData.Columns.process_id.name, __ORIGIN_VALUE__]):
            if len(grouped_df) == 1:
                continue

            start_num = grouped_df[__SUFFIX__].max()
            if pd.isna(start_num):
                start_num = 1

            target_duplicate_df = grouped_df.sort_values([MappingProcessData.Columns.data_id.name])[1:]
            target_duplicate_df[__NEW_SUFFIX__] = [
                f'{n:0>2}' for n in np.arange(start_num, start_num + len(target_duplicate_df))
            ]
            target_duplicate_df[__NEW__] = (
                target_duplicate_df[__ORIGIN_VALUE__] + '_' + target_duplicate_df[__NEW_SUFFIX__]
            )
            target_duplicate_df = target_duplicate_df[
                target_duplicate_df[__NEW__] != target_duplicate_df[MDataGroup.Columns.data_name_jp.name]
            ]
            if not target_duplicate_df.empty:
                target_duplicate_df[__NEW_SYSTEM__] = (
                    target_duplicate_df[MDataGroup.Columns.data_name_sys.name]
                    + '_'
                    + target_duplicate_df[__NEW_SUFFIX__]
                )
                check_duplicate_df.loc[target_duplicate_df.index.to_list(), MData.Columns.data_group_id.name] = pd.NA
                check_duplicate_df[MDataGroup.Columns.data_name_jp.name].update(target_duplicate_df[__NEW__])
                check_duplicate_df[MDataGroup.Columns.data_name_sys.name].update(target_duplicate_df[__NEW_SYSTEM__])
        check_duplicate_df.drop(columns=[__ORIGIN_VALUE__, __SUFFIX__], inplace=True)

    # Check duplicate name sys
    grouped_df = check_duplicate_df.groupby(by=[MData.Columns.process_id.name, MDataGroup.Columns.data_name_sys.name])
    duplicate_sys_group_series = grouped_df.transform('count')
    is_duplicate_series = duplicate_sys_group_series[MappingProcessData.Columns.data_id.name] > 1
    if is_duplicate_series.any():
        split_series = check_duplicate_df[MDataGroup.Columns.data_name_sys.name].map(_lambda_func)
        check_duplicate_df[[__ORIGIN_VALUE__, __SUFFIX__]] = split_series.apply(
            pd.Series,
            index=[__ORIGIN_VALUE__, __SUFFIX__],
        )
        check_duplicate_df[__SUFFIX__] = pd.to_numeric(check_duplicate_df[__SUFFIX__], errors='coerce').convert_dtypes()
        for idx, grouped_df in check_duplicate_df.groupby(by=[MData.Columns.process_id.name, __ORIGIN_VALUE__]):
            if len(grouped_df) == 1:
                continue

            start_num = grouped_df[__SUFFIX__].max()
            if pd.isna(start_num):
                start_num = 1

            target_duplicate_df = grouped_df.sort_values([MappingProcessData.Columns.data_id.name])[1:]
            target_duplicate_df[__NEW_SUFFIX__] = [
                f'{n:0>2}' for n in np.arange(start_num, start_num + len(target_duplicate_df))
            ]
            target_duplicate_df[__NEW_SYSTEM__] = (
                target_duplicate_df[__ORIGIN_VALUE__] + '_' + target_duplicate_df[__NEW_SUFFIX__]
            )
            target_duplicate_df = target_duplicate_df[
                target_duplicate_df[__NEW_SYSTEM__] != target_duplicate_df[MDataGroup.Columns.data_name_sys.name]
            ]
            if not target_duplicate_df.empty:
                check_duplicate_df[MDataGroup.Columns.data_name_sys.name].update(target_duplicate_df[__NEW_SYSTEM__])
                check_duplicate_df.loc[target_duplicate_df.index.to_list(), __IS_UPDATE_SYS__] = True
        check_duplicate_df.drop(columns=[__ORIGIN_VALUE__, __SUFFIX__], inplace=True)

    if check_duplicate_df.data_group_id.isnull().any() or (
        __IS_UPDATE_SYS__ in check_duplicate_df and check_duplicate_df[__IS_UPDATE_SYS__].notnull().any()
    ):
        is_generate_data_series = df.data_group_id >= DataGroupType.HORIZONTAL_DATA.value
        merged_df = (
            df.reset_index()
            .merge(
                check_duplicate_df,
                on=[MappingProcessData.Columns.data_id.name],
                how='left',
                suffixes=Suffixes.KEEP_RIGHT,
            )
            .set_index('index')
        )

        # insert new m_data_group to get new data_group_id for the new names
        df_insert = merged_df[merged_df.data_group_id.isnull() & is_generate_data_series]
        if not df_insert.empty:
            write_master_data(
                db_instance,
                df_insert,
                MDataGroup,
                etl_service=etl_service,
                is_thoroughly_map_data=True,
            )
            merged_df.data_group_id.update(df_insert.data_group_id)
            df.data_group_id.update(df_insert.data_group_id)

        # update m_data_group
        df_update = None
        if __IS_UPDATE_SYS__ in merged_df:
            df_update = merged_df[merged_df[__IS_UPDATE_SYS__] == True & is_generate_data_series]
            dict_update = dict(
                df_update[[MData.Columns.data_group_id.name, MDataGroup.Columns.data_name_sys.name]]
                .drop_duplicates()
                .values,
            )
            marker = BridgeStationModel.get_parameter_marker()
            sql = f'''
            UPDATE {MDataGroup.get_table_name()}
            SET {MDataGroup.Columns.data_name_sys.name} = {marker}
            WHERE {MDataGroup.Columns.id.name} = {marker}
            '''
            for id, data_name_sys in dict_update.items():
                params = (data_name_sys, id)
                db_instance.execute_sql(sql, params)

        # insert new m_data to get new data_id for the new names
        remove_data_ids = df_insert.data_id.drop_duplicates().astype(np.int64).to_list()
        if remove_data_ids:
            MData.delete_by_condition(
                db_instance,
                {MData.Columns.id.name: [(SqlComparisonOperator.IN, tuple(remove_data_ids))]},
                mode=0,
            )
            df_insert.drop(columns=[MappingProcessData.Columns.data_id.name], inplace=True)
            write_master_data(db_instance, df_insert, MData, etl_service=etl_service, is_thoroughly_map_data=True)
            df[MappingProcessData.Columns.data_id.name] = df[MappingProcessData.Columns.data_id.name].astype(
                pd.Int64Dtype(),
            )
            df[MappingProcessData.Columns.data_id.name].update(df_insert[MappingProcessData.Columns.data_id.name])

        return list(
            set(
                df_insert.data_id.astype(np.int64).to_list() + []
                if df_update is None
                else df_update.data_id.dropna().astype(np.int64).to_list(),
            ),
        )

    return []


@log_execution_time()
def add_suffix_to_duplicate_data_name_sys(db_instance: PostgreSQL, df: DataFrame, etl_service: ETLService):
    # Check duplicate data_name_sys
    #   data_name_jp --> data_name_sys
    #   入り口        --> Iriguchi
    #   入口          --> Iriguchi_01
    data_name_sys = MDataGroup.Columns.data_name_sys.name
    df = MDataGroup.get_all_as_df(db_instance)
    df = df[df[MData.Columns.data_group_id.name] > DataGroupType.HORIZONTAL_DATA.value]
    df_origin = df.copy()
    # df.sort_values(by=[MDataGroup.Columns.id.name], inplace=True)
    suffix = '_'
    counter = df.groupby(data_name_sys).cumcount().add(0)
    df[data_name_sys] = df[data_name_sys].where(
        ~df[data_name_sys].duplicated(),
        df[data_name_sys] + suffix + counter.astype(str),
    )
    # df = df.rename(columns={MData.Columns.data_group_id.name: MDataGroup.Columns.id.name})
    diff_df = df.merge(df_origin, on=data_name_sys, how='outer', indicator=True, suffixes=Suffixes.KEEP_LEFT)
    diff_df = diff_df[diff_df['_merge'] != 'both'].drop('_merge', axis=1)
    ids = diff_df[MData.Columns.data_group_id.name].astype(np.int64).to_list()
    new_values = diff_df[data_name_sys].to_list()
    dict_update = dict(zip(ids, new_values))
    for update_id, value in dict_update.items():
        dic_update_values = {data_name_sys: value}
        MDataGroup.bulk_update_by_ids(db_instance, [update_id], dic_update_values)
    write_master_data(db_instance, df, MDataGroup, etl_service=etl_service, is_thoroughly_map_data=True)


def append_with_data_in_exist_file(file_path: str, df: DataFrame) -> DataFrame:
    if check_exist(file_path):
        saved_df = read_feather_file(file_path)
        saved_df = saved_df.fillna(pd.NA)
        df = df.fillna(pd.NA)
        bs_column_name = [str(data_name) for data_name in DataGroupType.get_values() if str(data_name) in df.columns]
        _df = df.drop(columns=bs_column_name)
        unique_cols = [
            col
            for col in _df.columns.tolist()
            if not (col.endswith('_id') and not col.startswith('t_') and col != CfgDataTableColumn.data_table_id.name)
        ]
        _df = pd.concat([_df, saved_df]).drop_duplicates(subset=unique_cols).reset_index(drop=True)

        logger.debug(f'[SCAN_MASTER] Combine with exist exported file: {file_path}')
        return _df
    else:
        return df


def write_nayose_data_to_file(df: DataFrame, file_path: str):
    bs_column_name = [str(data_name) for data_name in DataGroupType.get_values() if str(data_name) in df.columns]
    df.drop(columns=bs_column_name, inplace=True)
    write_feather_file(df, file_path)


# ↓==== Split only exist masters of same data source ====↓
def filter_master_same_datasource(
    db_instance: PostgreSQL,
    etl_service: ETLService,
    model_cls: MasterModel,
    df_existing_unique: DataFrame,
    foreign_id_col: str,
    skip_merge_with_different_data_sources: bool,
) -> DataFrame:
    if model_cls in DUMMY_MASTER_DATA.keys():
        if (
            etl_service.cfg_data_table.data_source.master_type == MasterDBType.OTHERS.name
            or skip_merge_with_different_data_sources
        ):
            data_tables = etl_service.cfg_data_table.data_source.data_tables
        else:
            # TODO: check this
            data_tables = []
            data_sources = BSCfgDataSource.get_data_source_efa_and_v2(db_instance, is_cascade=True)
            for data_source in data_sources:
                data_tables += data_source.data_tables

        data_table_ids = [data_table.id for data_table in data_tables]

        try:
            exist_masters = model_cls.get_unique_by_data_table_ids(
                db_instance=db_instance,
                data_table_ids=data_table_ids,
            )
        except Exception as e:
            raise e

        exist_master_ids = [exist_master.id for exist_master in exist_masters]
        dummy_master_ids = DUMMY_MASTER_DATA.get(model_cls)
        exist_master_ids = set(exist_master_ids + dummy_master_ids)
        df_existing_unique = df_existing_unique[df_existing_unique[foreign_id_col].isin(exist_master_ids)]

    return df_existing_unique


# ↑==== Split only exist masters of same data source ====↑


@log_execution_time()
def write_master_data(
    db_instance: PostgreSQL,
    df: DataFrame,
    model_cls: Type[MasterModel],
    is_publish_redis: bool = False,
    etl_service: ETLService = None,
    is_thoroughly_map_data: bool = False,
    return_new_master_dict: dict = None,
    is_export_to_pickle_files: bool = False,
):
    if df.empty:
        return df

    skip_merge_with_different_data_sources = model_cls is MProcess and etl_service.cfg_data_table.skip_merge

    # Add default sign value for 3 master tables
    if model_cls in [MLine, MEquip, MSt]:
        model_cls: Union[MLine, MEquip, MSt]
        sign_col = model_cls.get_sign_column().name
        if sign_col not in df:
            df[sign_col] = DEFAULT_NONE_VALUE
        df[sign_col] = df[sign_col].fillna(model_cls.get_default_sign_value())

    table_name = model_cls.get_table_name()
    config_bs_unique_key = dict_config_bs_unique_key.get(table_name)
    if not config_bs_unique_key:
        config_bs_unique_key = ['id']

    df_unique = get_df_unique(df, model_cls, config_bs_unique_key)
    if df_unique is None or not len(df_unique):
        return None

    _df = df[config_bs_unique_key].copy()
    _df[INDEX_COL] = _df.index
    df_existing_unique = model_cls.get_all_as_df(db_instance, is_convert_null_string_to_na=True).drop_duplicates(
        config_bs_unique_key,
        keep='last',
    )
    foreign_id_col = model_cls.get_foreign_id_column_name()
    if not is_export_to_pickle_files:
        df_existing_unique = filter_master_same_datasource(
            db_instance,
            etl_service,
            model_cls,
            df_existing_unique,
            foreign_id_col,
            skip_merge_with_different_data_sources,
        )

    convert_type_base_df(df_unique, df_existing_unique, config_bs_unique_key)
    # TODO: ignore process_data_id
    if foreign_id_col == 'process_data_id':
        foreign_id_col = MappingProcessData.Columns.data_id.name

    # Ignore mapping tables
    if 'id' in model_cls.Columns.get_column_names() and not getattr(model_cls, '__is_mapping_table__', None):
        # for master & relation tables
        if df_existing_unique.empty:
            df_unique[foreign_id_col] = DEFAULT_NONE_VALUE  # get_none_series(len(df_unique))
            df_insert = df_unique
        else:
            df_unique = df_unique.merge(
                df_existing_unique,
                how='left',
                on=config_bs_unique_key,
                suffixes=Suffixes.KEEP_LEFT,
            )
            if is_thoroughly_map_data:
                # 8. Transfer Upper case(大文字) => Lower case(小文字) for unmappable data
                map_master_data_by_lowercase_comparing(
                    df_unique,
                    df_existing_unique,
                    config_bs_unique_key,
                    foreign_id_col,
                )

                # 9. Compare system name for unmappable data
                unique_key = model_cls.get_sys_name_column()
                if unique_key:
                    map_master_data_by_system_name_comparing(
                        df_unique,
                        df_existing_unique,
                        [unique_key],
                        foreign_id_col,
                    )

                # 10. Remove all space, lowercase -> Compare name for unmappable data
                map_master_data_by_non_spacing_comparing(
                    df_unique,
                    df_existing_unique,
                    config_bs_unique_key,
                    foreign_id_col,
                )

            df_insert = df_unique[df_unique[foreign_id_col].isna()]

        if not df_insert.empty:
            df_insert['id'] = PostgresSequence.get_next_id_by_table(db_instance, table_name, len(df_insert))
            cols = [col for col in model_cls.Columns.get_column_names() if col in df_insert.columns]

            # TODO: determine model class have time columns
            if hasattr(model_cls.Columns, 'created_at'):
                time_cols = ['created_at', 'updated_at']
                for time_col in time_cols:
                    if time_col not in cols:
                        cols.append(time_col)
                df_insert[time_cols] = get_current_timestamp()

            df_rows = df_insert[cols]

            rows = convert_nan_to_none(df_rows, convert_to_list=True)
            db_instance.bulk_insert(table_name, cols, rows)

            df_unique.loc[df_unique[foreign_id_col].isna(), foreign_id_col] = df_insert['id']
            if (df_unique[foreign_id_col].isna()).any():
                logger.warning(' >>>>>> BUG HERE : import master but not found id <<<<<<')  # temp for debug

    else:
        # for 3 mapping tables
        df_existing_unique[INDEX_COL] = df_existing_unique.index
        df_duplicate = df_unique.dropna(how='all').merge(df_existing_unique, how='left')
        df_insert: DataFrame = df_duplicate[df_duplicate[INDEX_COL].isna()][df_unique.columns]
        if not df_insert.empty:
            if issubclass(model_cls, MappingProcessData):
                # Remove records that no process & no column
                subset_cols = [
                    MappingProcessData.Columns.t_process_id.name,
                    MappingProcessData.Columns.t_process_name.name,
                    MappingProcessData.Columns.t_process_abbr.name,
                    MappingProcessData.Columns.t_data_id.name,
                    MappingProcessData.Columns.t_data_name.name,
                    MappingProcessData.Columns.t_data_abbr.name,
                ]
                df_insert = df_insert.dropna(subset=subset_cols, how='all')
            if etl_service:  # Fix bug that etl_service is null when this function is called in Mapping Config page
                df_insert[model_cls.Columns.data_table_id.name] = etl_service.cfg_data_table.id
            rows = convert_nan_to_none(df_insert, convert_to_list=True)
            db_instance.bulk_insert(table_name, df_insert.columns.tolist(), rows)

    dict_log = {'File': global_file_name, 'Table': table_name, 'Insert': len(df_insert)}
    write_dict_to_csv(dict_log)

    convert_type_base_df(_df, df_unique, config_bs_unique_key)
    _temp = _df.merge(df_unique, how='left', on=config_bs_unique_key, suffixes=(None, '_y'))
    _temp.set_index(INDEX_COL, inplace=True)
    df[foreign_id_col] = _temp[foreign_id_col]

    # add new master ids to return back
    if not df_insert.empty and return_new_master_dict is not None and 'id' in df_insert:
        return_new_master_dict[table_name] = list(
            set(df_insert['id'].to_list() + return_new_master_dict.get(table_name, [])),
        )
        if table_name == MData.get_table_name():
            return_new_master_dict[NEW_COLUMN_PROCESS_IDS_KEY] = list(
                set(
                    df_insert[MData.Columns.process_id.name].astype(int).unique().tolist()
                    + return_new_master_dict.get(NEW_COLUMN_PROCESS_IDS_KEY, []),
                ),
            )
        logger.info(f'[NEW_MASTER] table: {table_name}  -  ids: {df_insert["id"].tolist()}')

    if not df_insert.empty and is_publish_redis:
        publish_master_config_changed(table_name=table_name, crud_type=CRUDType.INSERT.name)

    return len(df_insert)


def map_master_data_by_lowercase_comparing(
    df_source: DataFrame,
    df_target: DataFrame,
    unique_keys: list[str],
    foreign_id_col: str,
):
    def _lowercase(series: Series) -> Series:
        non_na_series = series[~series.isna()].convert_dtypes()
        if is_string_dtype(non_na_series):
            # Replace ":space()/_" to "_" & space character to empty
            non_na_series = (
                non_na_series.str.replace(r'[:\s()/_]+', HALF_WIDTH_SPACE, regex=True)
                .str.replace(r'\s+$', EMPTY_STRING, regex=True)
                .str.lower()
            )
            series.update(non_na_series)
        return series

    # lowercase all string col
    df_not_matched = df_source[df_source[foreign_id_col].isna()].copy()
    if df_not_matched.empty:
        return

    df_not_matched[INDEX_COL] = df_not_matched.index
    df_existing = df_target.copy()
    for col in unique_keys:
        if col.endswith('_id') and not col.startswith('t_'):
            continue
        df_not_matched[col] = _lowercase(df_not_matched[col])
        df_existing[col] = _lowercase(df_existing[col])

    df_not_matched = merge_df_not_matched_with_df_existing(df_not_matched, df_existing, unique_keys)
    df_source[foreign_id_col].update(df_not_matched[foreign_id_col])


def map_master_data_by_system_name_comparing(
    df_source: DataFrame,
    df_target: DataFrame,
    unique_keys: list[str],
    foreign_id_col: str,
):
    df_not_matched = df_source[df_source[foreign_id_col].isna()].copy().convert_dtypes()
    if df_not_matched.empty:
        return

    df_not_matched[INDEX_COL] = df_not_matched.index

    df_not_matched = merge_df_not_matched_with_df_existing(df_not_matched, df_target, unique_keys)
    df_source[foreign_id_col].update(df_not_matched[foreign_id_col])


def map_master_data_by_non_spacing_comparing(
    df_source: DataFrame,
    df_target: DataFrame,
    unique_keys: list[str],
    foreign_id_col: str,
):
    def _remove_all_space_and_lowercase(series: Series) -> Series:
        try:
            non_na_series = series[~series.isna()].convert_dtypes()
            if is_string_dtype(non_na_series):
                non_na_series = non_na_series.str.replace(r'\s', EMPTY_STRING, regex=True).str.lower()
                series.update(non_na_series)
        except Exception:
            pass
        return series

    df_not_matched = df_source[df_source[foreign_id_col].isna()].copy()
    if df_not_matched.empty:
        return

    df_not_matched[INDEX_COL] = df_not_matched.index
    df_existing = df_target.copy()
    for col in unique_keys:
        if col.endswith('_id') and not col.startswith('t_'):
            continue
        df_not_matched[col] = _remove_all_space_and_lowercase(df_not_matched[col])
        df_existing[col] = _remove_all_space_and_lowercase(df_existing[col])

    df_not_matched = merge_df_not_matched_with_df_existing(df_not_matched, df_existing, unique_keys)
    df_source[foreign_id_col].update(df_not_matched[foreign_id_col])


def merge_df_not_matched_with_df_existing(
    df_not_matched: pd.DataFrame,
    df_existing: pd.DataFrame,
    unique_keys: list[str],
) -> pd.DataFrame:
    # convert types
    for key in unique_keys:
        if key not in df_not_matched or key not in df_existing:
            raise KeyError(f'column {key} must exist in dataframe.')
        df_not_matched[key] = df_not_matched[key].astype(df_existing[key].dtype)

    # map with exist data
    df_not_matched = df_not_matched.merge(df_existing, how='left', on=unique_keys, suffixes=Suffixes.KEEP_LEFT)
    df_not_matched.set_index(INDEX_COL)
    return df_not_matched


def get_df_unique(df: DataFrame, model_cls: MasterModel, config_bs_unique_key: list[str]) -> DataFrame:
    # if column is in unique rule but not found in df. set to default none.
    cols = list(model_cls.Columns.get_column_names())
    for col in config_bs_unique_key:
        if col not in df.columns:
            df[col] = DEFAULT_NONE_VALUE

    cols = [col for col in cols if col in df.columns]
    df_unique = df.drop_duplicates(subset=config_bs_unique_key)[cols].convert_dtypes()

    return df_unique


@log_execution_time()
def pre_processing(
    db_instance: PostgreSQL,
    df: DataFrame,
    model_cls: Type[MasterModel],
) -> DataFrame:
    # update_bridge_station_language_dictionary()
    rename_default_column_to_language_column(df, model_cls)

    if model_cls is MPartType:
        unique_keys = dict_config_bs_unique_key[MPartType.get_table_name()].copy()
        if set(unique_keys) - set(df.columns):
            return df

        df_parts_type = MPartType.get_all_as_df(db_instance)
        df_parts_type[MPartType.Columns.assy_flag.name].dropna(inplace=True)
        if MPartType.Columns.assy_flag.name in df.columns:
            df_assy_flag = df[df[MPartType.Columns.assy_flag.name].isna()]
            df_assy_flag.drop(columns=MPartType.Columns.assy_flag.name, inplace=True)
        else:
            df_assy_flag = df
            df[MPartType.Columns.assy_flag.name] = False

        df_assy_flag = df_assy_flag.merge(df_parts_type, how='inner', on=unique_keys, suffixes=Suffixes.KEEP_LEFT)
        df[MPartType.Columns.assy_flag.name] = (
            df[MPartType.Columns.assy_flag.name].replace({DEFAULT_NONE_VALUE: False}).astype(bool)
        )
        df.loc[df_assy_flag.index, MPartType.Columns.assy_flag.name] = df_assy_flag[MPartType.Columns.assy_flag.name]
    elif model_cls is MUnit:
        # insert null record to MUnit if it does not exist
        selection = [MUnit.Columns.id.name]
        dic_conditions = {MUnit.Columns.unit.name: NULL_DEFAULT_STRING}
        cols, rows = MUnit.select_records(db_instance, dic_conditions, select_cols=selection, row_is_dict=False)
        if not rows:
            MUnit.insert_record(db_instance, {MUnit.Columns.unit.name: NULL_DEFAULT_STRING})
    elif model_cls is MData:
        if MData.Columns.unit_id.name in df.columns:
            non_unit_id = MUnit.get_empty_unit_id(db_instance)
            df[MData.Columns.unit_id.name].fillna(non_unit_id)
        if MData.Columns.data_type.name in df.columns:
            df[MData.Columns.data_type.name].fillna(DataType.TEXT.value)
    elif model_cls is MDataGroup:
        condition_regx = ''
        for col in [
            DataGroupType.SUB_SERIAL.name,
            DataGroupType.SUB_TRAY_NO.name,
            DataGroupType.SUB_LOT_NO.name,
            DataGroupType.SUB_PART_NO.name,
        ]:
            condition_regx += rf'{"|" if condition_regx else ""}^{col}_\d' + r'{6}$'
        sub_name_series = df[MDataGroup.Columns.data_name_jp.name].str.match(condition_regx)
        df[MDataGroup.Columns.data_name_sys.name].update(df[sub_name_series][MDataGroup.Columns.data_name_jp.name])


@log_execution_time()
def insert_reserved_m_data(
    db_instance: PostgreSQL,
    cfg_data_table: CfgDataTable,
    df: DataFrame,
    is_direct_others: bool = False,
) -> DataFrame:
    if MData.Columns.process_id.name not in df.columns:
        return df

    df_process = df.drop_duplicates(subset=[MData.Columns.process_id.name])[[MData.Columns.process_id.name]]
    process_ids = list(df_process[MData.Columns.process_id.name].dropna(how='all').values)

    m_data_s = MData.get_in_process_ids(db_instance, process_ids)
    existing_m_process_ids = [m_data.process_id for m_data in m_data_s]

    df_process = df_process[~df_process[MData.Columns.process_id.name].isin(existing_m_process_ids)]
    if not len(df_process):
        return df

    # get non unit id
    from ap.setting_module.models import MDataGroup as EdgeMDataGroup

    bridge_station_common_columns = {
        data_group.value: configs[1].value
        # TODO: Change to get from CfgDataTableColumn
        for data_group, configs in list(BS_COMMON_PROCESS_COLUMNS.items())
        if data_group
        not in [
            DataGroupType.SUB_PART_NO,
            DataGroupType.SUB_TRAY_NO,
            DataGroupType.SUB_LOT_NO,
            DataGroupType.SUB_SERIAL,
        ]
    }

    # check serial in cfg_data_table
    is_has_serial = cfg_data_table.is_has_serial_col()
    dummy_master_columns = BaseMasterColumn.get_dummy_column_name()
    data_table_columns = cfg_data_table.columns

    del bridge_station_common_columns[DataGroupType.DATA_ID.value]
    del bridge_station_common_columns[DataGroupType.DATA_NAME.value]
    del bridge_station_common_columns[DataGroupType.DATA_VALUE.value]
    del bridge_station_common_columns[DataGroupType.DATA_ABBR.value]

    if cfg_data_table.data_source.master_type == MasterDBType.EFA.name:
        master_column_ids = [column.data_group_type for column in data_table_columns]
        master_column_ids.append(DataGroupType.DATA_SOURCE_NAME.value)
        bridge_station_common_columns = {
            key: bridge_station_common_columns[key] for key in bridge_station_common_columns if key in master_column_ids
        }
    else:
        dummy_master_column_ids = [
            column.data_group_type for column in data_table_columns if column.column_name in dummy_master_columns
        ]
        if dummy_master_column_ids:
            bridge_station_common_columns = {
                key: bridge_station_common_columns[key]
                for key in bridge_station_common_columns
                if key not in dummy_master_column_ids
            }

    if is_has_serial:
        bridge_station_common_columns[DataGroupType.DATA_SERIAL.value] = None
    else:
        del bridge_station_common_columns[DataGroupType.DATA_SERIAL.value]

    if not cfg_data_table.get_date_col():
        del bridge_station_common_columns[DataGroupType.DATA_TIME.value]

    m_data_groups = EdgeMDataGroup.get_data_group_in_group_types(list(bridge_station_common_columns.keys()))

    data_group_ids = [row.id for row in m_data_groups]
    data_types = [bridge_station_common_columns.get(row.data_group_type) for row in m_data_groups]

    non_unit_id = MUnit.get_empty_unit_id(db_instance)
    df_reserved_m_data = DataFrame(
        {
            MData.Columns.data_group_id.name: data_group_ids,
            MData.Columns.data_type.name: data_types,
            MData.Columns.unit_id.name: non_unit_id,
        },
    )

    df_reserved_m_data = df_process.merge(df_reserved_m_data, how='cross')
    df = df.append(df_reserved_m_data, ignore_index=True)

    return df


@log_execution_time()
def gen_name_sys_column(df, model_cls, is_local_language=False):
    name_sys_col = model_cls.get_sys_name_column()
    default_name_col = model_cls.get_default_name_column()
    if not name_sys_col:
        return
    if name_sys_col not in df.columns and default_name_col in df.columns:
        is_not_null_series = df[default_name_col].notnull()
        filtered_series = df[is_not_null_series][default_name_col]
        if is_local_language:
            filtered_series = filtered_series.apply(anyascii)
        filtered_series = filtered_series.apply(to_romaji, remote_underline=True)
        df.loc[is_not_null_series, name_sys_col] = filtered_series


def rename_default_column_to_language_column(df, model_cls):
    """
    Rename from default column (ex: line_name) to suitable language column (ex: line_name_jp, line_name_ex)
    And generate system name column (ex: line_name_sys)

    :param df:
    :param model_cls:
    :return:
    """
    from_column, to_column = detect_language_in_df(df, model_cls)
    is_local_lang = to_column == model_cls.get_local_name_column()
    gen_name_sys_column(df, model_cls, is_local_lang)
    if to_column:
        df.rename(columns={from_column: to_column}, inplace=True)


@log_execution_time()
def detect_language_in_df(df, model_cls):
    default_name_col = model_cls.get_default_name_column()
    from_column, to_column = default_name_col, None
    if default_name_col in df.columns:
        # candidate_langs = detect_language(df, default_name_col)
        # TODO: HARD CODE ja . because there is a bug that it always chooses local.
        # best_prob_lang = candidate_langs[0].lang if candidate_langs else 'ja'  # temp default ja.
        best_prob_lang = 'ja'  # test todo remove
        name_col = model_cls.pick_column_by_language(best_prob_lang, mode='I')
        to_column = name_col
    return from_column, to_column


def detect_language_str_and_pick_column(model_cls, _str):
    candidate_langs = detect_language_str(_str)
    best_prob_lang = candidate_langs[0].lang
    name_col = model_cls.pick_column_by_language(best_prob_lang, mode='I')
    return name_col


def dum_data_from_files():
    with BridgeStationModel.get_db_proxy() as db_instance:
        dump_from_csv(db_instance, get_dummy_data_path(), is_truncate=False, extensions=['csv', 'tsv'])


# ↓==== Split only exist masters of same data source ====↓
@memoize()
def load_dict_dummy_master_data():
    map_model_dict = {
        # 'm_column_group': MColumnGroup,
        # 'm_config_equation': MColumnGroup,
        'm_data': MData,
        'm_data_group': MDataGroup,
        'm_dept': MDept,
        'm_equip': MEquip,
        'm_equip_group': MEquipGroup,
        'm_factory': MFactory,
        # 'm_function': MFunction,
        # 'm_group': MGroup,
        'm_line': MLine,
        'm_line_group': MLineGroup,
        'm_location': MLocation,
        'm_part': MPart,
        'm_part_type': MPartType,
        'm_plant': MPlant,
        'm_process': MProcess,
        'm_prod': MProd,
        'm_prod_family': MProdFamily,
        'm_sect': MSect,
        'm_st': MSt,
        'm_unit': MUnit,
    }

    files = list(glob.glob(f'{get_dummy_data_path()}/*.tsv'))
    files.sort()
    for file_relative_path in files:
        file_name_with_ext = os.path.basename(file_relative_path)
        split_name = file_name_with_ext.split('.')
        if len(split_name) < 3:
            print(f'file {split_name} has wrong format name')
            continue
        table_name = split_name[1]
        table_model = map_model_dict.get(table_name)
        if not table_model:
            continue

        data = read_data(file_relative_path)
        headers = next(data)
        next(data)
        next(data)
        rows = list(data)
        df = pd.DataFrame(rows, columns=headers)
        if 'id' in df:
            if table_model is MFactory:  # special case
                DUMMY_MASTER_DATA[table_model] = [1]
                continue
            DUMMY_MASTER_DATA[table_model] = df.id.astype(int).to_list()


# ↑==== Split only exist masters of same data source ====↑


@log_execution_time()
def copy_physical_column(df: DataFrame):
    for bridge_station_column, mapping_key_column in dict_config_bs_name.items():
        # bridge_station_column (same with PrimaryGroups) use for logic handling
        # mapping_key_column use for insert to physical db
        if bridge_station_column in df.columns:
            df[mapping_key_column] = df[bridge_station_column]


@log_execution_time()
def remove_existing_mapping(
    db_instance: PostgreSQL,
    data_table_id: int,
    df: DataFrame,
    model_cls: Type[Union[MappingProcessData, MappingPart, MappingFactoryMachine, str]],
    is_export_to_pickle_files: bool = True,
) -> DataFrame:
    df_mapping_data: DataFrame
    if model_cls != ETLService.THE_ALL:
        # in case model_cls is Union[MappingProcessData, MappingPart, MappingFactoryMachine] class
        table_name = model_cls.get_table_name()
        df_mapping_data = model_cls.get_all_as_df(db_instance)
    else:
        # in case of ETLService.THE_ALL, model_cls is string
        table_name = model_cls
        df_mapping_data = (
            pd.DataFrame(columns=dict_config_bs_unique_key[table_name]) if is_export_to_pickle_files else pd.DataFrame()
        )

    if not df_mapping_data.empty:
        config_bs_unique_key = dict_config_bs_unique_key[table_name]
        keys = [key for key in config_bs_unique_key if key in df.columns]
        if CfgDataTableColumn.data_table_id.name in df_mapping_data.columns:
            df_mapping_data = df_mapping_data[df_mapping_data[CfgDataTableColumn.data_table_id.name] == data_table_id]

        df = df.merge(df_mapping_data[keys], how='left', on=keys, indicator=True)
        df = df[df['_merge'] == 'left_only'].drop('_merge', axis=1)

    return df


def replace_word(df: DataFrame, df_word: DataFrame, model_cls: Type[MasterModel]):
    if model_cls.get_table_name() not in df_word['column'].to_list():
        return
    df_word_replace = df_word[df_word['column'] == model_cls.get_table_name()]

    text_columns = model_cls.Columns.get_column_name_by_data_type([DataType.TEXT])
    text_columns = [column for column in text_columns if column in df.columns]

    default_name_column = model_cls.get_default_name_column()
    if default_name_column and default_name_column in df.columns:
        text_columns.append(default_name_column)

    if text_columns:
        dict_convert = dict(zip(df_word_replace['left'], df_word_replace['right']))
        df[text_columns] = df[text_columns].replace(dict_convert)


def general_transform_column(db_instance: PostgreSQL, df: DataFrame):
    units = None
    for source_column, bs_columns in dict_config_general_transform_column.items():
        if source_column not in df:
            continue

        # Add passing list unit to compare when extracting data name to separate parts
        extend_args = {}
        if source_column == DataGroupType.DATA_NAME.name:
            # lazy load all units in m_unit table
            if not units:
                units = MUnit.get_all_units(db_instance)
            extend_args['units'] = units

        transform_column(df, source_column, bs_columns, master_type=MasterDBType.OTHERS, **extend_args)


def software_workshop_transform_column(db_instance: PostgreSQL, df: DataFrame):
    units = None
    for source_column, bs_columns in dict_config_etl_software_workshop_transform_column.items():
        if source_column not in df:
            continue

        # Add passing list unit to compare when extracting data name to separate parts
        extend_args = {}
        if source_column == DataGroupType.DATA_NAME.name:
            # lazy load all units in m_unit table
            if not units:
                units = MUnit.get_all_units(db_instance)
            extend_args['units'] = units

        is_exist_two_unit_value = source_column == DataGroupType.UNIT.name
        transform_column(
            df,
            source_column,
            bs_columns,
            master_type=MasterDBType.SOFTWARE_WORKSHOP,
            is_exist_two_unit_value=is_exist_two_unit_value,
            **extend_args,
        )


def efa_v2_transform_column(
    db_instance: PostgreSQL,
    df: DataFrame,
    master_type: str,
    is_direct_others: bool,
    is_export_to_pickle_files=False,
):
    units = None
    m_prod_df = None
    m_prod_family_df = None

    # nayose
    # ↓ ----- ↓
    for (
        source_column,
        bs_columns,
    ) in dict_config_efa_v2_transform_column.items():  # type: str, list[str]
        if source_column not in df or (
            source_column == DataGroupType.DATA_ID.name
            and master_type != MasterDBType.EFA_HISTORY.name
            and not is_export_to_pickle_files
        ):
            # only get 5 last character of quality_id for EFA_HISTORY master type
            continue

        # Add passing list unit to compare when extracting data name to separate parts
        extend_args = {}
        if source_column == DataGroupType.DATA_ID.name and master_type == MasterDBType.EFA_HISTORY.name:
            extend_args['is_efa_history'] = True
        elif source_column == DataGroupType.DATA_NAME.name:
            # lazy load all units in m_unit table
            if not units:
                units = MUnit.get_all_units(db_instance)
            extend_args['units'] = units
        elif source_column == DataGroupType.LINE_NAME.name:
            extend_args['is_direct_others'] = is_direct_others
            # lazy load all data in m_prod table
            if m_prod_df is None:
                m_prod_df = MProd.get_all_as_df(db_instance)
            extend_args['m_prod_df'] = m_prod_df
            # lazy load all data in m_prod_family table
            if m_prod_family_df is None:
                m_prod_family_df = MProdFamily.get_all_as_df(db_instance)
            extend_args['m_prod_family_df'] = m_prod_family_df

        elif source_column == DataGroupType.EQUIP_ID.name and is_direct_others:
            extend_args['is_direct_others'] = True

        transform_column(df, source_column, bs_columns, **extend_args)
    # ↑ ----- ↑


@log_execution_time()
def transform_column(
    df: DataFrame,
    from_column: str,
    to_columns: list,
    ignore_cols: list = None,
    master_type: MasterDBType = MasterDBType.V2,
    is_exist_two_unit_value: bool = False,
    **extend_args,
):
    default_index_column_name = '%%_INDEX_%%'
    df[default_index_column_name] = df.index

    origin_series = df[from_column].drop_duplicates().reset_index(drop=True)
    handle_series = normalize_series(origin_series).replace(
        dict.fromkeys(PANDAS_DEFAULT_NA | NA_VALUES, DEFAULT_NONE_VALUE),
    )

    # Add passing list unit to compare when extracting data name to separate parts
    if master_type == MasterDBType.OTHERS:
        handle_series = handle_series.apply(
            general_extract_master(from_column, **extend_args),
        )
    elif master_type == MasterDBType.SOFTWARE_WORKSHOP:
        handle_series = handle_series.apply(
            etl_software_workshop_extract_master(from_column, **extend_args),
        )
    else:
        handle_series = handle_series.apply(
            efa_v2_extract_master(from_column, **extend_args),
        )

    handle_df = handle_series.apply(pd.Series, index=to_columns)
    handle_df[from_column] = origin_series

    if is_exist_two_unit_value:
        # In case unit value also exist in DATA_NAME column and UNIT master, unit value in DATA_NAME is upper priority.
        df.update(
            df[[from_column, default_index_column_name]]
            .merge(handle_df, on=from_column)
            .set_index(default_index_column_name)[to_columns],
            overwrite=False,
        )
    else:
        df[to_columns] = (
            df[[from_column, default_index_column_name]]
            .merge(handle_df, on=from_column)
            .set_index(default_index_column_name)[to_columns]
        )
    df.drop(default_index_column_name, axis=1, inplace=True)

    if 'data_group_name' in df:
        df.dropna(subset=['data_group_name'], inplace=True)
    if ignore_cols:
        df.drop(columns=ignore_cols, inplace=True)

    # Correct dtype for small int columns
    for small_int_col in [MLine.Columns.line_no.name, MEquip.Columns.equip_no.name, MSt.Columns.st_no.name]:
        if small_int_col in df:
            df[small_int_col] = pd.to_numeric(df[small_int_col], errors='coerce').astype(pd.Int16Dtype())


@log_execution_time()
@BridgeStationModel.use_db_instance()
def gen_m_data_manual(
    data_type: str,
    non_unit_id: int,
    data_name_sys: str,
    data_name_jp: str,
    data_name_en: str,
    process_id: int,
    data_group_type: int,
    process_function_column_id: int = None,
    db_instance: PostgreSQL = None,
):
    # m_data_group
    # Keep use GENERATED_EQUATION for m_data
    dict_data_group = {
        MDataGroup.Columns.data_name_sys.name: data_name_sys,
        MDataGroup.Columns.data_name_jp.name: data_name_jp,
        MDataGroup.Columns.data_name_en.name: data_name_en,
        MDataGroup.Columns.data_group_type.name: data_group_type,
    }
    cols, rows = MDataGroup.select_records(db_instance, dict_data_group, row_is_dict=True)
    if not rows:
        dict_data_group['created_at'] = get_current_timestamp()
        dict_data_group['updated_at'] = get_current_timestamp()
        dict_data_group['id'] = PostgresSequence.get_next_id_by_table(db_instance, MDataGroup.get_table_name())[0]
        MDataGroup.insert_record(db_instance, dict_data_group, is_return_id=True)
        data_group_id = dict_data_group['id']
    else:
        data_group_id = rows[0][MDataGroup.Columns.id.name]

    dict_m_data = {
        MData.Columns.data_group_id.name: data_group_id,
        MData.Columns.process_id.name: process_id,
        MData.Columns.data_type.name: data_type,
        MData.Columns.unit_id.name: non_unit_id,
    }

    if process_function_column_id:
        dict_m_data[MData.Columns.config_equation_id.name] = process_function_column_id

    cols, rows = MData.select_records(db_instance, dict_m_data, row_is_dict=True)
    if rows:
        data_id = rows[0][ID]
    else:
        dict_m_data['created_at'] = get_current_timestamp()
        dict_m_data['updated_at'] = get_current_timestamp()
        dict_m_data['id'] = PostgresSequence.get_next_id_by_table(db_instance, MData.get_table_name())[0]
        data_id = MData.insert_record(db_instance, dict_m_data, is_return_id=True)

    return {
        'data_group_id': data_group_id,
        'data_id': data_id,
    }


def dum_represent_master_column_name():
    from ap.setting_module.models import MData as ESMData

    with make_session() as meta_session:
        df_m_data = ESMData.get_all_as_df()
        key_names = dict_config_bs_unique_key.get(ESMData.get_table_name())

        for process_id, df in df_m_data.groupby(by=ESMData.process_id.name):
            series_group_id = df[ESMData.data_group_id.name].unique()
            for column_meta in MasterColumnMetaCatalog.non_representative_column_meta():
                if column_meta.group.value in series_group_id:
                    new_m_data = ESMData()
                    new_m_data.process_id = process_id
                    new_m_data.data_group_id = column_meta.represent_group().value
                    new_m_data.data_type = DataType.TEXT.value
                    new_m_data.unit_id = 1
                    insert_or_update_config(meta_session, new_m_data, key_names)


def dum_represent_master_column_name_raw_sql(db_instance: PostgreSQL):
    df_m_data = MData.get_all_as_df(db_instance)
    key_names = dict_config_bs_unique_key.get(MData.get_table_name())

    columns = [
        MData.Columns.process_id.name,
        MData.Columns.data_group_id.name,
        MData.Columns.data_type.name,
        MData.Columns.unit_id.name,
    ]
    insert_items = []
    empty_unit_id = MUnit.get_empty_unit_id(db_instance)
    for process_id, df in df_m_data.groupby(by=MData.Columns.process_id.name):
        series_group_id = df[MData.Columns.data_group_id.name].unique()
        for column_meta in MasterColumnMetaCatalog.non_representative_column_meta():
            if column_meta.group.value in series_group_id:
                insert_items.append(
                    (
                        process_id,
                        column_meta.represent_group().value,
                        DataType.TEXT.value,
                        empty_unit_id,
                    ),
                )

    df_insert = pd.DataFrame(insert_items, columns=columns).drop_duplicates()
    df_insert = df_insert.merge(df_m_data, how='left', on=columns, suffixes=Suffixes.KEEP_LEFT)
    df_insert = df_insert[df_insert[MData.get_foreign_id_column_name()].isnull()].drop_duplicates(subset=key_names)
    insert_cols = [col.name for col in MData.Columns if col.name != 'id']
    df_insert = df_insert[insert_cols]
    df_insert[[MData.Columns.created_at.name, MData.Columns.updated_at.name]] = get_current_timestamp()
    if df_insert.empty:
        return

    rows = convert_nan_to_none(df_insert, convert_to_list=True)
    db_instance.bulk_insert(MData.get_table_name(), insert_cols, rows)


# create if not existing. return id.
def create_or_get_id(db_instance: PostgreSQL, dict_cond: dict) -> Union[str, int]:
    cols, rows = CfgProcessFunctionColumn.select_records(db_instance, dict_cond, row_is_dict=True)
    if not rows:
        process_function_column_id = CfgProcessFunctionColumn.insert_record(db_instance, dict_cond, is_return_id=True)
    else:
        process_function_column_id = rows[0][CfgProcessFunctionColumn.Columns.id.name]
    return process_function_column_id


dict_lang = {'jp': set(), 'en': set(), 'local': set()}


def update_bridge_station_language_dictionary():
    master_model_cls_s = MasterModel.get_all_subclasses()

    with BridgeStationModel.get_db_proxy() as db_instance:
        for model_cls in master_model_cls_s:  # type: MasterModel
            jp_column = model_cls.get_jp_name_column()
            en_column = model_cls.get_en_name_column()
            local_column = model_cls.get_local_name_column()
            if not (jp_column or en_column or local_column):
                continue
            df = model_cls.get_all_as_df(db_instance)
            for dict_key, col in zip(('jp', 'en', 'local'), (jp_column, en_column, local_column)):
                if not col:
                    continue
                existing_voc = df[col].values().unique().dropna()
                dict_lang[dict_key].update(set(existing_voc))


# todo: temp to measure executetime. no use in main code
def write_dict_to_csv(dict_log):
    global global_df
    global_df = global_df.append(dict_log, ignore_index=True)
    global_df.to_csv('master_count.tsv', header=True, index=False, sep='\t', mode='w')
