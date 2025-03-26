from ap import list_id_alias
from bridge.models.cfg_constant import CfgConstantModel
from bridge.models.cfg_csv_column import CfgCsvColumn
from bridge.models.cfg_data_source import CfgDataSource
from bridge.models.cfg_data_source_csv import CfgDataSourceCSV
from bridge.models.cfg_data_source_db import CfgDataSourceDB
from bridge.models.cfg_filter import CfgFilter
from bridge.models.cfg_filter_detail import CfgFilterDetail
from bridge.models.cfg_process import CfgProcess
from bridge.models.cfg_process_column import CfgProcessColumn
from bridge.models.cfg_process_function_column import CfgProcessFunctionColumn
from bridge.models.cfg_trace import CfgTrace
from bridge.models.cfg_trace_key import CfgTraceKey
from bridge.models.cfg_user_setting import CfgUserSetting
from bridge.models.cfg_visualization import CfgVisualization
from bridge.models.m_data import MData
from bridge.models.m_data_group import MDataGroup
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
from bridge.models.m_unit import MUnit
from bridge.models.mapping_factory_machine import MappingFactoryMachine
from bridge.models.mapping_part import MappingPart
from bridge.models.mapping_process_data import MappingProcessData
from bridge.models.r_factory_machine import RFactoryMachine
from bridge.models.r_prod_part import RProdPart
from bridge.models.t_csv_import import CsvImport
from bridge.models.t_factory_import import FactoryImport
from bridge.models.t_job_management import JobManagement

CfgDataSource.children_model = {
    CfgDataSourceDB: {(CfgDataSourceDB.Columns.id): (CfgDataSource.Columns.id)},
    CfgDataSourceCSV: {(CfgDataSourceCSV.Columns.id): (CfgDataSource.Columns.id)},
}

CfgDataSourceCSV.children_model = {CfgCsvColumn: {(CfgCsvColumn.Columns.data_source_id): (CfgDataSourceCSV.Columns.id)}}

CfgProcess.children_model = {
    CfgTrace: {
        (CfgTrace.Columns.self_process_id): (CfgProcess.Columns.id),
        (CfgTrace.Columns.target_process_id): (CfgProcess.Columns.id),
    },
    CfgProcessColumn: {(CfgProcessColumn.Columns.process_id): (CfgProcess.Columns.id)},
}

CfgProcessColumn.children_model = {
    CfgTraceKey: {
        (CfgTraceKey.Columns.self_column_id): (CfgProcessColumn.Columns.id),
        (CfgTraceKey.Columns.target_column_id): (CfgProcessColumn.Columns.id),
    },
}

CfgTrace.children_model = {CfgTraceKey: {(CfgTraceKey.Columns.trace_id): (CfgTrace.Columns.id)}}

insertion_order_config = [
    CfgDataSource,
    CfgDataSourceDB,
    CfgDataSourceCSV,
    CfgCsvColumn,
    CfgProcess,
    CfgProcessColumn,
    CfgTrace,
    CfgTraceKey,
    CfgFilter,
    CfgFilterDetail,
    CfgUserSetting,
]

insertion_order_master = (
    MLocation,
    MProdFamily,
    MProd,
    MFactory,
    MPlant,
    MDept,
    MSect,
    MLineGroup,
    MLine,
    MEquip,
    MEquipGroup,
    MProcess,
    MPartType,
    MPart,
    MDataGroup,
    MData,
    MUnit,
    MFunction,
    CfgProcessFunctionColumn,
    RFactoryMachine,
    RProdPart,
    MappingPart,
    MappingFactoryMachine,
    MappingProcessData,
)


def get_foreign_ids():
    result = []
    for cls in insertion_order_master:
        if cls in [MappingPart, MappingFactoryMachine, MappingProcessData]:
            continue
        result.append(cls.get_foreign_id_column_name())
    return result


table_class_map = {
    'cfg_constant': CfgConstantModel,
    'cfg_csv_column': CfgCsvColumn,
    'cfg_data_source': CfgDataSource,
    'cfg_data_source_csv': CfgDataSourceCSV,
    'cfg_data_source_db': CfgDataSourceDB,
    'cfg_filter': CfgFilter,
    'cfg_filter_detail': CfgFilterDetail,
    'cfg_process': CfgProcess,
    'cfg_process_column': CfgProcessColumn,
    'cfg_trace': CfgTrace,
    'cfg_trace_key': CfgTraceKey,
    'cfg_visualization': CfgVisualization,
    't_csv_import': CsvImport,
    't_factory_import': FactoryImport,
    't_job_management': JobManagement,
}

list_id_alias.update(set(get_foreign_ids()))  # temp. todo refactor
