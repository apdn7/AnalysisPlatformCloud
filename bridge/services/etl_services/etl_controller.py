from __future__ import annotations

from enum import Enum, auto
from typing import Optional, Union

from ap.common.constants import MasterDBType
from ap.common.pydn.dblib.postgresql import PostgreSQL
from ap.setting_module.models import CfgDataTable
from bridge.models.cfg_data_table import CfgDataTable as BSCfgDataTable
from bridge.services.etl_services.etl_csv_service import EtlCsvService
from bridge.services.etl_services.etl_db_long_service import DBLongService
from bridge.services.etl_services.etl_db_service import EtlDbService
from bridge.services.etl_services.etl_efa_service import EFAService
from bridge.services.etl_services.etl_service import ETLService
from bridge.services.etl_services.etl_software_workshop_services import SoftwareWorkshopService
from bridge.services.etl_services.etl_v2_history_service import V2HistoryService
from bridge.services.etl_services.etl_v2_measure_service import V2MeasureService
from bridge.services.etl_services.etl_v2_multi_history_service import V2MultiHistoryService
from bridge.services.etl_services.etl_v2_multi_measure_service import V2MultiMeasureService


class ETLDataSourceType(Enum):
    CsvWideGeneral = auto()
    WideOther = auto()
    CsvLongGeneral = auto()

    DbWideGeneral = auto()
    DbLongGeneral = auto()

    Efa = auto()
    EfaHistory = auto()

    V2 = auto()
    V2Multi = auto()
    V2History = auto()
    V2MultiHistory = auto()
    SoftwareWorkshop = auto()

    @classmethod
    def from_master_db_type(cls, master_type: MasterDBType) -> Optional['ETLDataSourceType']:
        if master_type is MasterDBType.EFA:
            return cls.Efa
        if master_type is MasterDBType.EFA_HISTORY:
            return cls.EfaHistory
        if master_type is MasterDBType.V2:
            return cls.V2
        if master_type is MasterDBType.V2_MULTI:
            return cls.V2Multi
        if master_type is MasterDBType.V2_HISTORY:
            return cls.V2History
        if master_type is MasterDBType.V2_MULTI_HISTORY:
            return cls.V2MultiHistory
        if master_type is MasterDBType.SOFTWARE_WORKSHOP:
            return cls.SoftwareWorkshop
        return None


class ETLController:
    @staticmethod
    def get_etl_service(
        cfg_data_table: Union[CfgDataTable, BSCfgDataTable],
        db_instance: PostgreSQL = None,
    ) -> ETLService | None:
        # use method like this : etl_service.is_horizon_data()

        etl_data_source_type = ETLController.get_etl_data_source_type(cfg_data_table, db_instance=db_instance)

        if etl_data_source_type is ETLDataSourceType.V2:
            return V2MeasureService(cfg_data_table, db_instance=db_instance)

        if etl_data_source_type is ETLDataSourceType.V2Multi:
            return V2MultiMeasureService(cfg_data_table, db_instance=db_instance)

        if etl_data_source_type is ETLDataSourceType.V2History:
            return V2HistoryService(cfg_data_table, db_instance=db_instance)

        if etl_data_source_type is ETLDataSourceType.V2MultiHistory:
            return V2MultiHistoryService(cfg_data_table, db_instance=db_instance)

        if etl_data_source_type in [ETLDataSourceType.Efa, ETLDataSourceType.EfaHistory]:
            return EFAService(cfg_data_table, db_instance=db_instance)

        if etl_data_source_type in [ETLDataSourceType.SoftwareWorkshop]:
            return SoftwareWorkshopService(cfg_data_table, db_instance=db_instance)

        # if etl_data_source_type is ETLDataSourceType.EfaHistory:
        #     raise NotImplementedError('Confirm about this case')

        if etl_data_source_type is ETLDataSourceType.CsvWideGeneral:
            return EtlCsvService(cfg_data_table, db_instance=db_instance)

        if etl_data_source_type is ETLDataSourceType.DbWideGeneral:
            return EtlDbService(cfg_data_table, db_instance=db_instance)

        if etl_data_source_type is ETLDataSourceType.CsvLongGeneral:
            # TODO: return v2 service at the moment, will change in the future
            return V2MeasureService(cfg_data_table, db_instance=db_instance)

        if etl_data_source_type is ETLDataSourceType.DbLongGeneral:
            return DBLongService(cfg_data_table, db_instance=db_instance)

        if etl_data_source_type is ETLDataSourceType.WideOther:
            raise NotImplementedError

        return None

    @staticmethod
    def get_etl_data_source_type(
        cfg_data_table: Union[CfgDataTable, BSCfgDataTable],
        db_instance: PostgreSQL = None,
    ) -> ETLDataSourceType:
        """Logic to guess etl data source
        If direct import:
            - long type: v2, efa, etc
            - no long type: wide general
        else:
            - long type: long general
            - no long type: wide general
        """

        etl_service = ETLService(cfg_data_table, db_instance=db_instance)
        # assert etl_service.cfg_data_table.data_source.is_direct_import, (
        #     "currently, we set is_direct_import = True, however we want to change this into enum type later"
        # )

        master_type = getattr(MasterDBType, etl_service.master_type, None)

        etl_data_source_type = ETLDataSourceType.from_master_db_type(master_type)
        if etl_data_source_type is not None:
            return etl_data_source_type

        is_horizon_data = etl_service.is_horizon_data()

        # TODO: refactor this later, use explicit boolean type
        is_csv_detail = etl_service.cfg_data_table.data_source.csv_detail
        if is_csv_detail:
            return ETLDataSourceType.CsvWideGeneral if is_horizon_data else ETLDataSourceType.CsvLongGeneral
        return ETLDataSourceType.DbWideGeneral if is_horizon_data else ETLDataSourceType.DbLongGeneral
