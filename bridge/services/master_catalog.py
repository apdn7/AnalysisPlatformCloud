from functools import lru_cache
from typing import Iterator, Optional

from ap import logger
from ap.common.constants import DataGroupType
from ap.setting_module import models as ap_models
from ap.setting_module.models import MasterDBModel
from bridge.models.bridge_station import MasterModel
from bridge.models.m_dept import MDept
from bridge.models.m_equip import MEquip
from bridge.models.m_factory import MFactory
from bridge.models.m_line import MLine
from bridge.models.m_part import MPart
from bridge.models.m_part_type import MPartType
from bridge.models.m_plant import MPlant
from bridge.models.m_process import MProcess
from bridge.models.m_prod import MProd
from bridge.models.m_prod_family import MProdFamily
from bridge.models.m_sect import MSect
from bridge.models.m_st import MSt
from bridge.services.hover.base import BaseHoverModel
from bridge.services.hover.dept import DeptHoverModel
from bridge.services.hover.equip import EquipHoverModel
from bridge.services.hover.factory import FactoryHoverModel
from bridge.services.hover.line import LineHoverModel
from bridge.services.hover.part import PartHoverModel
from bridge.services.hover.part_type import PartTypeHoverModel
from bridge.services.hover.plant import PlantHoverModel
from bridge.services.hover.process import ProcessHoverModel
from bridge.services.hover.prod import ProdHoverModel
from bridge.services.hover.prod_family import ProdFamilyHoverModel
from bridge.services.hover.sect import SectHoverModel
from bridge.services.hover.st import StHoverModel
from bridge.services.sql import mapping_master
from bridge.services.sql.mapping_master import RelationMaster


class AbstractColumnMeta:
    group: DataGroupType
    ap_model: type[MasterDBModel]
    bridge_model: type[MasterModel]
    relation_model: type[RelationMaster]
    hover_model: type[BaseHoverModel]
    represent: bool = False

    @classmethod
    def children_classes(cls) -> Iterator[type['AbstractColumnMeta']]:
        for child_class in cls.__subclasses__():
            yield child_class

    @classmethod
    def represent_column_meta(cls) -> type['AbstractColumnMeta']:
        current_cls = cls
        while not isinstance(current_cls, AbstractColumnMeta):
            if current_cls.represent:
                return current_cls
            current_cls = current_cls.__base__
        raise Exception(f'represent column meta is not found for {cls}')

    @classmethod
    def represent_group(cls) -> DataGroupType:
        return cls.represent_column_meta().group


class MasterColumnMetaCatalog:
    _instance: dict[DataGroupType, AbstractColumnMeta] = {}
    _populated: bool = False

    @classmethod
    def _populate(cls) -> None:
        if cls._populated:
            return
        cls._populated = True
        cls._populate_root(AbstractColumnMeta())

    @classmethod
    def _populate_root(cls, root: AbstractColumnMeta) -> None:
        for child_class in root.children_classes():
            if child_class.group in cls._instance:
                # We searched this group, don't need to go further
                return

            child = child_class()
            assert child.group is not None

            cls._instance[child.group] = child
            cls._populate_root(child)

    @classmethod
    def instance(cls, group: DataGroupType) -> Optional[AbstractColumnMeta]:
        cls._populate()
        result = cls._instance.get(group)
        if result is None:
            logger.error(f"group {group} doesn't exists")
        return result

    @classmethod
    @lru_cache(None)
    def non_representative_column_meta(cls) -> list[AbstractColumnMeta]:
        cls._populate()
        values = []
        for key, value in cls._instance.items():
            if not value.represent:
                values.append(value)
        return values

    @classmethod
    @lru_cache(None)
    def representative_column_meta(cls) -> list[AbstractColumnMeta]:
        cls._populate()
        values = []
        for key, value in cls._instance.items():
            if value.represent:
                values.append(value)
        return values


class RelationMasterSingleton:
    """Get cte for specific master data group type"""

    _instance: dict[DataGroupType, RelationMaster] = {}

    @classmethod
    def instance(cls, data_group_type: DataGroupType) -> Optional[RelationMaster]:
        master_column_meta = MasterColumnMetaCatalog.instance(data_group_type)
        represent_group = master_column_meta.represent_group()
        if represent_group not in cls._instance:
            cls._instance[represent_group] = master_column_meta.relation_model(master_column_meta.ap_model)
        return cls._instance[represent_group]


class FactoryColumnMeta(AbstractColumnMeta):
    group = DataGroupType.FACTORY
    ap_model = ap_models.MFactory
    bridge_model = MFactory
    relation_model = mapping_master.RelationMFactory
    hover_model = FactoryHoverModel
    represent = True


class FactoryIdColumnMeta(FactoryColumnMeta):
    group = DataGroupType.FACTORY_ID
    represent = False


class FactoryNameColumnMeta(FactoryColumnMeta):
    group = DataGroupType.FACTORY_NAME
    represent = False


class FactoryAbbrColumnMeta(FactoryColumnMeta):
    group = DataGroupType.FACTORY_ABBR
    represent = False


class PlantColumnMeta(AbstractColumnMeta):
    group = DataGroupType.PLANT
    ap_model = ap_models.MPlant
    bridge_model = MPlant
    relation_model = mapping_master.RelationMPlant
    hover_model = PlantHoverModel
    represent = True


class PlantIdColumnMeta(PlantColumnMeta):
    group = DataGroupType.PLANT_ID
    represent = False


class PlantNameColumnMeta(PlantColumnMeta):
    group = DataGroupType.PLANT_NAME
    represent = False


class PlantAbbrColumnMeta(PlantColumnMeta):
    group = DataGroupType.PLANT_ABBR
    represent = False


class ProdFamilyColumnMeta(AbstractColumnMeta):
    group = DataGroupType.PROD_FAMILY
    ap_model = ap_models.MProdFamily
    bridge_model = MProdFamily
    relation_model = mapping_master.RelationMProdFamily
    hover_model = ProdFamilyHoverModel
    represent = True


class ProdFamilyIdColumnMeta(ProdFamilyColumnMeta):
    group = DataGroupType.PROD_FAMILY_ID
    represent = False


class ProdFamilyNameColumnMeta(ProdFamilyColumnMeta):
    group = DataGroupType.PROD_FAMILY_NAME
    represent = False


class ProdFamilyAbbrColumnMeta(ProdFamilyColumnMeta):
    group = DataGroupType.PROD_FAMILY_ABBR
    represent = False


class LineColumnMeta(AbstractColumnMeta):
    group = DataGroupType.LINE
    ap_model = ap_models.MLine
    bridge_model = MLine
    relation_model = mapping_master.RelationMLine
    hover_model = LineHoverModel
    represent = True


class LineIdColumnMeta(LineColumnMeta):
    group = DataGroupType.LINE_ID
    represent = False


class LineNameColumnMeta(LineColumnMeta):
    group = DataGroupType.LINE_NAME
    represent = False


class LineNoColumnMeta(LineColumnMeta):
    group = DataGroupType.LINE_NO
    represent = False


class OutsourceColumnMeta(LineColumnMeta):
    group = DataGroupType.OUTSOURCE
    represent = False


class DeptColumnMeta(AbstractColumnMeta):
    group = DataGroupType.DEPT
    ap_model = ap_models.MDept
    bridge_model = MDept
    relation_model = mapping_master.RelationMDept
    hover_model = DeptHoverModel
    represent = True


class DeptIdColumnMeta(DeptColumnMeta):
    group = DataGroupType.DEPT_ID
    represent = False


class DeptNameColumnMeta(DeptColumnMeta):
    group = DataGroupType.DEPT_NAME
    represent = False


class DeptAbbrColumnMeta(DeptColumnMeta):
    group = DataGroupType.DEPT_ABBR
    represent = False


class SectColumnMeta(AbstractColumnMeta):
    group = DataGroupType.SECT
    ap_model = ap_models.MSect
    bridge_model = MSect
    relation_model = mapping_master.RelationMSect
    hover_model = SectHoverModel
    represent = True


class SectIdColumnMeta(SectColumnMeta):
    group = DataGroupType.SECT_ID
    represent = False


class SectNameColumnMeta(SectColumnMeta):
    group = DataGroupType.SECT_NAME
    represent = False


class SectAbbrColumnMeta(SectColumnMeta):
    group = DataGroupType.SECT_ABBR
    represent = False


class ProdColumnMeta(AbstractColumnMeta):
    group = DataGroupType.PRODUCT
    ap_model = ap_models.MProd
    bridge_model = MProd
    relation_model = mapping_master.RelationMProd
    hover_model = ProdHoverModel
    represent = True


class ProdIdColumnMeta(ProdColumnMeta):
    group = DataGroupType.PROD_ID
    represent = False


class ProdNameColumnMeta(ProdColumnMeta):
    group = DataGroupType.PROD_NAME
    represent = False


class ProdAbbrColumnMeta(ProdColumnMeta):
    group = DataGroupType.PROD_ABBR
    represent = False


class PartTypeRepresentColumnMeta(AbstractColumnMeta):
    group = DataGroupType.PARTTYPE
    ap_model = ap_models.MPartType
    bridge_model = MPartType
    relation_model = mapping_master.RelationMPartType
    hover_model = PartTypeHoverModel
    represent = True


class PartTypeColumnMeta(PartTypeRepresentColumnMeta):
    group = DataGroupType.PART_TYPE
    represent = False


class PartNameColumnMeta(PartTypeRepresentColumnMeta):
    group = DataGroupType.PART_NAME
    represent = False


class PartAbbrColumnMeta(PartTypeRepresentColumnMeta):
    group = DataGroupType.PART_ABBR
    represent = False


class PartColumnMeta(AbstractColumnMeta):
    group = DataGroupType.PART
    ap_model = ap_models.MPart
    bridge_model = MPart
    relation_model = mapping_master.RelationMPart
    hover_model = PartHoverModel
    represent = True


class PartNoColumnMeta(PartColumnMeta):
    group = DataGroupType.PART_NO
    represent = False


class PartNoFullColumnMeta(PartColumnMeta):
    group = DataGroupType.PART_NO_FULL
    represent = False


class EquipColumnMeta(AbstractColumnMeta):
    group = DataGroupType.EQUIP
    ap_model = ap_models.MEquip
    bridge_model = MEquip
    relation_model = mapping_master.RelationMEquip
    hover_model = EquipHoverModel
    represent = True


class EquipIdColumnMeta(EquipColumnMeta):
    group = DataGroupType.EQUIP_ID
    represent = False


class EquipNameColumnMeta(EquipColumnMeta):
    group = DataGroupType.EQUIP_NAME
    represent = False


class EquipNoColumnMeta(EquipColumnMeta):
    group = DataGroupType.EQUIP_NO
    represent = False


class EquipProductNoColumnMeta(EquipColumnMeta):
    group = DataGroupType.EQUIP_PRODUCT_NO
    represent = False


class EquipProductDateColumnMeta(EquipColumnMeta):
    group = DataGroupType.EQUIP_PRODUCT_DATE
    represent = False


class StationColumnMeta(AbstractColumnMeta):
    group = DataGroupType.STATION
    ap_model = ap_models.MSt
    bridge_model = MSt
    relation_model = mapping_master.RelationMSt
    hover_model = StHoverModel
    represent = True


class StationNoColumnMeta(StationColumnMeta):
    group = DataGroupType.STATION_NO
    represent = False


class ProcessColumnMeta(AbstractColumnMeta):
    group = DataGroupType.PROCESS
    ap_model = ap_models.MProcess
    bridge_model = MProcess
    relation_model = mapping_master.RelationMProcess
    hover_model = ProcessHoverModel
    represent = True


class ProcessIdColumnMeta(ProcessColumnMeta):
    group = DataGroupType.PROCESS_ID
    represent = False


class ProcessNameColumnMeta(ProcessColumnMeta):
    group = DataGroupType.PROCESS_NAME
    represent = False


class ProcessAbbrColumnMeta(ProcessColumnMeta):
    group = DataGroupType.PROCESS_ABBR
    represent = False
