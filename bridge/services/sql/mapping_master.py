"""Mapping from r_factory_machine to master join relation
"""
from typing import Any, Final, Iterator, Optional, Union

import sqlalchemy as sa
from sqlalchemy.sql import Join, Select
from sqlalchemy.sql import functions as func
from sqlalchemy.sql.selectable import CTE

from ap.common.common_utils import camel_to_snake
from ap.setting_module.models import (
    MasterDBModel,
    MDept,
    MEquip,
    MEquipGroup,
    MFactory,
    MLine,
    MLineGroup,
    MPart,
    MPartType,
    MPlant,
    MProcess,
    MProd,
    MProdFamily,
    MSect,
    MSt,
    RFactoryMachine,
    RProdPart,
)


class RelationMaster:
    default_column_aliases: Final[list[str]] = ['id', 'name', 'master_id']
    r_table: Union[type[RProdPart], type[RFactoryMachine]]
    r_column_name: str

    def __init__(self, master_model: type[MasterDBModel]):
        self.master_model = master_model
        self.cte: CTE = self.stmt().cte(self.cte_aliased())
        self.verify()

    @property
    def id(self) -> sa.Column:
        return self.cte.c.get('id')

    @property
    def name(self) -> sa.Column:
        return self.cte.c.get('name')

    @property
    def master_id(self) -> sa.Column:
        return self.cte.c.get('master_id')

    @classmethod
    def columns(cls) -> list[Any]:
        """Must implement at child class"""
        raise NotImplementedError

    @classmethod
    def join_condition(cls) -> Optional[Join]:
        """Must implement at child class"""
        raise NotImplementedError

    @classmethod
    def cte_aliased(cls) -> str:
        return camel_to_snake(cls.__name__)

    @staticmethod
    def _assign_labels(columns: list[Any]) -> Iterator[Any]:
        assert len(columns) == len(RelationMaster.default_column_aliases)
        for column, alias in zip(columns, RelationMaster.default_column_aliases):
            yield column.label(alias)

    def stmt(self) -> Select:
        # get all selected columns
        columns = self.columns()

        # add one more ap model column
        master_id_column = getattr(self.master_model, 'id')
        assert master_id_column is not None
        columns.append(master_id_column)

        # rename all columns
        columns_aliased = self._assign_labels(columns)

        join_conditions = self.join_condition()
        stmt = sa.select(columns_aliased)
        if join_conditions is not None:
            stmt = stmt.select_from(join_conditions)
        return stmt

    @classmethod
    def verify(cls) -> None:
        assert cls.r_table is not None
        assert cls.r_column_name is not None


class RelationMFactory(RelationMaster):
    r_table = RFactoryMachine
    r_column_name = 'line_id'

    @classmethod  # noqa
    def columns(cls) -> list[Any]:
        return [
            MLine.id,
            func.coalesce(
                MFactory.factory_abbr_jp,
                MFactory.factory_abbr_en,
                MFactory.factory_abbr_local,
                MFactory.factory_name_jp,
                MFactory.factory_name_en,
                MFactory.factory_name_local,
                MFactory.factory_factid,
            ),
        ]

    @classmethod
    def join_condition(cls) -> Optional[Join]:
        """m_factory -> m_plant -> m_line -> r_factory_machine"""
        return sa.join(left=MFactory, right=MPlant, onclause=MFactory.id == MPlant.factory_id).join(
            right=MLine,
            onclause=MPlant.id == MLine.plant_id,
        )


class RelationMPlant(RelationMaster):
    r_table = RFactoryMachine
    r_column_name = 'line_id'

    @classmethod  # noqa
    def columns(cls) -> list[Any]:
        return [
            MLine.id,
            func.coalesce(
                MPlant.plant_abbr_jp,
                MPlant.plant_abbr_en,
                MPlant.plant_abbr_local,
                MPlant.plant_name_jp,
                MPlant.plant_name_en,
                MPlant.plant_name_local,
                MPlant.plant_factid,
            ),
        ]

    @classmethod
    def join_condition(cls) -> Optional[Join]:
        """m_plant -> m_line -> r_factory_machine"""
        return sa.join(left=MPlant, right=MLine, onclause=MPlant.id == MLine.plant_id)


class RelationMProdFamily(RelationMaster):
    r_table = RFactoryMachine
    r_column_name = 'line_id'

    @classmethod  # noqa
    def columns(cls) -> list[Any]:
        return [
            MLine.id,
            func.coalesce(
                MProdFamily.prod_family_abbr_jp,
                MProdFamily.prod_family_abbr_en,
                MProdFamily.prod_family_abbr_local,
                MProdFamily.prod_family_name_jp,
                MProdFamily.prod_family_name_en,
                MProdFamily.prod_family_name_local,
                MProdFamily.prod_family_factid,
            ),
        ]

    @classmethod
    def join_condition(cls) -> Optional[Join]:
        """m_prod_family -> m_line -> r_factory_machine"""
        return sa.join(left=MProdFamily, right=MLine, onclause=MProdFamily.id == MLine.prod_family_id)


class RelationMLine(RelationMaster):
    r_table = RFactoryMachine
    r_column_name = 'line_id'

    @classmethod  # noqa
    def columns(cls) -> list[Any]:
        return [
            MLine.id,
            func.concat(MLine.line_sign, MLine.line_no),
        ]

    @classmethod
    def join_condition(cls) -> Optional[Join]:
        """m_line -> m_line_group"""
        return sa.join(left=MLine, right=MLineGroup, onclause=MLine.line_group_id == MLineGroup.id)


class RelationMDept(RelationMaster):
    r_table = RFactoryMachine
    r_column_name = 'sect_id'

    @classmethod  # noqa
    def columns(cls) -> list[Any]:
        return [
            MSect.id,
            func.coalesce(
                MDept.dept_abbr_jp,
                MDept.dept_abbr_en,
                MDept.dept_abbr_local,
                MDept.dept_name_jp,
                MDept.dept_name_en,
                MDept.dept_name_local,
                MDept.dept_factid,
            ),
        ]

    @classmethod
    def join_condition(cls) -> Optional[Join]:
        """m_part -> m_sect -> r_factory_machine"""
        return sa.join(left=MDept, right=MSect, onclause=MDept.id == MSect.dept_id)


class RelationMSect(RelationMaster):
    r_table = RFactoryMachine
    r_column_name = 'sect_id'

    @classmethod  # noqa
    def columns(cls) -> list[Any]:
        return [
            MSect.id,
            func.coalesce(
                MSect.sect_abbr_jp,
                MSect.sect_abbr_en,
                MSect.sect_abbr_local,
                MSect.sect_name_jp,
                MSect.sect_name_en,
                MSect.sect_name_local,
                MSect.sect_factid,
            ),
        ]

    @classmethod
    def join_condition(cls) -> Optional[Join]:
        """m_sect -> r_factory_machine"""
        return None


class RelationMProd(RelationMaster):
    r_table = RFactoryMachine
    r_column_name = 'line_id'

    @classmethod  # noqa
    def columns(cls) -> list[Any]:
        return [
            MLine.id,
            func.coalesce(
                MProd.prod_abbr_jp,
                MProd.prod_abbr_en,
                MProd.prod_abbr_local,
                MProd.prod_name_jp,
                MProd.prod_name_en,
                MProd.prod_name_local,
                MProd.prod_factid,
            ),
        ]

    @classmethod
    def join_condition(cls) -> Optional[Join]:
        """m_prod -> m_prod_family -> m_line -> r_factory_machine"""
        return sa.join(left=MProd, right=MProdFamily, onclause=MProd.prod_family_id == MProdFamily.id).join(
            right=MLine,
            onclause=MProdFamily.id == MLine.prod_family_id,
        )


class RelationMPartType(RelationMaster):
    r_table = RProdPart
    r_column_name = 'part_id'

    @classmethod  # noqa
    def columns(cls) -> list[Any]:
        return [
            MPart.id,
            func.coalesce(
                MPartType.part_abbr_jp,
                MPartType.part_abbr_en,
                MPartType.part_abbr_local,
                MPartType.part_name_jp,
                MPartType.part_name_en,
                MPartType.part_name_local,
                MPartType.part_type_factid,
            ),
        ]

    @classmethod
    def join_condition(cls) -> Optional[Join]:
        """m_part_type -> m_part -> r_prod_part"""
        return sa.join(left=MPartType, right=MPart, onclause=MPartType.id == MPart.part_type_id)


class RelationMPart(RelationMaster):
    r_table = RProdPart
    r_column_name = 'part_id'

    @classmethod
    def columns(cls) -> list[Any]:
        return [
            MPart.id,
            func.coalesce(
                MPart.part_no,
                MPart.part_factid,
            ),
        ]

    @classmethod
    def join_condition(cls) -> Optional[Join]:
        """m_part -> r_prod_part"""
        return None


class RelationMEquip(RelationMaster):
    r_table = RFactoryMachine
    r_column_name = 'equip_id'

    @classmethod  # noqa
    def columns(cls) -> list[Any]:
        return [
            MEquip.id,
            func.concat(MEquip.equip_sign, MEquip.equip_no),
        ]

    @classmethod
    def join_condition(cls) -> Optional[Join]:
        """m_equip -> m_equip_group"""
        return sa.join(left=MEquip, right=MEquipGroup, onclause=MEquip.equip_group_id == MEquipGroup.id)


class RelationMSt(RelationMaster):
    r_table = RFactoryMachine
    r_column_name = 'st_id'

    @classmethod  # noqa
    def columns(cls) -> list[Any]:
        return [
            MSt.id,
            func.concat(MSt.st_sign, MSt.st_no),
        ]

    @classmethod
    def join_condition(cls) -> Optional[Join]:
        """m_st -> r_factory_machine"""
        return None


class RelationMProcess(RelationMaster):
    r_table = RFactoryMachine
    r_column_name = 'process_id'

    @classmethod  # noqa
    def columns(cls) -> list[Any]:
        return [
            MProcess.id,
            func.coalesce(
                MProcess.process_abbr_jp,
                MProcess.process_abbr_en,
                MProcess.process_abbr_local,
                MProcess.process_name_jp,
                MProcess.process_name_en,
                MProcess.process_name_local,
                MProcess.process_factid,
            ),
        ]

    @classmethod
    def join_condition(cls) -> Optional[Join]:
        """No need to join"""
        return None
