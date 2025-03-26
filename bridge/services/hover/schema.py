from __future__ import annotations

from datetime import datetime  # noqa: TCH003
from typing import Optional

from pydantic import BaseModel, ConfigDict, Field


class HoverSchema(BaseModel):
    model_config = ConfigDict(from_attributes=True)


class MFactoryHoverSchema(HoverSchema):
    id: int = Field()  # noqa: A003
    factory_factid: Optional[str]
    factory_name_jp: Optional[str]
    factory_name_en: Optional[str]
    factory_name_sys: Optional[str] = Field(exclude=True)
    factory_name_local: Optional[str]
    factory_abbr_jp: Optional[str]
    factory_abbr_en: Optional[str]
    factory_abbr_local: Optional[str]
    location_id: Optional[int] = Field()
    # location info
    location_name_jp: Optional[str]
    location_name_en: Optional[str]
    location_name_sys: Optional[str] = Field(exclude=True)
    location_abbr: Optional[str]


class MPlantHoverSchema(HoverSchema):
    id: int = Field()  # noqa: A003
    factory_id: Optional[int] = Field()
    plant_factid: Optional[str]
    plant_name_jp: Optional[str]
    plant_name_en: Optional[str]
    plant_name_sys: Optional[str] = Field(exclude=True)
    plant_name_local: Optional[str]
    plant_abbr_jp: Optional[str]
    plant_abbr_en: Optional[str]
    plant_abbr_local: Optional[str]
    # factory info
    factory_name_jp: Optional[str]
    factory_name_en: Optional[str]
    factory_name_sys: Optional[str] = Field(exclude=True)
    factory_name_local: Optional[str]


class MProdFamilyHoverSchema(HoverSchema):
    id: int = Field()  # noqa: A003
    prod_family_factid: Optional[str]
    prod_family_name_jp: Optional[str]
    prod_family_name_en: Optional[str]
    prod_family_name_sys: Optional[str] = Field(exclude=True)
    prod_family_name_local: Optional[str]
    prod_family_abbr_jp: Optional[str]
    prod_family_abbr_en: Optional[str]
    prod_family_abbr_local: Optional[str]


class MLineHoverSchema(HoverSchema):
    id: int = Field()  # noqa: A003
    plant_id: Optional[int] = Field()
    prod_family_id: Optional[int] = Field()
    line_group_id: Optional[int] = Field()
    line_factid: Optional[str]
    line_no: Optional[int]
    line_sign: Optional[str] = Field(exclude=True)
    outsourcing_flag: Optional[bool] = Field(exclude=True)
    outsource: Optional[str]
    supplier: Optional[str] = Field(exclude=True)
    line_name_jp: Optional[str]
    line_name_en: Optional[str]
    line_name_sys: Optional[str] = Field(exclude=True)
    line_name_local: Optional[str]
    # plant info
    plant_name_jp: Optional[str]
    plant_name_en: Optional[str]
    plant_name_sys: Optional[str] = Field(exclude=True)
    plant_name_local: Optional[str]
    # prod_family info
    prod_family_name_jp: Optional[str]
    prod_family_name_en: Optional[str]
    prod_family_name_sys: Optional[str] = Field(exclude=True)
    prod_family_name_local: Optional[str]


class MDeptHoverSchema(HoverSchema):
    id: int = Field()  # noqa: A003
    dept_factid: Optional[str]
    dept_name_jp: Optional[str]
    dept_name_en: Optional[str]
    dept_name_sys: Optional[str] = Field(exclude=True)
    dept_name_local: Optional[str]
    dept_abbr_jp: Optional[str]
    dept_abbr_en: Optional[str]
    dept_abbr_local: Optional[str]


class MSectHoverSchema(HoverSchema):
    id: int = Field()  # noqa: A003
    dept_id: Optional[int] = Field()
    sect_factid: Optional[str]
    sect_name_jp: Optional[str]
    sect_name_en: Optional[str]
    sect_name_sys: Optional[str] = Field(exclude=True)
    sect_name_local: Optional[str]
    sect_abbr_jp: Optional[str]
    sect_abbr_en: Optional[str]
    sect_abbr_local: Optional[str]
    # dept info
    dept_name_jp: Optional[str]
    dept_name_en: Optional[str]
    dept_name_sys: Optional[str] = Field(exclude=True)
    dept_name_local: Optional[str]


class MProdHoverSchema(HoverSchema):
    id: int = Field()  # noqa: A003
    prod_family_id: Optional[int] = Field()
    prod_factid: Optional[str]
    prod_name_jp: Optional[str]
    prod_name_en: Optional[str]
    prod_name_sys: Optional[str] = Field(exclude=True)
    prod_name_local: Optional[str]
    prod_abbr_jp: Optional[str]
    prod_abbr_en: Optional[str]
    prod_abbr_local: Optional[str]
    # prod_family info
    prod_family_name_jp: Optional[str]
    prod_family_name_en: Optional[str]
    prod_family_name_sys: Optional[str] = Field(exclude=True)
    prod_family_name_local: Optional[str]


class MPartTypeHoverSchema(HoverSchema):
    id: int = Field()  # noqa: A003
    part_type_factid: Optional[str]
    part_name_jp: Optional[str]
    part_name_en: Optional[str]
    part_name_local: Optional[str]
    part_abbr_jp: Optional[str]
    part_abbr_en: Optional[str]
    part_abbr_local: Optional[str]
    assy_flag: Optional[bool] = Field(exclude=True)


class MPartHoverSchema(HoverSchema):
    id: int = Field()  # noqa: A003
    part_type_id: Optional[int] = Field()
    part_factid: Optional[str]
    part_no: Optional[str]
    part_use: Optional[bool] = Field(exclude=True)
    location_id: Optional[int] = Field(exclude=True)
    # part type info
    part_name_jp: Optional[str]
    part_name_en: Optional[str]
    part_name_local: Optional[str]


class MEquipHoverSchema(HoverSchema):
    id: int = Field(exclude=True)  # noqa: A003
    equip_group_id: Optional[int] = Field()
    equip_no: Optional[int]
    equip_sign: Optional[str] = Field(exclude=True)
    equip_factid: Optional[str]
    equip_product_no: Optional[str]
    equip_product_date: Optional[datetime]
    equip_name_jp: Optional[str]
    equip_name_en: Optional[str]
    equip_name_sys: Optional[str] = Field(exclude=True)
    equip_name_local: Optional[str]


class MStHoverSchema(HoverSchema):
    id: int = Field(exclude=True)  # noqa: A003
    equip_id: Optional[int] = Field(exclude=True)
    st_no: Optional[int]
    st_sign: Optional[str] = Field(exclude=True)
    # equip info
    equip_no: Optional[int]
    equip_sign: Optional[str] = Field(exclude=True)
    equip_factid: Optional[str]
    equip_product_no: Optional[str]
    equip_product_date: Optional[datetime]


class MProcessHoverSchema(HoverSchema):
    id: int = Field(exclude=True)  # noqa: A003
    prod_family_id: Optional[int] = Field(exclude=True)
    process_factid: Optional[str]
    process_name_jp: Optional[str]
    process_name_en: Optional[str]
    process_name_sys: Optional[str] = Field(exclude=True)
    process_name_local: Optional[str]
    process_abbr_jp: Optional[str]
    process_abbr_en: Optional[str]
    process_abbr_local: Optional[str]
