from __future__ import annotations

from typing import TYPE_CHECKING, Optional

from ap.setting_module.models import MEquip, MEquipGroup
from bridge.services.hover.base import BaseHoverModel, QueryResultType
from bridge.services.hover.schema import MEquipHoverSchema

if TYPE_CHECKING:
    from sqlalchemy.orm import Session


class EquipHoverModel(BaseHoverModel[MEquip, MEquipHoverSchema]):
    master_model = MEquip
    schema = MEquipHoverSchema

    @classmethod
    def get(cls, *, db_session: Session, id: int) -> Optional[QueryResultType]:  # noqa: A002
        return (
            db_session.query(MEquip, MEquipGroup)
            .outerjoin(MEquipGroup, MEquipGroup.id == MEquip.equip_group_id)
            .filter(MEquip.id == id)
            .one_or_none()
        )

    @classmethod
    def get_by_ids(cls, *, db_session: Session, ids: int) -> list[QueryResultType]:
        return (
            db_session.query(MEquip, MEquipGroup)
            .outerjoin(MEquipGroup, MEquipGroup.id == MEquip.equip_group_id)
            .filter(MEquip.id.in_(ids))
            .all()
        )
