from __future__ import annotations

from typing import TYPE_CHECKING, Optional

from ap.setting_module.models import MPart, MPartType
from bridge.services.hover.base import BaseHoverModel, QueryResultType
from bridge.services.hover.schema import MPartHoverSchema

if TYPE_CHECKING:
    from sqlalchemy.orm import Session


class PartHoverModel(BaseHoverModel[MPart, MPartHoverSchema]):
    master_model = MPart
    schema = MPartHoverSchema

    @classmethod
    def get(cls, *, db_session: Session, id: int) -> Optional[QueryResultType]:  # noqa: A002
        return (
            db_session.query(MPart, MPartType)
            .outerjoin(MPartType, MPartType.id == MPart.part_type_id)
            .filter(MPart.id == id)
            .one_or_none()
        )

    @classmethod
    def get_by_ids(cls, *, db_session: Session, ids: int) -> list[QueryResultType]:
        return (
            db_session.query(MPart, MPartType)
            .outerjoin(MPartType, MPartType.id == MPart.part_type_id)
            .filter(MPart.id.in_(ids))
            .all()
        )
