from __future__ import annotations

from typing import TYPE_CHECKING, Optional

from ap.setting_module.models import MLine, MLineGroup, MPlant, MProdFamily
from bridge.services.hover.base import BaseHoverModel, QueryResultType
from bridge.services.hover.schema import MLineHoverSchema

if TYPE_CHECKING:
    from sqlalchemy.orm import Session


class LineHoverModel(BaseHoverModel[MLine, MLineHoverSchema]):
    master_model = MLine
    schema = MLineHoverSchema

    @classmethod
    def get(cls, *, db_session: Session, id: int) -> Optional[QueryResultType]:  # noqa: A002
        return (
            db_session.query(MLine, MLineGroup, MPlant, MProdFamily)
            .outerjoin(MLineGroup, MLineGroup.id == MLine.line_group_id)
            .outerjoin(MPlant, MPlant.id == MLine.plant_id)
            .outerjoin(MProdFamily, MProdFamily.id == MLine.prod_family_id)
            .filter(MLine.id == id)
            .one_or_none()
        )

    @classmethod
    def get_by_ids(cls, *, db_session: Session, ids: int) -> list[QueryResultType]:
        return (
            db_session.query(MLine, MLineGroup, MPlant, MProdFamily)
            .outerjoin(MLineGroup, MLineGroup.id == MLine.line_group_id)
            .outerjoin(MPlant, MPlant.id == MLine.plant_id)
            .outerjoin(MProdFamily, MProdFamily.id == MLine.prod_family_id)
            .filter(MLine.id.in_(ids))
            .all()
        )
