from __future__ import annotations

from typing import TYPE_CHECKING, Optional

from ap.setting_module.models import MFactory, MPlant
from bridge.services.hover.base import BaseHoverModel, QueryResultType
from bridge.services.hover.schema import MPlantHoverSchema

if TYPE_CHECKING:
    from sqlalchemy.orm import Session


class PlantHoverModel(BaseHoverModel):
    master_model = MPlant
    schema = MPlantHoverSchema

    @classmethod
    def get(cls, *, db_session: Session, id: int) -> Optional[QueryResultType]:  # noqa: A002
        return (
            db_session.query(MPlant, MFactory)
            .outerjoin(MFactory, MFactory.id == MPlant.factory_id)
            .filter(MPlant.id == id)
            .one_or_none()
        )

    @classmethod
    def get_by_ids(cls, *, db_session: Session, ids: int) -> list[QueryResultType]:
        return (
            db_session.query(MPlant, MFactory)
            .outerjoin(MFactory, MFactory.id == MPlant.factory_id)
            .filter(MPlant.id.in_(ids))
            .all()
        )
