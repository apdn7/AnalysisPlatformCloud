from __future__ import annotations

from typing import TYPE_CHECKING, Optional

from ap.setting_module.models import MFactory, MLocation
from bridge.services.hover.base import BaseHoverModel, QueryResultType
from bridge.services.hover.schema import MFactoryHoverSchema

if TYPE_CHECKING:
    from sqlalchemy.orm import Session


class FactoryHoverModel(BaseHoverModel[MFactory, MFactoryHoverSchema]):
    master_model = MFactory
    schema = MFactoryHoverSchema

    @classmethod
    def get(cls, *, db_session: Session, id: int) -> Optional[QueryResultType]:  # noqa: A002
        return (
            db_session.query(MFactory, MLocation)
            .outerjoin(MLocation, MLocation.id == MFactory.location_id)
            .filter(MFactory.id == id)
            .one_or_none()
        )

    @classmethod
    def get_by_ids(cls, *, db_session: Session, ids: int) -> list[QueryResultType]:
        return (
            db_session.query(MFactory, MLocation)
            .outerjoin(MLocation, MLocation.id == MFactory.location_id)
            .filter(MFactory.id.in_(ids))
            .all()
        )
