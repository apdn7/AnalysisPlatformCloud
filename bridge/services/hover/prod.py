from __future__ import annotations

from typing import TYPE_CHECKING, Optional

from ap.setting_module.models import MProd, MProdFamily
from bridge.services.hover.base import BaseHoverModel, QueryResultType
from bridge.services.hover.schema import MProdHoverSchema

if TYPE_CHECKING:
    from sqlalchemy.orm import Session


class ProdHoverModel(BaseHoverModel[MProd, MProdHoverSchema]):
    master_model = MProd
    schema = MProdHoverSchema

    @classmethod
    def get(cls, *, db_session: Session, id: int) -> Optional[QueryResultType]:  # noqa: A002
        return (
            db_session.query(MProd, MProdFamily)
            .outerjoin(MProdFamily, MProdFamily.id == MProd.prod_family_id)
            .filter(MProd.id == id)
            .one_or_none()
        )

    @classmethod
    def get_by_ids(cls, *, db_session: Session, ids: int) -> list[QueryResultType]:
        return (
            db_session.query(MProd, MProdFamily)
            .outerjoin(MProdFamily, MProdFamily.id == MProd.prod_family_id)
            .filter(MProd.id.in_(ids))
            .all()
        )
