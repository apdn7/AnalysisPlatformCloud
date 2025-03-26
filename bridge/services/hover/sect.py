from __future__ import annotations

from typing import TYPE_CHECKING, Optional

from ap.setting_module.models import MDept, MSect
from bridge.services.hover.base import BaseHoverModel, QueryResultType
from bridge.services.hover.schema import MSectHoverSchema

if TYPE_CHECKING:
    from sqlalchemy.orm import Session


class SectHoverModel(BaseHoverModel[MSect, MSectHoverSchema]):
    master_model = MSect
    schema = MSectHoverSchema

    @classmethod
    def get(cls, *, db_session: Session, id: int) -> Optional[QueryResultType]:  # noqa: A002
        return (
            db_session.query(MSect, MDept)
            .outerjoin(MDept, MDept.id == MSect.dept_id)
            .filter(MSect.id == id)
            .one_or_none()
        )

    @classmethod
    def get_by_ids(cls, *, db_session: Session, ids: int) -> list[QueryResultType]:
        return (
            db_session.query(MSect, MDept).outerjoin(MDept, MDept.id == MSect.dept_id).filter(MSect.id.in_(ids)).all()
        )
