from __future__ import annotations

from typing import TYPE_CHECKING, Generic, Optional, TypeVar, Union

from ap.setting_module.models import MasterDBModel
from bridge.services.hover.schema import HoverSchema

if TYPE_CHECKING:
    from sqlalchemy.orm import Session


_MasterModel = TypeVar('_MasterModel', bound=MasterDBModel)
_HoverSchema = TypeVar('_HoverSchema', bound=HoverSchema)
QueryResultType = Union[_MasterModel, tuple[..., MasterDBModel]]


class BaseHoverModel(Generic[_MasterModel, _HoverSchema]):
    """BaseHover is enough for MVP"""

    master_model: type[_MasterModel]
    schema: type[_HoverSchema]

    @classmethod
    def get(cls, *, db_session: Session, id: int) -> Optional[QueryResultType]:  # noqa: A002
        return db_session.query(cls.master_model).filter(cls.master_model.id == id).one_or_none()

    @classmethod
    def get_by_ids(cls, *, db_session: Session, ids: list[int]) -> list[QueryResultType]:
        return (  # type: ignore
            db_session.query(cls.master_model).filter(cls.master_model.id.in_(ids)).order_by(cls.master_model.id).all()
        )

    @classmethod
    def query_result_to_hover(cls, query_result: QueryResultType) -> _HoverSchema:
        if not isinstance(query_result, tuple):
            return cls.schema.model_validate(query_result)

        result_dict = {}
        for res in reversed(query_result):
            result_dict.update(res.as_dict())
        return cls.schema(**result_dict)

    @classmethod
    def get_hover(cls, *, db_session: Session, id: int) -> _HoverSchema | None:  # noqa: A002
        res = cls.get(db_session=db_session, id=id)
        if res is None:
            return None
        return cls.query_result_to_hover(res)

    @classmethod
    def get_hover_by_ids(cls, *, db_session: Session, ids: list[int]) -> list[_HoverSchema]:
        return [cls.query_result_to_hover(res) for res in cls.get_by_ids(db_session=db_session, ids=ids)]
