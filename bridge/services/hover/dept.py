from __future__ import annotations

from ap.setting_module.models import MDept
from bridge.services.hover.base import BaseHoverModel
from bridge.services.hover.schema import MDeptHoverSchema


class DeptHoverModel(BaseHoverModel[MDept, MDeptHoverSchema]):
    master_model = MDept
    schema = MDeptHoverSchema
