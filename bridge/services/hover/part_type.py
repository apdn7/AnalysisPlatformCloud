from __future__ import annotations

from ap.setting_module.models import MPartType
from bridge.services.hover.base import BaseHoverModel
from bridge.services.hover.schema import MPartTypeHoverSchema


class PartTypeHoverModel(BaseHoverModel[MPartType, MPartTypeHoverSchema]):
    master_model = MPartType
    schema = MPartTypeHoverSchema
