from __future__ import annotations

from ap.setting_module.models import MProdFamily
from bridge.services.hover.base import BaseHoverModel
from bridge.services.hover.schema import MProdFamilyHoverSchema


class ProdFamilyHoverModel(BaseHoverModel):
    master_model = MProdFamily
    schema = MProdFamilyHoverSchema
