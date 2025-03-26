from __future__ import annotations

from ap.setting_module.models import MProcess
from bridge.services.hover.base import BaseHoverModel
from bridge.services.hover.schema import MProcessHoverSchema


class ProcessHoverModel(BaseHoverModel[MProcess, MProcessHoverSchema]):
    master_model = MProcess
    schema = MProcessHoverSchema
