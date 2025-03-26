from ap.common.constants import DataType
from bridge.models.bridge_station import ConfigModel
from bridge.models.model_utils import TableColumn


class CfgUserSetting(ConfigModel):
    class Columns(TableColumn):
        id = (1, DataType.INTEGER)
        key = (2, DataType.TEXT)

        title = (3, DataType.TEXT)
        page = (4, DataType.TEXT)

        created_by = (5, DataType.TEXT)
        priority = (6, DataType.INTEGER)
        use_current_time = (7, DataType.BOOLEAN)

        description = (8, DataType.TEXT)
        share_info = (9, DataType.BOOLEAN)
        settings = (10, DataType.TEXT)

        created_at = (97, DataType.DATETIME)
        updated_at = (98, DataType.DATETIME)

    def __init__(self, dict_data=None):
        if not dict_data:
            pass
        self.id = dict_data.get(CfgUserSetting.Columns.id.name)
        if self.id is None:
            del self.id

        self.key = dict_data.get(CfgUserSetting.Columns.key.name)

        self.title = dict_data.get(CfgUserSetting.Columns.title.name)
        self.page = dict_data.get(CfgUserSetting.Columns.page.name)

        self.created_by = dict_data.get(CfgUserSetting.Columns.created_by.name)
        self.priority = dict_data.get(CfgUserSetting.Columns.priority.name)
        self.use_current_time = dict_data.get(CfgUserSetting.Columns.use_current_time.name)
        self.description = dict_data.get(CfgUserSetting.Columns.description.name)
        self.share_info = dict_data.get(CfgUserSetting.Columns.share_info.name)
        self.settings = dict_data.get(CfgUserSetting.Columns.settings.name)

        self.created_at = dict_data.get(CfgUserSetting.Columns.created_at.name)
        self.updated_at = dict_data.get(CfgUserSetting.Columns.updated_at.name)

    _table_name = 'cfg_user_setting'
    primary_keys = [Columns.id]
