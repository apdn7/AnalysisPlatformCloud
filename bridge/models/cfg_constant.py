from ap.common.constants import CfgConstantType, DataType, DiskUsageStatus
from ap.common.pydn.dblib.db_common import gen_update_sql
from bridge.models.bridge_station import ConfigModel
from bridge.models.model_utils import TableColumn


class CfgConstantModel(ConfigModel):
    class Columns(TableColumn):
        id = (1, DataType.INTEGER)
        type = (2, DataType.TEXT)
        name = (3, DataType.TEXT)
        value = (4, DataType.TEXT)
        created_at = (5, DataType.DATETIME)
        updated_at = (6, DataType.DATETIME)

    _table_name = 'cfg_constant'
    primary_keys = [Columns.id]

    @classmethod
    def get_value_by_type_name(cls, db_instance, const_type: CfgConstantType, const_name, parse_val=None):
        cond = {cls.Columns.type.name: str(const_type)}
        if const_name:
            cond[cls.Columns.name.name] = str(const_name)
        selection = [cls.Columns.value.name]
        _, rows = cls.select_records(
            db_instance,
            dic_conditions=cond,
            select_cols=selection,
            row_is_dict=False,
            limit=1,
        )
        if not rows:
            return None
        return rows[0] if not rows[0] else parse_val(rows[0])

    @classmethod
    def create_or_update_by_type(cls, db_instance, const_type=None, const_value=0, const_name=None):
        cond = {cls.Columns.type.name: str(const_type)}
        if const_name:
            cond[cls.Columns.name.name] = str(const_name)
        _, rows = cls.select_records(db_instance, dic_conditions=cond, limit=1)

        dict_value = cond.copy()
        dict_value[cls.Columns.value.name] = str(const_value)

        is_inserted = not rows
        if not rows:
            cls.insert_record(db_instance, dict_value)
        else:
            dict_value[cls.Columns.id.name] = rows[cls.Columns.id.name]
            sql, params = gen_update_sql(cls, dic_values=dict_value, dic_conditions=cond)
            db_instance.execute_sql(sql, params=params)
        return str(const_value), is_inserted

    @classmethod
    def get_warning_disk_usage(cls, db_instance) -> int:
        return cls.get_value_by_type_name(
            db_instance,
            CfgConstantType.DISK_USAGE_CONFIG,
            DiskUsageStatus.Warning,
            parse_val=int,
        )

    @classmethod
    def get_error_disk_usage(cls, db_instance) -> int:
        return cls.get_value_by_type_name(
            db_instance,
            CfgConstantType.DISK_USAGE_CONFIG,
            DiskUsageStatus.Full,
            parse_val=int,
        )
