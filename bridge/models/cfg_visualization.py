from ap.common.constants import DataType
from bridge.models.bridge_station import ConfigModel
from bridge.models.model_utils import TableColumn


class CfgVisualization(ConfigModel):
    class Columns(TableColumn):
        id = (1, DataType.INTEGER)
        process_id = (2, DataType.INTEGER)

        control_column_id = (3, DataType.INTEGER)
        filter_column_id = (4, DataType.INTEGER)

        filter_value = (5, DataType.TEXT)
        is_from_data = (6, DataType.BOOLEAN)
        filter_detail_id = (7, DataType.INTEGER)

        ucl = (8, DataType.REAL)
        lcl = (9, DataType.REAL)
        upcl = (10, DataType.REAL)
        lpcl = (11, DataType.REAL)
        ymax = (12, DataType.REAL)
        ymin = (13, DataType.REAL)

        act_from = (14, DataType.TEXT)
        act_to = (15, DataType.TEXT)

        order = (16, DataType.INTEGER)

        created_at = (17, DataType.DATETIME)
        updated_at = (18, DataType.DATETIME)

    _table_name = 'cfg_visualization'
    primary_keys = [Columns.id]

    @classmethod
    def get_sensor_default_chart_info(cls, col_id, start_tm, end_tm):
        pass
        # return cls.query.filter(cls.control_column_id == col_id) \
        #     .filter(and_(cls.filter_detail_id.is_(None), cls.filter_column_id.is_(None))) \
        #     .filter(or_(cls.act_from.is_(None), cls.act_from < end_tm, cls.act_from == '')) \
        #     .filter(or_(cls.act_to.is_(None), cls.act_to > start_tm, cls.act_to == '')) \
        #     .order_by(cls.act_from.desc()).all()

    @classmethod
    def get_chart_info_by_fitler_col_id(cls, col_id, filter_col_id, start_tm, end_tm):
        pass
        # return cls.query.filter(
        #     and_(cls.control_column_id == col_id,
        #          cls.filter_column_id == filter_col_id,
        #          cls.filter_value.is_(None),
        #          cls.filter_detail_id.is_(None),
        #          )) \
        #     .filter(or_(cls.act_from.is_(None), cls.act_from < end_tm, cls.act_from == '')) \
        #     .filter(or_(cls.act_to.is_(None), cls.act_to > start_tm, cls.act_to == '')) \
        #     .order_by(cls.act_from.desc()).all()

    @classmethod
    def get_chart_info_by_fitler_detail_id(cls, col_id, filter_detail_id, start_tm, end_tm):
        pass
        # return cls.query.filter(and_(cls.control_column_id == col_id, cls.filter_detail_id == filter_detail_id)) \
        #     .filter(or_(cls.act_from.is_(None), cls.act_from < end_tm, cls.act_from == '')) \
        #     .filter(or_(cls.act_to.is_(None), cls.act_to > start_tm, cls.act_to == '')) \
        #     .order_by(cls.act_from.desc()).all()
