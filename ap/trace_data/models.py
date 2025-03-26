from sqlalchemy import event
from sqlalchemy.sql import func

from ap import db
from ap.common.common_utils import chunks, get_current_timestamp
from ap.common.constants import SQL_IN_MAX
from ap.common.services.normalization import model_normalize
from ap.setting_module.models import CommonDBModel, db_timestamp


class TransactionDBModel(CommonDBModel):
    __abstract__ = True

    @classmethod
    def gen_valid_dict(cls, dic_data, cols=None):
        if not cols:
            cols = cls.get_column_names()

        if not isinstance(dic_data, dict):
            dic_output = {col: getattr(dic_data, col) for col in cols if hasattr(dic_data, col)}
        else:
            dic_output = {key: val for key, val in dic_data.items() if key in cols}

        return dic_output

    @classmethod
    def insert_records(cls, columns, rows, session):
        """
        insert records
        :return:
        :param columns:
        :param rows: [{id:1,name:abc},{id:2,name:def]
        :param session:
        :return:
        """
        if not rows:
            return False

        # insert to db
        cols = cls.get_column_names()
        for dic_data in rows:
            valid_dict_data = cls.gen_valid_dict(dic_data, cols)
            session.merge(cls(**valid_dict_data))

        return True

    @classmethod
    def get_column_names(cls):
        """
        use for physical table only
        :return:
        """
        # real_cls = cls.get_first_child_class()
        cols = [col.name for col in list(cls.__table__.columns)]

        return cols


class DataTypeModel(TransactionDBModel):
    __abstract__ = True

    @classmethod
    def delete_by_cycle_ids(cls, proc_id, cycle_ids, session):
        for chunk_ids in chunks(cycle_ids, SQL_IN_MAX):
            session.query(cls).filter(cls.process_id == proc_id, cls.cycle_id.in_(chunk_ids)).delete(
                synchronize_session='fetch',
            )

    @classmethod
    def insert_records(cls, cols, rows, session):
        """
        insert records . Important : must specify DataType class before use
        :param cols:
        :param rows:
        :param session:
        :return:
        """
        # split by proc_id

        if not cols or not rows:
            return False

        session.execute(cls.__table__.insert(), [dict(zip(cols, row)) for row in rows])

        return True


class ProcDataCount(TransactionDBModel):
    __tablename__ = 't_proc_data_count'
    __table_args__ = {'sqlite_autoincrement': True}
    id = db.Column(db.Integer(), primary_key=True)
    datetime = db.Column(db_timestamp())
    process_id = db.Column(db.Integer(), index=True)
    job_id = db.Column(db.Integer())
    count = db.Column(db.Integer())
    count_file = db.Column(db.Integer(), default=0)
    created_at = db.Column(db_timestamp(), default=get_current_timestamp)

    @classmethod
    def get_procs_count(cls):
        output = cls.query.with_entities(cls.process_id, func.sum(cls.count).label(cls.count.key))
        output = output.group_by(cls.process_id).all()
        return output

    @classmethod
    def get_by_proc_id(cls, proc_id, start_date, end_date, count_in_file: bool):
        if count_in_file:
            count_col = func.sum(cls.count_file).label(cls.count.key)
        else:
            count_col = func.sum(cls.count).label(cls.count.key)

        result = cls.query.with_entities(
            cls.datetime,
            count_col,
        ).filter(cls.process_id == proc_id)
        if start_date != end_date:
            result = result.filter(cls.datetime >= start_date, cls.datetime < end_date)
        result = result.group_by(cls.datetime)

        result = result.all()
        return result

    @classmethod
    def delete_data_count_by_process_id(cls, proc_id):
        delete_query = cls.__table__.delete().where(cls.process_id == proc_id)
        db.session.execute(delete_query)
        db.session.commit()


# ###################### TABLE PARTITION ###########################

# PARTITION_TABLE = dic_config[PARTITION_NUMBER]
# CYCLE_CLASSES = []
# SENSOR_INT_CLASSES = []
# SENSOR_REAL_CLASSES = []
# SENSOR_TEXT_CLASSES = []
# PROC_LINK_CLASSES = []
#
#
# def find_cycle_class(process_id):
#     """
#     get partition class of cycle
#     :param process_id:
#     :return:
#     """
#     idx = int(process_id) % PARTITION_TABLE
#     return CYCLE_CLASSES[idx]


def make_f(model):
    @event.listens_for(model, 'before_insert')
    def before_insert(_mapper, _connection, target):
        model_normalize(target)

    @event.listens_for(model, 'before_update')
    def before_update(_mapper, _connection, target):
        model_normalize(target)


def add_listen_event():
    models = TransactionDBModel.__subclasses__()
    for model in models:
        make_f(model)


# GUI normalization
add_listen_event()
