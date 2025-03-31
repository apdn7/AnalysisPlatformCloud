import os
from collections import namedtuple

from apscheduler.executors.pool import ProcessPoolExecutor, ThreadPoolExecutor
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from sqlalchemy.pool import NullPool

from ap import get_basic_yaml_obj, get_start_up_yaml_obj
from ap.common.common_utils import resource_path
from ap.common.constants import (
    DATABASE_HOST_ENV,
    DATABASE_NAME_ENV,
    DATABASE_PASSWORD_ENV,
    DATABASE_PORT_ENV,
    DATABASE_USERNAME_ENV,
    DEFAULT_POSTGRES_SCHEMA,
    SCHEDULER_PROCESS_POOL_SIZE,
)
from ap.common.logger import logger

basedir = os.getcwd()

DbMode = namedtuple('DbMode', ['dbname', 'host', 'port', 'username', 'password'])


def get_db_mode(file_name=None) -> DbMode:
    start_up_yaml = get_start_up_yaml_obj()
    basic_config_yaml = get_basic_yaml_obj(file_name)

    dbname = os.environ.get(DATABASE_NAME_ENV) or start_up_yaml.get_db_name() or basic_config_yaml.get_db_name()
    host = os.environ.get(DATABASE_HOST_ENV) or start_up_yaml.get_db_host() or basic_config_yaml.get_db_host()
    port = os.environ.get(DATABASE_PORT_ENV) or start_up_yaml.get_db_port() or basic_config_yaml.get_db_port()
    username = (
        os.environ.get(DATABASE_USERNAME_ENV) or start_up_yaml.get_db_username() or basic_config_yaml.get_db_username()
    )
    password = (
        os.environ.get(DATABASE_PASSWORD_ENV) or start_up_yaml.get_db_password() or basic_config_yaml.get_db_password()
    )

    return DbMode(dbname, host, port, username, password)


def get_current_mode_db_url(file_name=None):
    """
    Bridge Station database
    :return:
    """
    db_mode = get_db_mode(file_name)
    db_url = f'postgresql+psycopg2://{db_mode.username}:{db_mode.password}@{db_mode.host}:{db_mode.port}/{db_mode.dbname}?options=-c%20search_path={DEFAULT_POSTGRES_SCHEMA}'
    return db_url


class Config(object):
    SECRET_KEY = '736670cb10a600b695a55839ca3a5aa54a7d7356cdef815d2ad6e19a2031182b'
    POSTS_PER_PAGE = 10
    PORT = 80
    os.environ['FLASK_ENV'] = os.environ.get('FLASK_ENV', 'production')
    CICD_BASE_DIR = os.environ.get('CICD_BASE_DIR')
    parent_dir = CICD_BASE_DIR if CICD_BASE_DIR else os.path.dirname(basedir)

    R_PORTABLE = os.path.join(parent_dir, 'R-Portable', 'bin')
    os.environ['PATH'] = '{};{}'.format(R_PORTABLE, os.environ.get('PATH', ''))

    # R-PORTABLEを設定する。
    os.environ['R-PORTABLE'] = os.path.join(parent_dir, 'R-Portable')

    ORACLE_PATH = os.path.join(parent_dir, 'Oracle-Portable')
    os.environ['PATH'] = '{};{}'.format(ORACLE_PATH, os.environ.get('PATH', ''))

    ORACLE_PATH_WITH_VERSION = os.path.join(ORACLE_PATH, 'instantclient_21_3')
    os.environ['PATH'] = '{};{}'.format(ORACLE_PATH_WITH_VERSION, os.environ.get('PATH', ''))

    logger.info(os.environ['PATH'])
    print(R_PORTABLE)

    BABEL_DEFAULT_LOCALE = 'en'

    # run `python ap/script/generate_db_secret_key.py` to generate DB_SECRET_KEY
    DB_SECRET_KEY = '4hlAxWLWt8Tyqi5i1zansLPEXvckXR2zrl_pDkxVa-A='

    # CREATE_ENGINE_PARAMS = {'timeout': 180, 'isolation_level': 'IMMEDIATE'}
    CREATE_ENGINE_PARAMS = {'timeout': 60 * 5}
    # timeout
    # SQLALCHEMY_ENGINE_OPTIONS = {'connect_args': CREATE_ENGINE_PARAMS}
    # SQLALCHEMY_POOL_SIZE = 20
    # SQLALCHEMY_MAX_OVERFLOW = 0
    # SQLALCHEMY_ENGINE_OPTIONS = {'connect_args': {'timeout': 30}}
    # set to NullPool until we know how to handle QueuePool
    # SQLALCHEMY_POOL_SIZE = 20
    # SQLALCHEMY_MAX_OVERFLOW = -1

    # db_url = get_current_mode_db_url()
    # SQLALCHEMY_DATABASE_URI = db_url
    # SCHEDULER_DATABASE_URI = db_url

    # APScheduler
    SCHEDULER_EXECUTORS = {
        'default': ProcessPoolExecutor(SCHEDULER_PROCESS_POOL_SIZE),
        'threadpool': ThreadPoolExecutor(20),
    }

    SCHEDULER_JOB_DEFAULTS = {
        'coalesce': True,
        'max_instances': 1,
        # 'misfire_grace_time': 60 * 60
        'misfire_grace_time': None,
    }
    VERSION_FILE_PATH = resource_path('VERSION')
    BASE_DIR = basedir
    PARTITION_NUMBER = 1
    PARTITION_YEAR_AGO = 3

    COMPRESS_MIMETYPES = [
        'text/html',
        'text/css',
        'text/xml',
        'text/csv',
        'text/tsv',
        'application/json',
        'application/javascript',
    ]
    COMPRESS_LEVEL = 6
    COMPRESS_MIN_SIZE = 500

    INIT_CONFIG_DIR = os.path.join(basedir, 'init')
    INIT_LOG_DIR = os.path.join(basedir, 'log')
    INIT_BASIC_CFG_FILE = os.path.join(INIT_CONFIG_DIR, 'basic_config.yml')

    IS_SEND_GOOGLE_ANALYTICS = True


class ProdConfig(Config):
    DEBUG = False
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    db_url = get_current_mode_db_url()
    SQLALCHEMY_DATABASE_URI = db_url
    SCHEDULER_DATABASE_URI = db_url
    # set to NullPool until we know how to handle QueuePool
    SCHEDULER_JOBSTORES = {
        'default': SQLAlchemyJobStore(url=SCHEDULER_DATABASE_URI, engine_options={'poolclass': NullPool}),
    }
    # SQLITE_CONFIG_DIR = os.path.join(basedir, 'instance')
    # SCHEDULER_DB_FILE = os.path.join(SQLITE_CONFIG_DIR, 'scheduler_db.sqlite3')
    # SCHEDULER_DATABASE_URI = 'sqlite:///' + SCHEDULER_DB_FILE
    YAML_CONFIG_DIR = os.path.join(basedir, 'ap', 'config')


class DevConfig(Config):
    DEBUG = True
    SQLALCHEMY_TRACK_MODIFICATIONS = True
    db_url = get_current_mode_db_url()
    SQLALCHEMY_DATABASE_URI = db_url
    SCHEDULER_DATABASE_URI = db_url
    # set to NullPool until we know how to handle QueuePool
    SCHEDULER_JOBSTORES = {
        'default': SQLAlchemyJobStore(url=SCHEDULER_DATABASE_URI, engine_options={'poolclass': NullPool}),
    }
    # SQLITE_CONFIG_DIR = os.path.join(basedir, 'instance')
    # SCHEDULER_DB_FILE = os.path.join(SQLITE_CONFIG_DIR, 'scheduler_db.sqlite3')
    # SCHEDULER_DATABASE_URI = 'sqlite:///' + SCHEDULER_DB_FILE
    YAML_CONFIG_DIR = os.path.join(basedir, 'ap', 'config')
    PARTITION_YEAR_AGO = 1
