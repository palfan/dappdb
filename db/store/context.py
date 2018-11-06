# -*- coding: utf-8 -*-

import dsnparse


class SQLContext(object):

    def __init__(self, **config):
        assert config.get('APP')
        assert config.get('DB_HOST')
        assert config.get('DB_PORT')
        assert config.get('DB_USER')
        assert config.get('DB_NAME')
        self.config = config


def get_by_env_prefix(name):
    mod = __import__('envcfg.json.%s' % name,
                     fromlist=[name])
    config = vars(mod)
    return config


def init_context(app_config_prefix_name, dsn=None, read_only=False):
    READ_SOURCE = ''
    DEVELOP_MODE = True
    name = app_config_prefix_name

    config = get_by_env_prefix(name)
    DEVELOP_MODE = config.get('DEBUG')
    MYSQL_DSN = dsn or config.get('MYSQL_DSN')

    dsn = dsnparse.parse(MYSQL_DSN)
    host = dsn.host
    port = dsn.port or 3306
    username = dsn.username or 'root'
    password = dsn.password or ''
    database = dsn.paths[0]

    assert host
    assert database

    DB_READ_ONLY = config.get('MQSQL_READ_ONLY') or read_only
    READ_SOURCE = 'envcfg'

    return SQLContext(APP=app_config_prefix_name,
                      DEVELOP_MODE=DEVELOP_MODE,
                      DB_HOST=host,
                      DB_PORT=port,
                      DB_USER=username,
                      DB_PASSWD=password,
                      DB_NAME=database,
                      READ_SOURCE=READ_SOURCE,
                      DB_READ_ONLY=DB_READ_ONLY)
