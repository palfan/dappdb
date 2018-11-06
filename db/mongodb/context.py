# -*- coding: utf-8 -*-


class MongoDBContext(object):

    def __init__(self, **config):
        assert config.get('APP')
        assert config.get('MONGO_SERVER')
        self.config = config


def get_by_env_prefix(name):
    mod = __import__('envcfg.json.%s' % name,
                     fromlist=[name])
    config = vars(mod)
    return config


def init_context(app_config_prefix_name, server=None, db=None):
    DEVELOP_MODE = True
    name = app_config_prefix_name

    config = get_by_env_prefix(name)
    MONGO_SERVER = server or config.get('MONGO_SERVER')
    MONGO_DB = db or config.get('MONGO_DB')
    if not MONGO_SERVER:
        raise Exception('MONGO_SERVER should not be empty.')
    READ_SOURCE = 'envcfg'

    return MongoDBContext(APP=app_config_prefix_name,
                          DEVELOP_MODE=DEVELOP_MODE,
                          MONGO_SERVER=MONGO_SERVER,
                          MONGO_DB=MONGO_DB,
                          READ_SOURCE=READ_SOURCE)
