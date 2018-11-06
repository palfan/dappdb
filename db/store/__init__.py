# -*- coding: utf-8 -*-

"""
    store for database
"""

import random
import re
import sys
import threading

import MySQLdb
from werkzeug.utils import cached_property


def connect_db(CONTEXT_HOST, CONTEXT_PORT,
               CONTEXT_USER, CONTEXT_PAWD,
               CONTEXT_DBNAME):
    conn = MySQLdb.connect(
        host=CONTEXT_HOST,
        port=CONTEXT_PORT,
        user=CONTEXT_USER,
        passwd=CONTEXT_PAWD,
        db=CONTEXT_DBNAME,
        use_unicode=True,
        charset="utf8")
    return conn


SQL_PATTERNS = {
    'select': re.compile(
        r'select\s.*?\sfrom\s+`?(?P<table>\w+)`?', re.I | re.S),
    'insert': re.compile(
        r'insert\s+(ignore\s+)?(into\s+)?`?(?P<table>\w+)`?', re.I),
    'update': re.compile(
        r'update\s+(ignore\s+)?`?(?P<table>\w+)`?\s+set', re.I),
    'replace': re.compile(
        r'replace\s+(into\s+)?`?(?P<table>\w+)`?', re.I),
    'delete': re.compile(
        r'delete\s+from\s+`?(?P<table>\w+)`?', re.I),
}


class SqlStorePool(object):
    def __init__(self, context, total=10):
        stores = []
        for i in range(total):
            _store = SqlStore(context, index=i)
            stores.append(_store)
        self.stores = stores
        self.total = total
        self.modified_stores = []

    def __repr__(self):
        return '<SqlStorePool total=%s>' % self.total

    def _get_store(self):
        _idx = max(random.randint(0, self.total) - 1, 0)
        _idx = min(self.total, _idx)
        return self.stores[_idx]

    def execute(self, sql, args=None):
        store = self._get_store()
        cmd = self.parse_execute_sql(sql)
        if cmd != 'select':
            self.modified_stores.append(store)
        return store.execute(sql, args)

    def commit(self):
        for store in self.modified_stores:
            store.commit()
        self.modified_stores = []

    def rollback(self):
        for store in self.modified_stores:
            store.rollback()
        self.modified_stores = []

    def is_testing(self):
        # DO NOT CHANGE THIS METHOD!
        return False

    def get_cursor(self):
        store = self._get_store()
        return store._conn.cursor()

    def parse_execute_sql(self, sql):
        sql = sql.lstrip()
        cmd = sql.split(' ', 1)[0].lower()
        if self.is_testing():
            SQL_PATTERNS['truncate'] = re.compile(
                r'truncate\s`?(?P<table>\w+)`?')
        if cmd not in SQL_PATTERNS:
            raise Exception('SQL command %s is not yet supported' % cmd)
        match = SQL_PATTERNS[cmd].match(sql)
        if not match:
            raise Exception(sql)


class DistributedSqlStore(object):
    def __init__(self, write_store,
                 read_store):
        self.write_store = write_store
        self.read_store = read_store

    def execute(self, sql, args=None):
        cmd = self.parse_execute_sql(sql)
        if cmd == 'select':
            r = self.read_store.execute(sql, args)
            if not r:
                r = self.write_store.execute(sql, args)
            return r
        else:
            return self.write_store.execute(sql, args)

    def commit(self):
        return self.write_store.commit()

    def rollback(self):
        return self.write_store.rollback()

    def parse_execute_sql(self, sql):
        sql = sql.lstrip()
        cmd = sql.split(' ', 1)[0].lower()
        if self.is_testing():
            SQL_PATTERNS['truncate'] = re.compile(
                r'truncate\s`?(?P<table>\w+)`?')
        if cmd not in SQL_PATTERNS:
            raise Exception('SQL command %s is not yet supported' % cmd)
        match = SQL_PATTERNS[cmd].match(sql)
        if not match:
            raise Exception(sql)

    def is_testing(self):
        # DO NOT CHANGE THIS METHOD!
        return False

    def get_cursor(self):
        return self.write_store._conn.cursor()


class SqlStore(object):

    def __init__(self, context, index=0, retry=1):
        self.retry = retry
        self.lock = threading.Lock()
        self.context = context
        self.index = index
        self.executed_sql = []

    def __repr__(self):
        return '<SqlStore index=%s>' % self.index

    @cached_property
    def _conn(self):
        CONTEXT_HOST = self.context.config.get('DB_HOST')
        CONTEXT_PORT = self.context.config.get('DB_PORT')
        CONTEXT_USER = self.context.config.get('DB_USER')
        CONTEXT_PAWD = self.context.config.get('DB_PASSWD')
        CONTEXT_DBNAME = self.context.config.get('DB_NAME')
        return connect_db(CONTEXT_HOST, CONTEXT_PORT,
                          CONTEXT_USER, CONTEXT_PAWD,
                          CONTEXT_DBNAME)

    def init_app(self, app):
        self.app = app

    def is_testing(self):
        # DO NOT CHANGE THIS METHOD!
        return False

    # pylint: disable=E0213, E1102
    def execute_retry(func):
        def call(self, *args, **kwargs):
            attempts = 0
            while True:
                self.lock.acquire()
                try:
                    return func(self, *args, **kwargs)
                except MySQLdb.Error as e:
                    if attempts >= self.retry:
                        raise
                    if e.args[0] == 2006:
                        # gone away
                        self._conn.close()
                        self.__dict__.pop('_conn', None)
                    attempts += 1
                finally:
                    self.lock.release()

        return call

    # pylint: enable=E0213, E1102

    @execute_retry
    def execute(self, sql, args=None):
        cmd = self.parse_execute_sql(sql)

        if args is not None and not isinstance(args, (list, tuple, dict)):
            args = (args,)

        for retry in xrange(self.retry, -1, -1):
            try:
                cursor = self._conn.cursor()

                norm = sql.lower()

                # update and delete need where
                if cmd in ('delete', 'update') and 'where' not in norm:
                    raise Exception('delete without where is forbidden')

                # check is readonly
                if cmd != 'select':
                    if self.context.config.get('DB_READ_ONLY') is True:
                        raise Exception('Database is read only')

                # execute sql
                if (sql, args,) not in self.executed_sql:
                    cursor.execute(sql, args)
                    self.executed_sql.append((sql, args))

                # if cmd is select, auto commint and return all
                if cmd == 'select':
                    self.commit()
                    return cursor.fetchall()

                # if cmd is insert and has lastrowid, return id
                if cmd == 'insert' and cursor.lastrowid:
                    return cursor.lastrowid

            except MySQLdb.OperationalError:
                exc_class, exception, tb = sys.exc_info()

                if not retry:
                    raise exc_class, exception, tb
        return cursor

    def get_cursor(self):
        return self._conn.cursor()

    cursor = property(get_cursor, )

    def commit(self):
        r = self._conn.commit()
        for sql, args in self.executed_sql:
            self.executed_sql = []
        return r

    def rollback(self):
        r = self._conn.rollback()
        self.executed_sql = []
        return r

    def parse_execute_sql(self, sql):
        sql = sql.lstrip()
        cmd = sql.split(' ', 1)[0].lower()
        if self.is_testing():
            SQL_PATTERNS['truncate'] = re.compile(
                r'truncate\s`?(?P<table>\w+)`?')
        if cmd not in SQL_PATTERNS:
            raise Exception('SQL command %s is not yet supported' % cmd)
        match = SQL_PATTERNS[cmd].match(sql)
        if not match:
            raise Exception(sql)

        # tables = [t for t in find_tables(sql) if t in self.tables]
        # table = match.group('table')

        # if table in tables:
        #     tables.remove(table)

        # return cmd, [table] + list(tables)
        return cmd

    @classmethod
    def init_by_context(cls, context):
        return SqlStore(context)
