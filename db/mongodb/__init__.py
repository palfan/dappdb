# -*- coding: utf-8 -*-

"""
A simple client for MongoDB

db          -> MongoDB中的一个数据库
collection  -> MongoDB中的一个表
document    -> MongoDB中的一个文档

例如，一个用户需要放在一个叫user的collection中，document的id可以是user的id
那么，mongodb.get('user', 'userid') 可以拿到用户的数据

mongodb.set('user', 'userid')可以设置一个用户的数据

不需要直接使用这个类，而推荐使用models.mixin.props里的类
"""

import simplejson as json
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError

from ..utils import encode

_MC_KEY_DOC = 'mongo:%s:%s'


class MongoDB(object):

    def __init__(self, context, cache_store):
        server = context.config.get('MONGO_SERVER')
        self.server = MongoClient(server)
        self.db = context.config.get('MONGO_DB')
        self._cache = dict()
        self.mc = cache_store
        self.name = context.config.get('MONGO_SERVER') or 'master'

    def __repr__(self):
        return '<MongoDB server=%s:%s>' % self.server.address

    @classmethod
    def init_by_context(cls, context, cache_store):
        return cls(context, cache_store=cache_store)

    def _set_cache(self, name, value):
        """
        A simple db mem cache
        """
        if not self._cache.get(name):
            self._cache[name] = value

    def _get_cache(self, name):
        """
        A simple db mem cache
        """
        return self._cache.get(name, None)

    def _get_db(self, name):
        """
        A method get db connection
        """
        db = self.server[name]
        return db

    def _get_collection(self, name):
        """
        A method get collection connection
        """
        collection = self.server[self.db].get_collection(name)
        return collection

    def get(self, collection, key, expire=None):
        """
        Get one value from mongodb
        collection -> collection name
        key        -> document id
        will be cached in redis
        """

        _coll = self._get_collection(collection)
        _mc_key = _MC_KEY_DOC % (collection, key)
        if _coll:
            _doc = self.mc.get(_mc_key)
            if _doc is None or str(_doc) == 'null':
                _doc = _coll.find_one({"_id": key})
                _doc = json.dumps(_doc, encoding='utf-8')
                if _doc is None:
                    _doc = dict(empty_cached_props=True)
                self.mc.set(_mc_key, _doc, ex=expire)
            return encode(json.loads(_doc, encoding='utf-8'))

    def delete(self, collection, key):
        """
        Get one value from mongodb
        """

        _coll = self._get_collection(collection)
        _coll.delete_one({"_id": key})
        self.mc.delete(_MC_KEY_DOC % (collection, key))

    def set(self, collection, key, value,
            try_again=True, expire=None):
        """
        Set value to mongodb db
        db         -> database name
        collection -> collection name
        key       -> document id
        value      -> document value
        """

        if value is None:
            return
        if not isinstance(value, dict):
            raise Exception('value should be dict')
        if not key:
            raise Exception('key should not be empty')
        if not isinstance(key, str):
            raise Exception('name should be str')
        if value.get('_id'):
            if value.get('_id') != key:
                raise Exception('_id conflict')

        value['_id'] = key
        key = encode(key)
        value = encode(value)
        try:
            json.dumps(value)
        except (ValueError, TypeError):
            raise Exception('value should be jsonize')

        _coll = self._get_collection(collection)
        try:
            _coll.insert_one(value)
            self.mc.set(_MC_KEY_DOC % (collection, key),
                        json.dumps(value), ex=expire)
        except DuplicateKeyError as e:
            # if key conflict then remove from cache
            # and reset to the mongodb
            if try_again:
                self.mc.delete(_MC_KEY_DOC % (collection, key))
                _coll.update_one({"_id": key}, {"$set": value})
                self.mc.set(_MC_KEY_DOC % (collection, key),
                            json.dumps(value), ex=expire)
                return
            raise Exception(
                'MongoDB conflict %s %s: %s' % (collection, key, e)
            )
