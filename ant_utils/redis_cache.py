# -*- coding: utf-8 -*-

import os, sys
import redis

class RedisCache(object):
    def __init__(self, host, port, password, db):
        self._redis = redis.StrictRedis(host=host, port=port, password=password, db=db)

    def get(self, key):
        return self._redis.get(key)

    def set(self, key, value, expire=0):
        self._redis.set(key, value)
        if expire:
            self._redis.expire(key, expire)

