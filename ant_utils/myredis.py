# -*- coding: utf-8 -*-

import os, sys
import redis
from hash64 import hash64

class MyRedis(object):
    def __init__(self, host, port, password, db, version='', expiry=0, to_id=False):
        self._redis = redis.StrictRedis(host=host, port=port, password=password, db=db)
        self._version = version
        self._expiry = expiry
        self._to_id = to_id

    def mkkey(self, key):
        if type(key) == type(u''):
            key = key.encode('utf8')
        new_key = self._version + str(key)
        if self._to_id:
            new_key = hash64(new_key)
        return new_key

    def get(self, key):
        return self._redis.get(self.mkkey(key))

    def set(self, key, value, expire=0):
        new_key = self.mkkey(key)
        self._redis.set(new_key, value)
        if expire or self._expiry:
            self._redis.expire(new_key, expire or self._expiry)

    def incr(self, key, count=1, expire=0):
        new_key = self.mkkey(key)
        r = self._redis.incr(new_key, count)
        if expire or self._expiry:
            self._redis.expire(new_key, expire or self._expiry)
        return r
    
    def hset(self, key, field, value, expire=0):
        new_key = self.mkkey(key)
        self._redis.hset(new_key, field, value)
        if expire or self._expiry:
            self._redis.expire(new_key, expire or self._expiry)

    def hget(self, key, field):
        return self._redis.hget(self.mkkey(key), field)

    def hgetall(self, key):
        new_key = self.mkkey(key)
        return self._redis.hgetall(new_key)

    def delete(self, key):
        new_key = self.mkkey(key)
        return self._redis.delete(new_key)

    def lpush(self, key, value, expire=0):
        new_key = self.mkkey(key)
        r = self._redis.lpush(new_key, value)
        if expire or self._expiry:
            self._redis.expire(new_key, expire or self._expiry)
        return r
    
    def rpush(self, key, value, expire=0):
        new_key = self.mkkey(key)
        r = self._redis.rpush(new_key, value)
        if expire or self._expiry:
            self._redis.expire(new_key, expire or self._expiry)
        return r

    def lpop(self, key):
        new_key = self.mkkey(key)
        return self._redis.lpop(new_key)

    def rpop(self, key):
        new_key = self.mkkey(key)
        return self._redis.rpop(new_key)

    def llen(self, key):
        new_key = self.mkkey(key)
        return self._redis.llen(new_key)

    def lrange(self, key, start, end):
        new_key = self.mkkey(key)
        return self._redis.lrange(new_key, start, end)
    
