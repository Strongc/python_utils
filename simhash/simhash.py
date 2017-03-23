# coding: utf8
""" simhash implementation
references:
    https://github.com/leonsim/simhash
"""
import re
import hashlib
import collections
from itertools import groupby


def _hash1(s):
    return int(hashlib.md5(s).hexdigest(), 16)


def _hash2(v, hashbits=128):
    if not v:
        return 0
    else:
        x = ord(v[0])<<7
        m = 1000003
        mask = 2**hashbits-1
        for c in v:
            x = ((x*m)^ord(c)) & mask
        x ^= len(v)
        if x == -1:
            x = -2
        return x


class Simhash(object):
    __reo_bin = re.compile(r'[01]+$')
    
    def __init__(self, value, d=64, reg=ur'[\w\u4E00-\u9FA50-9a-zA-Z]', hash_func=None, 
            slide_size=4):
        """
        `value`: basestring or iterable object
        `d`: dimensions of fingerprints
        `reg`: used to normalize value when value's type is basestring
        """
        self.d = d
        self.reo = re.compile(reg)
        self.slide_size = slide_size
        self.value = None
        self.hash_func = hash_func or _hash1

        if isinstance(value, Simhash):
            self.value = value.value
        elif isinstance(value, basestring):
            if Simhash.__reo_bin.match(value) and len(value) == self.d:
                self.value = value
            else:
                self.build_by_text(unicode(value))
        elif isinstance(value, collections.Iterable):
            self.build_by_features(value)
        else:
            raise Exception('Bad parameter with type {}'.format(type(value)))

    def _slide(self, text):
        return [text[i:i + self.slide_size] for i in 
                range(max(len(text) - self.slide_size + 1, 1))]

    def _tokenize(self, text):
        text = ''.join(self.reo.findall(text.lower()))
        return self._slide(text)

    def build_by_text(self, text):
        features = self._tokenize(text)
        features = {k:sum(1 for _ in g) for k, g in groupby(sorted(features))}
        self.build_by_features(features)

    def build_by_features(self, features):
        v = [0] * self.d
        masks = [1 << i for i in range(self.d)]

        if isinstance(features, dict):
            features = features.items()
        for k,w in features:
            h = self.hash_func(k.encode('utf-8'))
            for i in range(self.d):
                v[i] += w if h & masks[i] else -w

        self.value = ''.join(['1' if v[i] >=0 else '0' for i in range(self.d)])

    def distance(self, another):
        assert self.d == another.d
        return sum([1 if self.value[i] != another.value[i] else 0 
                for i in range(self.d)])

