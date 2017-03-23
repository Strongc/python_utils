# -*- coding: utf-8 -*-

import codecs

class BigFileReader(object):
    def __init__(self, path, trunk_size=1024 * 1024 * 32, encode='utf8'):
        if encode:
            self.fh = codecs.open(path, 'rb', encode)
        else:
            self.fh = open(path, 'rb')
        self.trunk_size = trunk_size
        self.tail = ''

    def __del__(self):
        if self.fh:
            self.fh.close()

    def readlines(self):
        if not self.fh:
            return []
        data = self.fh.read(self.trunk_size)
        content = self.tail + data
        if not content:
            return []
        lns = content.split('\n')
        if len(lns) == 1: # trunk size too small
            return []
        if len(data) < self.trunk_size:
            self.tail = ''
            return lns
        else:
            tail = lns[-1]
            return lns[:-1]

