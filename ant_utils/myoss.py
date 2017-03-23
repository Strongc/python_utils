# -*- coding: utf-8 -*-

import os, sys
import oss2

class MyOss(object):
    def __init__(self, oss_endpoint = 'oss-cn-hangzhou.aliyuncs.com',
            bucket_name = 'bdtt-api', oss_key_id = 'RBbUeiN8FHcPc0As',
            oss_key_value = '6Not2pLjT1drEr4imxiCXgieZTpXgs', protocol = 'http://',
            url_head = 'http://img.benditoutiao.com/'):
        self.oss_endpoint = oss_endpoint
        self.bucket_name = bucket_name
        self.oss_key_id = oss_key_id
        self.oss_key_value = oss_key_value
        self.protocol = protocol
        self.url_head = url_head
        self.separator = '/'
        auth = oss2.Auth(self.oss_key_id, self.oss_key_value)
        self.bucket = oss2.Bucket(auth, self.protocol + self.oss_endpoint, self.bucket_name)

    def upload(self, content, path):
        r = self.bucket.put_object(path, content)
        return self.url_head + path if r else None

def get_oss():
    return MyOss()
