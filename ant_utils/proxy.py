
import time
import hashlib
import random

class Proxy(object):
    @staticmethod
    def get_proxies():
        return {u'http': u'http://182.92.1.222:8123'}

    @staticmethod
    def get_address():
        return 'http://182.92.1.222:8123'
    
    @staticmethod
    def get_header():
        appkey = u'179617456'
        secret = u'6abb87c25d47a9f242cde8bbace18d10'
        paramMap = {u'app_key': appkey, u'timestamp': time.strftime("%Y-%m-%d %H:%M:%S")}
        keys = paramMap.keys()
        keys.sort()
        codes= u'%s%s%s' % (secret, str().join('%s%s' % (key, paramMap[key]) for key in keys), secret)
        sign = hashlib.md5(codes).hexdigest().upper()
        paramMap["sign"] = sign
        keys = paramMap.keys()
        authHeader = u'MYH-AUTH-MD5 ' + str('&').join('%s=%s' % (key, paramMap[key]) for key in keys)
        return authHeader

