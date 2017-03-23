# -*- coding: utf-8 -*-
import  hashlib
import time
mayi_proxies = {u'http': u'http://182.92.1.222:9064'}


def get_authHeader():
    appkey = u'179617456'
    secret = u'6abb87c25d47a9f242cde8bbace18d10'
    authHeader = u''

    paramMap = {u'app_key': appkey, u'timestamp': time.strftime("%Y-%m-%d %H:%M:%S"), u'timeout': 30000}
    keys = paramMap.keys()
    keys.sort()
    codes = u'%s%s%s' % (secret, str().join('%s%s' % (key, paramMap[key]) for key in keys), secret)
    sign = hashlib.md5(codes).hexdigest().upper()
    paramMap["sign"] = sign
    keys = paramMap.keys()
    authHeader = u'MYH-AUTH-MD5 ' + str('&').join('%s=%s' % (key, paramMap[key]) for key in keys)
    return authHeader