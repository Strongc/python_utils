
import time
import hashlib
import random

_MAYI_PROXIES1 = [(u'171354325', u'71520f457e4bb2cbab32f73df85c3195', u'http://s2.proxy.mayidaili.com:8123'),
                  (u'179617456', u'6abb87c25d47a9f242cde8bbace18d10', u'http://s2.proxy.mayidaili.com:8123'),
                  (u'197141576', u'18b4ecaeaa858c3ecae118e3047e99ef', u'http://s2.proxy.mayidaili.com:8123'),
                  (u'146241707', u'c7505cf7a3995e330165da09dda0b41a', u'http://s2.proxy.mayidaili.com:8123')]

_MAYI_PROXIES2 = [(u'42731387', u'b4d62638b6170076a965694ea8a927ac', u'http://s1.proxy.mayidaili.com:8123'),
                  (u'155882394', u'88ee07e5b427522cdb3bd3f0b265ff8b', u'http://s1.proxy.mayidaili.com:8123'),
                  (u'175156648', u'103a281e23c78ec680e55849834bb47d', u'http://s1.proxy.mayidaili.com:8123'),
                  (u'207477904', u'bf87b5e8348a0158447c2557f149789a', u'http://s1.proxy.mayidaili.com:8123')]

class MayiProxy(object):
    def __init__(self):
        self.proxies = None
        self.header = None

def get_mayi_proxy(group=2):
    proxy = MayiProxy()
    if group == 2:
        key, secret, address = random.choice(_MAYI_PROXIES2)
    else:
        key, secret, address = random.choice(_MAYI_PROXIES1)
    proxy.proxies = {'http': address}
    
    params = {'app_key': key, 'timestamp': time.strftime("%Y-%m-%d %H:%M:%S")}
    keys = params.keys()
    keys.sort()
    content = '%s%s%s' % (secret, str().join('%s%s' % (key, params[key]) for key in keys), secret)
    sign = hashlib.md5(content).hexdigest().upper()
    params["sign"] = sign
    proxy.header = 'MYH-AUTH-MD5 ' + str('&').join('%s=%s' % (k, v) for k,v in params.items())
    return proxy
