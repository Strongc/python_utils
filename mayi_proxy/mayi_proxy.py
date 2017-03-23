# -*- coding: utf-8 -*-
import requests
import hashlib
import time
import re
import random
from mayi_proxy_config import *
"""
Create By liuriji For Mayi Proxy Usage
"""
def error(func):
    def _error(*args,**kwargs):
        try:
            func(*args,**kwargs)
        except Exception,err:
            print err
            pass
        return _error

def running(func):
    def _running(*args,**kwargs):
        while True:
            func(*args,**kwargs)
    return _running

def multiple_replace(text,adict):  
    rx = re.compile('|'.join(map(re.escape,adict)))
    def one_xlat(match):  
        return adict[match.group(0)]  
    return rx.sub(one_xlat,text)

def decodeHtml(content,decodeList):
    content = multiple_replace(content,decodeList)
    return content

class MayiRequests(object):
    def __init__(self):
        self.session = requests.Session()
        self.session.keep_alive = False
        self.proxies = mayi_proxies
        self.ua = [
		u'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.87 Safari/537.36',
		u'Mozilla/5.0 (Future Star Technologies Corp.; Star-Blade OS; x86_64; U; en-US) iNet Browser 4.7',
		u'Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.0)',
		u'Opera/9.27 (Windows NT 5.2; U; zh-cn)'
		]

    def getHeaders(self,flag):
        if flag:
            headers = {
                "Accept":u"text/html,application/xhtml+xml,application/xml;",
                "Accept-Encoding":u"gzip",
                "Accept-Language":u"zh-CN,zh;q=0.8",
                "User-Agent":random.choice(self.ua)
                }
        else:
            headers={'Proxy-Authorization': get_authHeader()}
        return headers



    def visitUrl(self,url,flag=True):
        headers = self.getHeaders(flag)
        if flag:
            request = self.session.get(url=url,headers=headers,timeout=60)
        else:
            request = self.session.get(url=url,proxies=self.proxies,headers = headers,timeout=60)
        return request


if __name__ =='__main__':
    myRequests  = MayiRequests()
    req = myRequests.visitUrl(u'http://www.baidu.com',True) 
    print req.text
