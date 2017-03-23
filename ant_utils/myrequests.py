
# -*- coding: utf-8 -*-

import requests
import time
from proxy import Proxy

class MyRequests(object):
    def __init__(self):
        self.session = requests.Session()
        self.session.keep_alive = False

    def get(self, url, timeout=5, try_count=1, accepts=[200], header=None):
        for i in range(max(1, try_count)):
            if header:
                headers = header
                headers['Proxy-Authorization'] = Proxy.get_header()  #add custom header
            else:
                headers = {'Proxy-Authorization': Proxy.get_header()}
            r = self.session.get(url=url, proxies=Proxy.get_proxies(), headers=headers, timeout=timeout)
            if r.status_code not in accepts:
                print '[%s][%d]' % (url, r.status_code)
                time.sleep(1)
                continue
            break
        return r

    def head(self, url, timeout=5, header=None):
        if header:
            headers = header
            headers['Proxy-Authorization'] = Proxy.get_header()  # add custom header
        else:
            headers = {'Proxy-Authorization': Proxy.get_header()}
        r = self.session.head(url=url, allow_redirects=True, proxies=Proxy.get_proxies(), headers=headers, timeout=timeout)
        return r.url

if __name__ == '__main__':
    myrequests = MyRequests()
    r = myrequests.get('http://www.baidu.com')
    print r.content