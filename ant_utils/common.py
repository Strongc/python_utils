# -*- coding: utf-8 -*-

import os, sys
import re
import urlparse
import requests
CWD = os.path.dirname(os.path.abspath(__file__))
sys.path.append('%s/util' % CWD)
from myrequests import MyRequests
from mylogger import MyLogger
from lxml import etree
import socket
log = MyLogger('logging')

TOP_DOMAINS_SET = set(["cc","com", "cn", "org","net", "edu.cn", "gov.cn", "com.cn", "ac.cn", "net.cn", "org.cn", "mil.cn"])
PROV_DOMAINS_SET = set(["ac.cn","bj.cn","sh.cn","tj.cn",\
                        "cq.cn","he.cn","sx.cn","nm.cn",\
                        "ln.cn","jl.cn","hl.cn","js.cn",\
                        "zj.cn","ah.cn","fj.cn","jx.cn",\
                        "sd.cn","ha.cn","hb.cn","hn.cn",\
                        "gd.cn","gx.cn","sc.cn","gz.cn",\
                        "yn.cn","gs.cn","qh.cn","nx.cn",\
                        "xj.cn","tw.cn","hk.cn","mo.cn",\
                        "xz.cn","hi.cn","sn.cn"])
#eg. anyang.gov.cn -> www.anyang.gov.cn
def is_root_site(site):
    arr = site.split('.')
    root = ".".join(arr[1:])
    if root in TOP_DOMAINS_SET or root in PROV_DOMAINS_SET:
        return True
    return False
#eg. anyang.gov.cn -> www.anyang.gov.cn
def get_host_site(site):
    if is_root_site(site):
        return "www."+site
    return site


def get_host(site):
    tmp = site.split(".")
    x = ""
    for i in range(0, len(tmp)):
        x = ".".join(tmp[i:])
        if is_root_site(x):
            break
    if x != tmp[-1]:
        root_site = x
    else:
        root_site = site
    return root_site


_reo1 = re.compile(ur'[^\u4E00-\u9FA50-9a-zA-Z]+')

def normalize(s):
    s = _reo1.sub('', s.lower())
    return s

_reo2 = re.compile(ur'[^\u4E00-\u9FA5]+')
def chinese(s):
    s = _reo2.sub('', s.lower())
    return s

def joinu(a, c='\t'):
    return c.join([unicode(x) for x in a])
def joins(a, c='\t'):
    return c.join([str(x) for x in a])

re_charset = re.compile(r'<meta\s+.*?charset\s*=\s*"?([\w-]+)"?', re.I)
re_charset2 = re.compile(r'charset\s*=\s*([\w-]+)', re.I)

#<META HTTP-EQUIV="REFRESH" CONTENT="0; URL=html/2016-09/13/node_2.htm">

re_refresh_url = re.compile(r'<meta\s+.*?http-equiv="refresh" content="\d+.*?url=([^"]+)">', re.I)

def open_url(url, use_proxy=False, timeout=5):
    try:
        if not use_proxy:
            r = requests.get(url, timeout=timeout)
            if r.status_code != 200:
                log.info('try proxy')
                return open_url(url, True)
        else:
            myrequests = MyRequests()
            r = myrequests.get(url, timeout=timeout, try_count=4)
        m = re_charset.search(r.text)
        if m:
            r.encoding = m.group(1) if m.group(1).lower().strip() != 'gb2312' else 'gbk'
        return r
    except Exception as e:
        log.info('open_url exception: '+e.message)
        return None

def open_url2(url, redirect=True, timeout=4, header=None):
    import socket
    socket.setdefaulttimeout(timeout)
    try:
        log.info('[%s] start head' % (url))
        r = requests.head(url, timeout=timeout, headers=header)
        headers = {k.lower():v for k,v in r.headers.items()}
        ct = headers.get('content-type', 'none')
        if ct.find('text') != 0:
            log.error('content-type error: '+ct)
            return None
        log.info('[%s] end head' % (url))

        location = headers.get('location', '')
        if location.strip('/') != url and location.find('http') == 0 and redirect:
            log.info('[%s] location: [%s]' % (url, location))
            if location != url:
                return open_url2(location, redirect=False, timeout=timeout, header=header)
        log.info('[%s] start get' % (url))
        r = requests.get(url, timeout=timeout, headers=header)
        if r.status_code != 200:
            log.info('[%s] use proxy' % url)
            myrequests = MyRequests()
            r = myrequests.get(url, timeout=timeout, try_count=2)
        log.info('[%s] end get' % (url))

        m = re_charset2.search(ct) or re_charset.search(r.text)
        if m:
            r.encoding = m.group(1).lower().strip()

        if r.encoding == 'gb2312':
            r.encoding = 'gbk'
        if r.encoding.lower() == 'iso-8859-1':  #ISO 往往是错误的
            #r.encoding = r.apparent_encoding #too slow!
            r.encoding = 'gbk'
        # if r.apparent_encoding == "UTF8" and r.encoding != r.apparent_encoding:
        #     r.encoding = r.apparent_encoding
        m = re_refresh_url.search(r.text)
        if m and redirect:
            if len(m.group(1)) > 0:
                refresh_url = m.group(1)
                refresh_url = urlparse.urljoin(url, refresh_url)
                log.info('[%s] refresh_url: [%s]' % (url, refresh_url))
                if refresh_url != url:
                    return open_url2(refresh_url, redirect=False, timeout=timeout, header=header)
        log.info('[%s] finish request' % (url))
        return r
    except Exception as e:
        log.error('open_url [%s] exception: %s' % (url, str(e)))
        return None

def open_url3(url, redirect=True, timeout=5):
    import socket
    socket.setdefaulttimeout(timeout)
    try:
        log.info('[%s] start get' % (url))
        myrequests = MyRequests()
        r = myrequests.get(url, timeout=timeout, try_count=2)
        log.info('[%s] end get' % (url))

        m = re_charset.search(r.text)
        if m:
            r.encoding = m.group(1).lower().strip()

        if r.encoding == 'gb2312':
            r.encoding = 'gbk'
        if r.encoding.lower() == 'iso-8859-1':  #ISO 往往是错误的
            #r.encoding = r.apparent_encoding #too slow!
            r.encoding = 'gbk'
        m = re_refresh_url.search(r.text)
        if m and redirect:
            if len(m.group(1)) > 0:
                refresh_url = m.group(1)
                refresh_url = urlparse.urljoin(url, refresh_url)
                log.info('[%s] refresh_url: [%s]' % (url, refresh_url))
                if refresh_url != url:
                    return open_url3(refresh_url, redirect=False, timeout=timeout)
        log.info('[%s] finish request' % (url))
        return r
    except Exception as e:
        log.error('open_url [%s] exception: %s' % (url, str(e)))
        return None

def get_doc_type(url):
    path = urlparse.urlparse(url).path
    t = path.split('/')[-1].split('.')
    return t[-1].lower() if len(t) > 1 else ''

def get_top_domain(url):
    netloc = urlparse.urlparse(url).netloc
    a = netloc.split('.')
    if a[0] == 'www':
        return '.'.join(netloc.split('.')[1:])
    else:
        return netloc

def get_etree(content):
    return etree.HTML(content)



if __name__ == '__main__':
    #r = open_url2('http://cjrb.cjn.cn/')
    #print r.status_code, r.url, r.text
    
    #print is_root_site("anyang.gov.cn")
    #print get_host_site("anyang.gov.cn")
    #print get_host_site("henan.sina.com.cn")
    #print get_host_site("sina.com.cn")
    #r = open_url2('http://zh.moegirl.org/')
    r = open_url2('http://www.yiyang.ccoo.cn/jiang/')
    print r.status_code
    r.encoding = 'gbk'

    #print r.content

