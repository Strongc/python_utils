# -*- coding: utf-8 -*-
import os
import sys
import re
import urlparse
import requests
from myrequests import MyRequests
from csdetect import detect
from proxies import get_mayi_proxy

_reo_refresh_url = re.compile(r'<meta\s+.*?http-equiv="refresh" content="\d+.*?url=([^"]+)">', re.I)

def open_url(url, redirect=True, timeout=5, header=None, logger=None):
    try:
        r = requests.head(url, timeout=timeout, headers=header)
        headers = {k.lower():v for k,v in r.headers.items()}
        ct = headers.get('content-type', 'none')
        if logger:
            logger.info('open_url content-type: [%s]' % ct)
        if ct.find('text') != 0:
            return None

        location = headers.get('location', '')
        if location.strip('/') != url and location.find('http') == 0 and redirect:
            if logger:
                logger.info('open_url get location: [%s]' % (location))
            if location != url:
                return open_url(location, redirect=False, timeout=timeout, header=header)
        
        r = requests.get(url, timeout=timeout, headers=header, stream=True)
        if r.status_code != 200:
            if logger:
                logger.info('open_url use proxy')
            myrequests = MyRequests()
            r = myrequests.get(url, timeout=timeout, try_count=2)

        data = ''
        for chunk in r.iter_content(chunk_size=40960):
            data += chunk
        if logger:
            logger.info('open_url get data len [%d]' % len(data))

        m = _reo_refresh_url.search(data)
        if m and redirect:
            if len(m.group(1)) > 0:
                refresh_url = m.group(1)
                refresh_url = urlparse.urljoin(url, refresh_url)
                if logger:
                    logger.info('open_url get refresh url: [%s]' % (refresh_url))
                if refresh_url != url:
                    return open_url(refresh_url, redirect=False, timeout=timeout, header=header)

        cs = detect(data)
        if logger:
            logger.info('open_url get charset [%s]' % cs)
        if not cs:
            return None
        else:
            return data.decode(cs, 'ignore')
    except Exception as e:
        if logger:
            logger.error('open_url exception: %s' % (str(e)))
        return None

def proxy_url_open(url, timeout=5):
    p = get_mayi_proxy()
    session = requests.Session()
    session.keep_alive = False
    headers = {'Proxy-Authorization': p.header}
    r = session.get(url=url, proxies=p.address, headers=headers, timeout=timeout)
    if r.status_code != 200:
        return None

    data = ''
    for chunk in r.iter_content(chunk_size=40960):
        data += chunk

    cs = detect(data)
    if not cs:
        return None
    return data.decode(cs, 'ignore')

