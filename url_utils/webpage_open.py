# coding: utf8
import re
import logging
import urlparse
from url_open3 import url_open
from response import Response
from csdetect import detect
from redirect import get_redirect_url

def webpage_open(url, use_proxy=0, max_redirects=1, 
        logger=logging.getLogger(__name__)):
    r = url_open(url, use_proxy=use_proxy, timeout=30)
    if r.status_code != 200:
        return r
    if max_redirects > 0:
        redirect_url = get_redirect_url(url, r.content[:4096])
        if redirect_url:
            logger.info('[%s] redirect to [%s]' % (url, redirect_url))
            return webpage_open(redirect_url, use_proxy, max_redirects-1)

    cs = detect(r.content)
    if not cs:
        r.status_code = Response.CHARSET_UNKNOWN
    else:
        r.content = r.content.decode(cs, 'ignore')
    return r
