# coding: utf8
import re
import urlparse

_reo_meta_redirect = re.compile(r'<meta\s+[^>]*?http-equiv="refresh"\s+[^>]*?content="\d+.*?url=([^"]+?)"', re.I|re.DOTALL)
_reo_js_redirect = re.compile(r'<script\s.*?(window|self|document)\.location\W+?(http://[^\'\">\)]+)', re.I|re.DOTALL)

def get_redirect_url(url, content):
    """ get redirect url:
    meta redirect
    js redirect
    """
    m = _reo_meta_redirect.search(content)
    if m:
        return urlparse.urljoin(url, m.group(1))
    
    m = _reo_js_redirect.search(content)
    if m:
        return urlparse.urljoin(url, m.group(2))
    return None

