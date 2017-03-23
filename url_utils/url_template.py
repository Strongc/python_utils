# -*- coding: utf-8 -*-

import sys
import re
import urlparse
import urllib

def _ctype(c):
    return 0 if c.isalnum() else 1

def _split(s):
    a = []
    i, e = 0, len(s)
    while i < e:
        t, j = _ctype(s[i]), i + 1
        while j < e and _ctype(s[j]) == t:
            j += 1
        a.append((s[i:j], t))
        i = j
    return a

_reo_t1 = re.compile(r'([a-z]+?)([0-9]{4,})$', re.I)
def _template(i, um=''):
    s, t = i
    if t == 1:
        return s
    if s.isdigit():
        return 'D'
    if s.isalpha():
        return s
    m = _reo_t1.match(s)
    if m:
        return '%sD' % m.group(1)
    if len(s) >= 24:
        return 'X'
    return um if um else s

def _path(s):
    return ''.join([_template(x) for x in _split(s)])

def _last(s):
    t = s.split('.')
    if len(t) == 2:
        return ''.join([_template(x, 'X') for x in _split(t[0])]) + '.' + t[1]
    else:
        return ''.join([_template(x, 'X') for x in _split(s)])

def _query(s):
    return ''.join([_template(x, 'X') for x in _split(s)])

def get_template(url):
    try:
        url = url.encode('utf8')
        t = urlparse.urlparse(url)
        ps = t.path.split('/')
        
        nps = []
        for i,p in enumerate(ps):
            nps.append(_path(p) if i+1 < len(ps) else _last(p))
       
        qs = {k : _query(v[0]) for k,v in urlparse.parse_qs(t.query).items() if len(v) == 1}

        template = urlparse.urlunparse((t.scheme, t.netloc, '/'.join(nps), '', urllib.urlencode(qs), _path(t.fragment)))
        return template if template != url else ''
    except Exception as e:
        print 'get template of [%s] exception: [%s]' % (url, str(e))
        return ''

