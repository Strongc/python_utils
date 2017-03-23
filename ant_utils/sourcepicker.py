
# -*- coding: utf-8 -*-

import sys
import re

_reo1 = re.compile(ur'([\u4E00-\u9FA50-9a-zA-Z]+)')

_reo_blacks = re.compile(ur'.*(作者|分享|出处|字|稿|本站|打印|时间|评论).*$')
_reo_postfix = re.compile(ur'[[\u4E00-\u9FA50-9a-zA-Z]+(网|报|闻|社|站|府|局|院|会|声|娱乐|体育|在线|论坛|港|办|中心|市|镇|县|乡)$')
_reo_source = re.compile(ur'[\u4E00-\u9FA5]*(新浪|网易|腾讯|搜狐|凤凰)[\u4E00-\u9FA5]*$')
def picksource(txt):
    cs = _reo1.findall(txt)
    for idx,c in enumerate(cs):
        if c.isdigit():
            continue
        if _reo_blacks.match(c):
            continue
        if len(c) < 2 or len(c) > 9:
            continue
        if _reo_postfix.match(c):
            return c
        if _reo_source.match(c):
            return c
        #if idx > 0 and cs[idx-1].find(u'来源') != -1 and len(c) > 2:
        #    return c
    return None

