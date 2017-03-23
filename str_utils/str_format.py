# -*- coding: utf-8 -*-
'''
  常用的字符串处理函数
'''
import os
import sys
import re

def normalize(s, u2l=True, f2h=True, repl=''):
    arr = []
    for c in s:
        u = ord(c)
        if 0xFF01 <= u <= 0xFF5E: # ascii fullwidth char
            u -= 0xFEE0
            c = unichr(u)

        if 0x30 <= u <= 0x39:
            arr.append(c)
        elif 0x41 <= u <= 0x5A:
            arr.append(c.lower() if u2l else c)
        elif 0x61 <= u <= 0x7A:
            arr.append(c)
        elif 0x4E00 <= u <= 0x9FBB:
            arr.append(c)
        else:
            continue
    return repl.join(arr)

_reo_space = re.compile(ur'[\x00-\x20\x7F-\xA0\u1680\u180E\u2000-\u200B\u2028\u2029\u202F\u205F\u3000\uFEFF]+')
def replace_space(s, repl=''):
    return _reo_space.sub(repl, s)

_reo_chinese = re.compile(ur'[^\u4E00-\u9FBB]+') # 0x4E00-0x9FA5 + 0x9FA6-0x9FBB
def chinese(s):
    s = _reo_chinese.sub('', s)
    return s

def joinu(a, c='\t'):
    return c.join([unicode(x) for x in a])

def joins(a, c='\t'):
    return c.join([str(x) for x in a])

_reo_title = re.compile(ur'[\uFF01-\uFF5D\u4E00-\u9FA50-9a-zA-Z\s?!":()~,%？！“”：（）~，%、《》。…]+$')
def is_news_title(s):
    s = replace_space(s, ' ')
    m = _reo_title.match(s)
    if not m:
        return False
    tlen = len(chinese(s))
    if tlen < 12 or tlen > 35:
        return False
    return True

