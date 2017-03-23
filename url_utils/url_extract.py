# -*- coding: utf-8 -*-

import sys
import re
import urlparse
import urllib
from collections import defaultdict
from HTMLParser import HTMLParser
from lxml.html.clean import Cleaner
from lxml import html

cleaner = Cleaner()
cleaner.scripts = True
cleaner.javascript = True
cleaner.comments = True
cleaner.style = True

_inline_tags = ['b', 'big', 'i', 'small', 'tt', 'strong', 'font', 'abbr', 
        'acronym', 'cite', 'code', 'dfn', 'em', 'kbd', 'strong', 'samp', 
        'time', 'var', 'a', 'bdo', 'br', 'img', 'map', 'object', 'q', 
        'script', 'span', 'sub', 'sup']
_inline_tags = set(_inline_tags)

_block_tags = ['tr', 'body', 'address', 'article', 'aside', 'blockquote', 
        'canvas', 'div', 'dl', 'fieldset', 'figcaption', 'figure', 'figcaption', 
        'footer', 'form', 'header', 'hgroup', 'hr', 'li', 'main', 'nav', 
        'noscript', 'ol', 'output', 'p', 'section', 'table', 'tfoot', 'ul']

_reo_block_node = re.compile(r'(%s)\[\d+\]$' % '|'.join(_block_tags), re.I)
def get_base_path(path):
    ns = path.split('/')
    i = len(ns) - 1
    while i >= 0:
        m =  _reo_block_node.match(ns[i])
        if m:
            ns[i] = m.group(1) + '[D]'
            break
        i -= 1
    return '/'.join(ns)

_reo_chinese = re.compile(ur'[^\u4E00-\u9FA5]+')
def chinese(s):
    s = _reo_chinese.sub('', s.lower())
    return s

def get_links(url, content):
    try:
        tree = cleaner.clean_html(html.fromstring(content))
        tree.make_links_absolute(url)
        root = tree.getroottree()
    except Exception as e:
        return {}
    
    links = []
    for e in root.iter():
        if e.tag != 'a' or 'href' not in e.attrib:
            continue
        href = e.attrib['href']
        title = unicode(e.text_content()).strip()
        xpath = root.getpath(e)
        bpath = get_base_path(xpath)
        links.append((href, title, xpath, bpath))

    return links

