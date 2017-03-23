# coding: utf-8
import os
import sys
from collections import defaultdict
from lxml import html
from lxml.html.clean import Cleaner
CWD = os.path.dirname(os.path.abspath(__file__))
sys.path.append('%s/../' % CWD)
from url_utils.url_format import get_domain
from str_utils.str_format import normalize

_cleaner = Cleaner()
_cleaner.scripts = True
_cleaner.javascript = True
_cleaner.comments = True
_cleaner.style = True

def _get_links(url, content):
    try:
        tree = _cleaner.clean_html(html.fromstring(content))
        tree.make_links_absolute(url)
        root = tree.getroottree()
    except Exception as e:
        return {}
    
    links = defaultdict(list)
    for e in root.iter():
        if e.tag != 'a' or 'href' not in e.attrib:
            continue            	
        href = e.attrib['href']
        title = normalize(unicode(e.text_content()))
        if not title and 'title' in e.attrib:
            title = normalize(unicode(e.attrib['title']))
        if href.find('http') != 0 or not title:
            continue
        nodes = root.getpath(e).split('/')
        if len(nodes) > 1 and nodes[-1][-1] != ']' and nodes[-2][-1] == ']':
            nodes[-2] = nodes[-2][:nodes[-2].find('[')] + '[D]'
        elif nodes[-1][-1] == ']':
            nodes[-1] = 'a[D]'
        xpath = '/'.join(nodes)
        #print xpath, title
        links[xpath].append((href, title))
    return links

def extract(url, content):
    domain = get_domain(url)
    links = _get_links(url, content)
    items = links.items()
    items.sort(key=lambda x:x[0], reverse=True)
    for bp, links in items:
        if len(links) < 3:
            continue
        
        #print bp, len(links)
        #for href, title in links[:4]: print '\t', title 
        avtlen = sum([len(x[1]) for x in links]) / float(len(links))
        if avtlen <= 3 or avtlen >= 6:
            continue
        
        outlinks = {}
        outdomains = set()
        for href, title in links:
            x = get_domain(href)
            if x != domain:
                outdomains.add(x)
                outlinks[href] = title
        if len(outdomains) >= 3 and len(outdomains) / float(len(links)) >= 0.4:
            return outlinks

    return {}

