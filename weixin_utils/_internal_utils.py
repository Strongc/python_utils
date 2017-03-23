#!/usr/bin/env python
# coding=utf-8

"""
Wechat._internal_utils
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This module contains all common functions 
using in spider process.
"""


from functools import wraps
from lxml import etree
from lxml.html.clean import Cleaner
from StringIO import StringIO as StringIO

from ..url_utils.url_open import webpage_open
from .exceptions import (UrlError, GetContentError, NoResultFind,
                        ParseElementError)
from .re_models import check_result_box
from .re_models import (mp_name_xpath, mp_title_xpath, mp_func_intro, 
                        mp_open_id, mp_logo_url)
from .re_models import (news_title_xpath, news_intro_xpath, 
                        news_time_xpath, news_url_xpath)

cleaner = Cleaner(page_structure=False, links=False, safe_attrs_only=False)

def check_result_content(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        content = f(*args, **kwargs)
        if check_result_box.search(content):
            return content
        else:raise NoResultFind('No Result find!')
    return wrapper

@check_result_content
def get_url_content(url=None, use_proxy=False, try_time=5):
    """Given a web page url, returns the page content.
    :@param `url`:page link.
    :@param `try_time`:if requests failed, try again.
    """

    if url is None:
        raise UrlError('url value error:{0!r}'.format(type(url).__name__))
    for i in xrange(try_time):
        response = webpage_open(url, use_proxy=use_proxy)
        if response.status_code == 200:
            return response.text
    raise GetContentError('For url:{0!r}'.format(url))

def get_gzh_element_from_xpath(tree):
    """
    : rtype:
    """

    mp_name_elements    = tree.xpath(mp_name_xpath)
    mp_title_elements   = tree.xpath(mp_title_xpath)
    mp_funir_elements   = tree.xpath(mp_func_intro)
    mp_openid_elements  = tree.xpath(mp_open_id)
    mp_logo_img_elements= tree.xpath(mp_logo_url)
    if (
        len(mp_name_elements)   ==
        len(mp_title_elements)  ==
        len(mp_funir_elements)  ==
        len(mp_openid_elements) ==
        len(mp_logo_img_elements)
        ): 

        return zip(mp_name_elements, mp_title_elements, 
                   mp_funir_elements, mp_openid_elements, 
                   mp_logo_img_elements)
    else:
        raise ParseElementError('parse gzh element failed')

def get_text(x):

    text = []
    if x.text:
        text.append(x.text)
    for ch_ in x.iterdescendants():
        if ch_.text:
            text.append(ch_.text.strip())
        if ch_.tail:
            text.append(ch_.tail.strip())
    return ''.join(text)

def get_tree(content):
    cleancontent = cleaner.clean_html(content)
    f = StringIO(cleancontent)
    parser = etree.HTMLParser()
    tree = etree.parse(f, parser)
    return tree

def get_gzh_infos(key_name=None, content=None):
    """This method is intend to get gzh basic infos."""

    if key_name is None:
        raise ValueError('mp_name:[%s], mp_name type: %r' %(key_name, type(key_name)))
    if content is None:
        raise ValueError('can not get infos from a type:%r content' %type(content))
    if not isinstance(content, (str, basestring)):
        raise ValueError('can not get infos from a type:%r content' %type(content))
    tree = get_tree(content)
    xpath_elements = get_gzh_element_from_xpath(tree)
    for element in xpath_elements:
        r_name, r_title, r_func, r_openid, r_logo = element
        mp_name  = get_text(r_name)

        if mp_name != key_name:
            continue

        mp_title   = get_text(r_title)
        mp_descs   = get_text(r_func)[5:]
        mp_open_id = r_openid.attrib.get('d', 0)
        mp_logo_url= r_logo.attrib.get('src', '')
        keys = ['mp_name', 'mp_title', 'mp_descs', 'mp_open_id', 'mp_logo_url']
        value= [mp_name, mp_title, mp_descs, mp_open_id, mp_logo_url]
        return dict(zip(keys, value))

def get_news_infos(content=None):
    if content is None:
        raise ValueError('news list content is error:{0!r}'.format(type(content)))

    tree = get_tree(content)
    news_title_elements = tree.xpath(news_title_xpath)
    news_intro_elements = tree.xpath(news_intro_xpath)
    news_time_elements  = tree.xpath(news_time_xpath)
    news_url_elements   = tree.xpath(news_url_xpath)
    news_title_list = list()
    news_intro_list = list()
    news_time_list  = list()
    news_url_list   = list()
    for title_element in news_title_elements:
        news_title_list.append(get_text(title_element)) 
    for intro_element in news_intro_elements:
        news_intro_list.append(get_text(intro_element))
    for time_element in news_time_elements:
        timestramp = time_element.attrib.get('t')
        if timestramp:
            news_time_list.append(timestramp)
    for url_element in news_url_elements:
        news_url_list.append(url_element.attrib.get('href'))

    if (len(news_title_list) == len(news_intro_list) == 
            len(news_time_list) == len(news_url_list)):

        ret = list()
        _ret = zip(news_title_list, news_intro_list, news_time_list, news_url_list) 
        for i in _ret:
            keys = ('news_title', 'news_intro', 'news_time', 'news_url')
            ret.append(dict(zip(keys, i)))

        return ret 

    else:
        raise ParseElementError('parse news element failed')

