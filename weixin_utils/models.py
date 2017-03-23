#!/usr/bin/env python
# coding=utf-8

"""
wechat.models
~~~~~~~~~~~~~~~~~

This module contains the primary objects that powered Weixin.
"""

import time

from ._internal_utils import (get_url_content, get_gzh_infos, 
                              get_news_infos)

class WechatRequests(object):
    """The :class:`WechatRequests` object, which contains primary 
       infos of requests.

    """

    def __init__(self, method  = None, 
                       mp_name = None,
                       mp_id   = None, 
                       time_id = 1, 
                       status  = 'slow', 
                       try_time= 5,
                       lastmodified = int(time.time())):
        super(WechatRequests, self).__init__()
        #: to check mp_name format whether right.
        self.method  = method
        self.mp_name = mp_name
        self.mp_id   = mp_id
        self.time_id = time_id
    
class WeichatParsers(object):
    """This class is the main process, have two basic funcs,
    #``get_basic_infos``: return the basic infos like `mp_name`, `mp_title`,
    ...of the gzh.
    #``get_news_infos`` : return the news infos like `news url`, `news_title`
    of the gzh.

    """

    gzh_list_base_url = u'http://weixin.sogou.com/weixin?type=1&query='

    gzh_news_base_url = (u'http://weixin.sogou.com/weixin?type=2&ie=utf8&'
                          'query=%(mp_name)s&tsn=%(time_id)s&wxid=%(mp_id)'
                          '&usip=%(mp_name)s&from=tool')

    def __init__(self):
        super(WeichatParsers, self).__init__()

    @classmethod
    def get_basic_info(cls, wxreq):
        """This methoc is to get basic info of the request gzh.
        :wxreq: class: `WechatRequests`.
        :rtype: ``gzh_basic_info``:class `dict`.

        """
        mp_name = None
        proxy = False
        try_time = 5
        if hasattr(wxreq, 'mp_name'):
            mp_name = wxreq.mp_name
        if hasattr(wxreq, 'proxy'):
            proxy = wxreq.proxy
        if hasattr(wxreq, 'try_time'):
            try_time = wxreq.try_time
        gzh_list_url = cls.gzh_list_base_url + mp_name
        gzh_list_con = get_url_content(gzh_list_url, proxy=proxy, try_time=try_time)
        gzh_basic_info = get_gzh_infos(gzh_list_con)
        return  gzh_basic_info

    @classmethod
    def get_news_info(cls, wxreq):
        #: maybe has bug!!!!
        kwargs = None   

        if isinstance(wxreq, WechatRequests):
            keys = ['mp_name', 'mp_id', 'time_id', 'lastmodified']
            values = [wxreq.mp_name, wxreq.mp_id, wxreq.lastmodified]
            kwargs = dict((keys, values))
        if kwargs is None:
            raise ValueError('get news info funcs param error:{0}'.format(type(kwargs)))
        gzh_news_url = cls.gzh_news_base_url %(kwargs)
        gzh_news_con = get_url_content(gzh_news_url)
        if gzh_news_con:
            return get_news_infos(gzh_news_con)

class WechatResponse(object):
    """The :class:`WechatResponse` object, which contains primary
    infos for weixin spider.

    """
    __attrs__ = [mp_name, mp_title, mp_logo_img, mp_func_intr, 
                 mp_wx_certify, news_urls]

    def __init__(self):
        super(WechatResponse, self).__init__()
        self.mp_name  = None
        self.mp_title = None
        self.mp_logo_img = None
        self.mp_open_id  = None
        self.mp_func_intr = None
        self.mp_wx_certify= None
        
        #: type:list of `dict`
        #: include {'news_title', 'news_intro', 'news_time', 'news_url'}
        self.news = None

    def __repr__(self):
        return '<Wechat Response object [%s]>' %self.mp_name
