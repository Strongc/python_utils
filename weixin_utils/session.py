#!/usr/bin/env python
# coding=utf-8

"""
weixin.session
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This module to build a session.
"""

from .models import (WechatRequests, WechatResponse, WechatParsers)

class Session(object):
    def __init__(self):
        super(Session, self).__init__()
        self.method = None

    def weixinrequest(self, method, mp_name, mp_id, lastmodified=None):
        wxreq = WechatRequests(method, mp_name, mp_id, lastmodified) 
        return self.adapter(wxreq)

    def adapter(self, wxreq=None):
        wxrsp = WechatResponse()
        if wxreq.method is not None:
            self.method = wxreq.method.upper()

        if self.method == 'GET_BASIC_INFOS':
            ret = WechatParsers.get_basic_info(wxreq)
            wxrsp.mp_name = ret.mp_name
            wxrsp.mp_title= ret.mp_title
            wxrsp.mp_logo_img   = ret.mp_logo_img
            wxrsp.mp_func_intr  = ret.mp_func_intr
            wxrsp.mp_wx_certify = ret.mp_wx_certify

        if self.method == 'GET_NEWS_INFOS':
            ret = WechatParsers.get_news_info(wxreq)
            wxrsp.news = ret

        return wxrsp
