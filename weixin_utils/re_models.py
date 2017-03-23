#!/usr/bin/env python
# coding=utf-8

"""
Wechat.re_models
~~~~~~~~~~~~~~~~~~~~~~~~~~~

This module is some regressions set power for weixin spider.
"""


import re

#: for common                                                                        #
"""                                                                                  
:@ split_bracket: abstract content in brackets like `<--em-- xxx --em-->xx<...>`.     
:@ check_result: to check content whether has search result.
"""

split_bracket_box = re.compile(r'<.+?>')

check_result_box = re.compile(r'noresult_part1_container')


#: for gzhs list page                                                                #
"""
:@ mp_name_box: abstract gzh english letter name like `wyxiyuan`.
:@ mp_title_box: abstract gzh chinese character name like `婺源熹园`.
:@ mp_logo_img: abstract gzh logo image url.
:@ mp_func_intr: abstract gzh function introduction.
:@ mp_wx_certify: abstract gzh wechat certified.
"""
mp_name_box = re.compile(r'<label name="em_weixinhao">(.+?)</label>')

mp_url_title_box= re.compile(
            ur'<a target="_blank" uigs="main_toweixin_account_name_\d+" '
            'href="(.+?)">(.+?)</a><i></i>', re.S)

mp_desc_box= re.compile(
            ur'<dl>.*<dt>功能介绍：</dt>.?<dd>(.+?)</dd>.*</dl>', re.S)

mp_open_id = re.compile(r'<li id="sogou_vr_\d+_box_\d+" d="(.+?)">', re.S)


#: for news list page                                                               # 

"""xpath module
~~~~~~~~~~~~~~~
for gzh list
"""
mp_open_id    = '//*[starts-with(@id, "sogou_vr_11002301_box_")]'
mp_func_intro = '//*[starts-with(@id, "sogou_vr_11002301_box_")]/dl[1]'
mp_logo_url   = '//*[starts-with(@id, "sogou_vr_11002301_box_")]/div/div[1]/a/img'
mp_name_xpath = '//*[starts-with(@id, "sogou_vr_11002301_box_")]/div/div[2]/p[2]'                                                                                       
mp_title_xpath= '//*[starts-with(@id, "sogou_vr_11002301_box_")]/div/div[2]/p[1]'


"""xpath module
~~~~~~~~~~~~~~~
for news list
"""
news_title_xpath = '//*[starts-with(@id, "sogou_vr_11002601_box_")]/div/h3'
news_intro_xpath = '//*[starts-with(@id, "sogou_vr_11002601_summary_")]'
news_time_xpath  = '//*[starts-with(@id, "sogou_vr_11002601_box_")]/div/div'
news_url_xpath   = '//*[starts-with(@id, "sogou_vr_11002601_title_")]'

