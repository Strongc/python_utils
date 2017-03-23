# -*- coding: utf-8 -*-

from BeautifulSoup import BeautifulSoup
import os
import sys
import re
sys.path.append('/Users/guxiangyu/Documents/wxspider/spider/utils/')
from download_image import *
from oss_utils import *
def WeixinParser(content):
    BeautifulSoup.NESTABLE_TAGS['p'] = ""
    BeautifulSoup.NESTABLE_TAGS['span'] = ""
    BeautifulSoup.NESTABLE_TAGS['section'] = ""
    soup = BeautifulSoup(content)
    js_script = soup.findAll('script', type="text/javascript")
    desc = ""
    for js in js_script:
        if (js.string is None):
            continue
        matchGroup = re.findall (r'var msg_desc = "([^"]+)";', js.string)
        for match in matchGroup:
            desc = match

    title = soup.head.title.string
    js_content = soup.find('div', id="js_article").find('div', id='js_content')  #find article content
    images = js_content.findAll('img')
    image_srcs = []
    for img in images:
        img_url =  img['data-src']


        localImgPath = getImageFromUrl(img_url)
        img_src, oss_key = pushPicToOss(localImgPath, getBucket())
        img_feature= "max-width:600px;"
        keys = img.attrMap.keys()
        for key in keys:
            del(img[key])
        img['src'] = img_src
        img['style']  = img_feature
        image_srcs.append(img_src)


    post_user = soup.find('a', id="post-user").string
    #print post_user
    #pinyin = js_content.find('span', id="profile_meta_value").string

    #news_title, news_intro, allimages, news_paper_name, typeName,

    return title, str(js_content), image_srcs, post_user, post_user

if __name__ == '__main__':
    html = "./weixin.html"
    if os.path.isfile(html):
        fileHandler = open (html)
        fileList = fileHandler.readlines()
        content = ""
        for fileLine in fileList:
            content += fileLine
        (title, content2, img, post, post_u) =  WeixinParser(content)
	print content2
        #print images
        #soup = BeautifulSoup(content)
        #print soup.prettify()
        #print soup.head.title.string
        #print soup.body.contents[3]
