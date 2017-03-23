# -*- coding: utf-8 -*-
import sys
import socket
from download_image import *
from oss_utils import *
from mongodb_utils import *
from remove_header_snailer import *
from filter_emoji import *
from BeautifulSoup import BeautifulSoup
import requests
import logging
import logging.handlers
import re
import copy
import os
import time
import HTMLParser
import threading
sys.path.append("../")
from division_utils.ossManipulator import *
sys.setrecursionlimit(5000)

def getMsg_desc(soup, js_script):
    msg_desc = ""
    for js in js_script:
        if (js.string is None):
            continue
        matchGroup = re.findall (r'var msg_desc = "([^"]+)";', js.string)
        msg_desc_new = msg_desc
        for match in matchGroup:
            if match is not None and match != "":
                msg_desc_new = match
                break
        if len(msg_desc_new) > len(msg_desc):
            msg_desc = msg_desc_new
    title = soup.head.title.string
    js_content = soup.find('div', id="js_article").find('div', id='js_content')  #find article content
    #videos = js_content.findAll('iframe', {'class':'video_iframe'})
    return msg_desc, title, js_content

def getImgurl(img):
    img_url = ''
    if img.has_key('data-src'):
        img_url = img['data-src']
    if img.has_key('src'):
        img_url = img['src']
    return img_url

def getVideourl(video):
    video_src = ''
    if video.has_key('data-src'):
        video_src = video['data-src']
    elif video.has_key('src'):
        video_src = video['src']
    return video_src

def WeixinParser(content, pretty_content=None, news_image=None, osspush=None, updatetime=None):
    #content = RemoveHeader(content)
    #avoid eject section/p
    BeautifulSoup.NESTABLE_TAGS['p'] = ""
    BeautifulSoup.NESTABLE_TAGS['span'] = ""
    BeautifulSoup.NESTABLE_TAGS['section'] = ""

    soup = BeautifulSoup(content)
    pretty_soup = BeautifulSoup(pretty_content)

    js_script = soup.findAll('script', type="text/javascript")
    js_script_pretty = pretty_soup.findAll('script', type="text/javascript")

    msg_desc, title, js_content = getMsg_desc(soup, js_script)
    msg_desc_pretty, title_pretty, js_content_pretty = getMsg_desc(pretty_soup, js_script_pretty)

    videos = js_content.findAll('iframe', {'class':'video_iframe'})
    videos_pretty = js_content_pretty.findAll('iframe', {'class':'video_iframe'})
    #if videos is None or len(videos) <= 0:
    if not videos:
        videos = js_content.findAll('iframe')
    #if videos_pretty is None or len(videos_pretty) <= 0:
    if not videos_pretty:
        videos_pretty = js_content_pretty.findAll('iframe')

    if videos:
        for video in videos:
            tmp_video = copy.deepcopy(video)
            video_src = getVideourl(video)
            tmp_src = copy.deepcopy(video_src)
            if not video_src:
                continue
            if video_src.find("preview") <= 0 and video_src.find("player") <= 0:
                continue
            #"https://v.qq.com/iframe/preview.html?vid=u13045ac6x0&amp;width=500&amp;height=375&amp;auto=0"
            matchGroup = re.findall (r'(((width=[\d\.]+&height=[\d\.]+)|(height=[\d\.]+&height=[\d\.]+))&)', video_src)
    
            for match in matchGroup:
                if match is not None and match != "":
                    value = match[0]
                    video_src = video_src.replace(value, "")
                    break
    
            video_src = video_src.replace("preview", "player") #using player
            video_src = video_src.replace("https", "http") #using http
            video['src'] = video_src
            video['data-src'] = video_src
            #onLoad=\"iFrameHeight()\"
            video['onLoad'] = "iFrameHeight()"
            video['height'] = '375'
            video['width'] = '500'
            video['style'] = ' z-index:1; '
            for video_pretty in videos_pretty:
                video_pretty_src = getVideourl(video_pretty)
                #tmp_video_src = getVideourl(tmp_video)
                #print 'tmp_video_src', tmp_video_src
                if video_pretty_src == tmp_src:
                    video_pretty['src'] = video_src
                    video_pretty['data-src'] = video_src
                    video_pretty['onLoad'] = "iFrameHeight()"
                    video_pretty['height'] = '375'
                    video_pretty['width'] = '500'
                    video_pretty['style'] = ' z-index:1; '
                    break

    images = js_content.findAll('img')
    images_pretty = js_content_pretty.findAll('img')
    image_srcs = []
    image_head = ""
    #if len(images) == len(images_pretty):
    if news_image is not None:
        news_new_image = news_image.replace("mmbiz.qlogo.cn", "mmbiz.qpic.cn")
        localImgPath, size, type = getImageAndSizeFromUrl(news_new_image)
        types = str(type)
        if localImgPath is not  None:
            # 上传OSS
            if not osspush:
                img_src, oss_key = pushPicToOss(localImgPath, types, getBucket())
            else:
                if not updatetime:
                    updatetime = time.time()
                img_src, oss_key, ret = osspush.upload_pic_to_oss(updatetime, localImgPath, types)
                if not ret:
                    img_src, oss_key, ret = osspush.upload_pic_to_oss(updatetime, localImgPath, types)
            # 删除已上传的图片
            os.remove(localImgPath)
            image_head = [ img_src, [size, type] , oss_key, 0]

        #print  news_image, image_head

    for img in images:
        if (not img.has_key('data-src') and not img.has_key('src')):
            continue
        #获取图片链接
        tmp_img = copy.deepcopy(img)
        img_url = ''
        img_url = getImgurl(img)
        news_new_image = img_url.replace("mmbiz.qlogo.cn", "mmbiz.qpic.cn")
        localImgPath, size, type = getImageAndSizeFromUrl(news_new_image)
        types = str(type)
        if localImgPath is None:
            print "Download img", img_url, "failed..."
            continue
        #上传OSS
        if not osspush:
            img_src, oss_key = pushPicToOss(localImgPath, getBucket())
        else:
            if not updatetime:
                updatetime = time.time()
            img_src, oss_key, ret = osspush.upload_pic_to_oss(updatetime, localImgPath, types)
            if not ret:
                img_src, oss_key, ret = osspush.upload_pic_to_oss(updatetime, localImgPath, types)
        pic_size = os.path.getsize(localImgPath)
        #删除已上传的图片
        os.remove(localImgPath)

        img_feature= "max-width:600px;"

        if img.has_key("data-ratio"):
            img_ratio = float(img['data-ratio'])
        elif len(size) >= 2:
            width = size[0]
            height = size[1]
            if width and height and height > 0:
                img_ratio = float(width)/height
        if img.has_key('width'):
            img_width = img['width']
        else:
            img_width = ''
        if img.has_key('height'):
            img_height = img['height']
        else:
            img_height = ''



        #仅允许长宽比2:1或者1:2的作为news_image,且仅允许一张，且要求图片大小超过10k，且第一张符合要求的图片
        #print img_ratio, pic_size
        if img_ratio >= 0.5 and img_ratio <= 2 and pic_size >= 10240 and len(image_head) <= 0:
            image_head = [img_src, [size, type], oss_key, 0]

        #remove all keys
        keys = img.attrMap.keys()
        for key in keys:
            del(img[key])

        if len(size) >= 2:
            width = size[0]
            height = size[1]
            if width:
                img['width'] = width
            if height:
                img['height'] = height

        #add back new keys
        img['style']  = img_feature
        img['src'] = img_src
        img['ratio'] = img_ratio

        if type:
            img['type'] = str.lower(type)
        v = len(image_srcs)
        #所有图片
        image_srcs.append([img_src, [size, type], oss_key, v])
        #add click action here
        onclick="showImageFunc("+str(v)+")"
        img['onclick'] = onclick
        tmp_img_url = getImgurl(tmp_img)
        for pretty_img in images_pretty:
            pretty_img_url = getImgurl(pretty_img)
            if pretty_img_url == tmp_img_url:
                #print 'same ok'
                if img_ratio <= 0.2:
                    pretty_img['src']=''
                    continue
                if img['width']<=100 and img['height']<=100:
                    pretty_img['src']=''
                    continue
                pretty_img['data-bdtt-width'] = img['width']
                pretty_img['data-bdtt-height'] = img['height']
                pretty_img['width'] = img_width
                pretty_img['height'] = img_height
                pretty_img['style'] = img_feature
                pretty_img['src'] = img_src
                pretty_img['ratio'] = img_ratio
                pretty_img['type'] = img['type']
                pretty_img['onclick'] = onclick
    post_user = soup.find('a', id="post-user").string
    subcontent = str(js_content)
    subcontent_pretty = str(js_content_pretty)
    return filter_invalid_str(title), subcontent, subcontent_pretty, msg_desc, post_user, post_user , image_head, image_srcs

if __name__ == '__main__':

    #hostMongo = 'mongodb://bdtt:Chengzi123@121.43.56.137:27017/bdtt'
    reload(sys)
    sys.setdefaultencoding('utf-8')
    #R = requests.get('http://mp.weixin.qq.com/s?timestamp=1463086514&src=3&ver=1&signature=*6Piz1DJ154xfusEjfzlRksr9KwKwzyXkCLfLHOcw*nq4D12reprMLdHaUXoYq6KEHkWMsYk26id-M1307*DId7mfLMmFGoDAqFoksc63MKNgbuu7*0LDAQJizT6OIjJcNY3HK3O92yHrAJu39pa3w3QPzhwSa2A7HNh3pvvh6w=')
    #R=requests.get('http://mp.weixin.qq.com/s?__biz=MzAxODEyMjc6OA==&mid=2652540294&idx=1&sn=d4685f296552c11171ca71286c4784a6&scene=1&srcid=0411WgLAdDoLRlpPGH7vKyti&key=b28b03434249256b9d095c4e50fe3881febf9842e0af4ead7491237c5f8e045f21476697ffa65c03962e323f3afe5b68&ascene=0&uin=MTA4MjAzNDUwNw%3D%3D&devicetype=iMac+MacBook8%2C1+OSX+OSX+10.11.4+build(15E65)&version=11020201&pass_ticket=lFDprKggkZ1C08RSnFsjFP%2BCWkhUnkeNPGVVda%2BUJIeVW%2BQ6SuFiRBzISYvbZsw1')
    #R=requests.get('http://mp.weixin.qq.com/s?timestamp=1469676789&src=3&ver=1&signature=dZln-5Efzude257*KzrCEy-eCsrRvfDHQ6KwOm-gH-0seqVs0yfwNaCus9xvna6a01HPJQKHC6lmJTUMJYRDN7EzC5y0Xb-w3uYkaSJBBt8JjUkq-YWJ1bq3-Nu40PPZLULb7TO99rI4JSJEce4oXnFqVC9Abd2voBZ20oI6rzI=')
    R=requests.get('http://mp.weixin.qq.com/s?timestamp=1469699654&src=3&ver=1&signature=jnCBLR-YVUe94OO4T3YA7HRGVW3RnP-aCAGEfeXHl00AZZ977R-5zMVQWIxSHFd-XS6zeakIhgJYd9ub4NXwg04PmwYnSMoz0r0kPidH4czT3jR6abuecPztvIWMn0bRHJ8SjaDHpJVKNH54nzxbfW9M54YeJgjHsx7RyRupKbU=')
    #R=requests.get('http://business.sohu.com/20160412/n443977152.shtml', headers=Header)
    content = ""
    if R.status_code==200:
        content = R.content
    #print content

    osspush = OssManipulator()
    title, subcontent, msg_desc, post_user, post_user , image_head, image_srcs = WeixinParser(content, osspush=osspush)
    print subcontent
    print image_head
    print image_srcs
    F=open('weixin_out.html','w')
    F.write(subcontent)
    F.close()
