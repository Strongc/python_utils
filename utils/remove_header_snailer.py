# -*- coding: utf-8 -*-
from BeautifulSoup import BeautifulSoup
import os
import sys
import re
sys.path.append('/Users/guxiangyu/Documents/wxspider/spider/utils/')
from download_image import *
from oss_utils import *
from mongodb_utils import *

import time

def RemoveHeader(content):
    try:
        #fo = file(html, "r")
        #content = ""
        #for line in fo.readlines():
        #    content += line
        BeautifulSoup.NESTABLE_TAGS['p'] = ""
        BeautifulSoup.NESTABLE_TAGS['span'] = ""
        BeautifulSoup.NESTABLE_TAGS['section'] = ""
        soup_c = BeautifulSoup(content)
        js_content = soup_c.find('div', id='js_content')  #find article content
        #TDO: section?
        ps = [] #js_content.findAll('section')
        pss = js_content.findAll('p')
        ps += pss
        #print ps
        click_word = ["点击", "看我", "关注", "订阅"]
        for id in range(0, len(ps)):
            if (id <= 1):
                img = ps[id].find('img')
                #过滤前3段中,图片且比率小于0.2的图片,最有可能是广告
                if img is not None and img.has_key('data-ratio') and  float(img['data-ratio']) <= 0.25:
                    print "header image filtered by image ratio", ps[id]
                    ps[id].clear()
                    continue
                bFilter = False
                for word in click_word:
                    if str(img).find(word) >= 0:
                        print "header image filtered by click word", ps[id]
                        bFilter = True
                        break
                if bFilter:
                    ps[id].clear()
                    continue
                try:
                    if img.has_key('data-w') and img['data-w'] != "" and int(img['data-w']) <= 50:
                        print "header image filtered by image width", ps[id]
                        ps[id].clear()
                        continue
                except:
                    continue
            else:
                break
        #去除最后两段中可能的图片:由于二维码是方形的,不好确定比率
        for id in range(len(ps)-2, len(ps)):
            img = ps[id].find('img')
            #clear last image
            if img is not None:
                print "snail image filtered:", ps[id]
                ps[id].clear()

        return str(soup_c)
    except Exception, ex:
        print ex
        return content
def removeHeader(col1, col2):
    data = col1.find({"is_handwork_news":2, "_id":ObjectId("56d027062a06b50ba2c60525")})
    for item in data:
        content = item[u'news_desc']
        content_new = RemoveHeader(content)
        item[u'news_desc'] = content_new
        SaveMongoItem(col2, item)

if __name__ == '__main__':
    time = time.strftime("%Y%m%d", time.localtime(time.time()))


    hostMongo = 'mongodb://bdtt:Chengzi123@121.43.56.137:27017/bdtt'
    db = ConnectMongo(hostMongo, "bdtt")
    collection = GetCollection(db, u'news')
    print collection.find({}).count()

    insert_col = GetCollection(db, u'news_test')
    print insert_col.find({}).count()
    try:
        removeHeader(collection, insert_col)
    except Exception, ex:
        print("trans data error... %s"%ex)


