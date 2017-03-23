# -*- coding: utf-8 -*-
import urllib
import os
import urllib2
import socket
socket.setdefaulttimeout(60.0)
from os.path import getsize
import time
import random
from PIL import Image
import requests
import urllib

#temporarily disable insecure HTTPS request warnings
#FIX ME
import requests
from requests.packages.urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

from StringIO import StringIO
from retrying import retry
#通过url获取图片，下载到本地
# input param: addr 图片的url链接
# return     : 本地文件名
@retry(stop_max_attempt_number=3)
def getImageFromUrl( addr ):
    addr = urllib.unquote(addr)
    #print u'addr',addr
    size = 0
    retry = 0
    headers = {
        #'User-Agent':"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/47.0.2526.80 Safari/537.36"
        'User-Agent':u'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/47.0.2526.73 Safari/537.36'
    }

    while retry <= 2:
        try:
            if addr.find("mmbiz") > 0:
                r = requests.get(url=addr,headers=headers,allow_redirects=True,timeout=10,verify=False)
            else:
                r = requests.get(url=addr,headers=headers,allow_redirects=True,timeout=10)
        except Exception,error:
            print error
            continue
            #return None
        if r.status_code == 403:
            return None
        if u'content-length' not in r.headers:
            return None #####
        if r.headers[u'content-length'] > 120:
            #print r.encoding
            #r.encoding = u'utf-8'
            data = r.content
            splitPath = addr.split('/')
            fName = str(int(time.time())) + "_" + str(random.randint(0, 10000)) + splitPath.pop()
            fName = fName.replace(u'?',u'').strip()# + u'.' + addr.split(u'.')[-1]

            f = open(fName, 'wb')
            f.write(data)
            f.close()
            return fName
        else:
            retry += 1
    return None


##返回图片尺寸
##path, [width, height], type
def getImageAndSizeFromUrl(addr):
    try:
        filePath = getImageFromUrl(addr)
        if filePath is None:
            return None, None, None
        img = Image.open(filePath)
        imgSize = img.size
        imformat = str.lower(img.format)

        if not filePath.endswith("."+imformat):
            filePathNew = filePath + "."+imformat
            os.rename(filePath, filePathNew)
            return filePathNew, imgSize, imformat
        return filePath, imgSize, imformat
    except Exception, ex:
        print "error", ex
        return None, None, None


if __name__ == '__main__':
    path,width,height = getImageAndSizeFromUrl("http://www.dqjy.cn/sites/main/UploadFiles/image/2016/05/18/20160518085137_8167.jpg")
    path,width,height = getImageAndSizeFromUrl('https://mmbiz.qpic.cn/mmbiz/OicchGF4Xe1OB05h1Gto5lgGvuw9Wg0VaFaolDvDebBkvNnLbRHicHc7T5lbKEXjp2h90Q6xdUpia7mib4JBPDczaA/0?wx_fmt=png')
    #path,width,height = getImageAndSizeFromUrl("http://mmbiz.qpic.cn/mmbiz/VSjQsfiaBAaauR4ibiabiaPWib2S0kS1PIJrx1ukafYSWXsUkwn381CecYg7jRMYWFBO1jNiclylGnyomriaqeY7ibWeqQ/0?")
    print path,width,height

#根据url获取content
#value: post params
#user-agent: ua
def getContentFromUrl(url, values=None,user_agent=None):
    #print u'@getContentFromUrl:',url
    size = 0
    retry = 0
    headers = {}
    if user_agent is not None:
        headers = { 'User-Agent' : user_agent }

    while retry <= 2:
        r = requests.get(url=url,headers=headers,timeout=20) if len(headers) else requests.get(url=url,timeout=20)

        if r.encoding.lower() not in u'utf-8':
            #print r.encoding
            if r.encoding.lower() == u'gbk':
                continue
            r.encoding = u'utf-8'

        content = r.text


        if len(content) > 100:
            return content
        else:
            retry += 1
    return None


#根据url获取content
#value: post params
#user-agent: ua
def getContentFromUrlWithRedirect(url, values=None,  user_agent=None):
    size = 0
    retry = 0
    headers = {}
    if user_agent is not None:
        headers = { 'User-Agent' : user_agent }

    if values is None:
        values = {}
    data = urllib.urlencode(values)

    while retry <= 2:
        if len(headers):
            req = urllib2.Request(url,headers=headers)
        else:
            req = urllib2.Request(url)
        response = urllib2.urlopen(req)
        print response.geturl()

        data = response.read()
        print data
        if (len(data) >= 100):
            if data.find("charset=gb2312") > 0 :
                content = data.decode('gb2312').encode('utf-8')
            else:
                content = data.encode('utf-8')
            return content
        retry += 1

    return None
