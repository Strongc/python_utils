# coding = utf-8
import requests
import time
filepath = u'image/'

def saveImage(content):
    fname = unicode(time.time()).replace(u'.',u'')
    
    with open(filepath + fname + u'.png', u'wb') as f:
        f.write(content)

def downloadImage(url):
    r = requests.get(url)
    content = r.content
    saveImage(content)

def main():
    url_base = u'http://mp.weixin.qq.com/mp/verifycode?cert='

    for i in xrange(1,1000):
        print i
        random_num = unicode(time.time()).replace(u'.',u'')
        url = url_base + random_num + u'.1738'
        downloadImage(url)

if __name__ == u'__main__':
    main()
    #downloadImage(url)
