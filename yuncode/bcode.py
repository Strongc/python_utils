# -*- coding: cp936 -*-

import requests
import re
import time
import yunCode

headers = {
            u'User-Agent':u'Mozilla/5.0 (Windows NT 6.1; WOW64) App\
            leWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.87 Safari/537.36'
            }

model = re.compile(u'<title>请输入验证码\s*</title>')

def saveImage(filename, content):
    filename = unicode(time.time()).replace(u'.', u'')
    #filename = filename + u'.png'
    with open(filename, u'wb') as f:
        f.write(content)
    return filename


class CodeAna(object):
    
    def makeSession(self):
        s = requests.session()
        self.session = s

    def makecodeUrl(self, t):
        url = u'http://mp.weixin.qq.com/mp/verifycode?cert=%s' % t
        return url

    def makecodePostParas(self, t, code):
        url = u'http://mp.weixin.qq.com/mp/verifycode'
        paras = {u'cert': unicode(t),
                 u'input': code}
        return url, paras
        

    def postdata(self, url, paras, headers = None):
        if headers is None:
            r = self.session.post(url, data=paras)
        else:
            r = self.session.post(url, data=paras, headers=headers)
        return r

    def getdata(self, url, headers = None):
        if headers is None:
            r = self.session.get(url)
        else:
            r = self.session.get(url, headers=headers)
        return r
    
        r = self.session.get(url)
        return r

    def analysisCode(self, url):
        self.makeSession()
        
        #url = u'http://mp.weixin.qq.com/profile?src=3&timestamp=1471414472&ver=1&signature=2SIYL6prekN-oPlIyvzOmycgrnCFc7hUBl2NGjGrxd9VSJOI7SWhS3vikHwiyw*VhGPPsV8p*ujID16nSfFg0Q=='
        r = self.getdata(url)
        content =  r.text
        #print content
        is_code = model.findall(content)
        if is_code:
            t = time.time()
            headers[u'Referer'] = url
            codeurl = self.makecodeUrl(t)
            r = self.getdata(codeurl)
            imageName = saveImage(unicode(t), r.content)
            print imageName
            time.sleep(3)
            code_input = yunCode.yunDamaFunction(imageName)
            print u'code_input:', code_input
            #code_input = raw_input(u'code:')

            posturl,paras = self.makecodePostParas(t, code_input)
            r = self.postdata(posturl, paras)
            print r.content
        else:
            print u'yes'
            #print content
            
            
            
            
            
 
if __name__ == u'__main__':
    code = CodeAna()
    code.analysisCode()
