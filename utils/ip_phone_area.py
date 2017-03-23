# coding=UTF-8

# get provider information by phoneNumber

from urllib import urlopen
import re
import time

import json
# get html source code for url
def getPageCode(url):
    file = urlopen(url)
    text = file.read()
    file.close()
#   text = text.decode("utf-8")     # depending on coding of source code responded
    return text

# parse html source code to get provider information
def parseString(src, result):
    pat = []
    pat.append('(?<=归属地：</span>).+(?=<br />)')
    pat.append('(?<=卡类型：</span>).+(?=<br />)')
    pat.append('(?<=运营商：</span>).+(?=<br />)')
    pat.append('(?<=区号：</span>)\d+(?=<br />)')
    pat.append('(?<=邮编：</span>)\d+(?=<br />)')

    item = []
    for i in range(len(pat)):
        m = re.search(pat[i], src)
        if m:
            v = m.group(0)
            item.append(v)
    return item

def parseIPString(src, result):
    pat = []

    #Get IP Address
    # ip = re.findall(r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}',src)
    #   print "IP Address:",ip[0]
    #   #Get IP Address Location
    #   result = re.findall(r'(<li>.*?</li>)',s)
    #   for i in result:
    #     print i[4:-5]
    #   print "*"*45
    #   print "\n"
    src_u = src.decode("gb2312").encode("utf-8")
    pat.append('(?<=本站主数据：)([^ ]+) ')
    item = []
    for i in range(len(pat)):
        m = re.search(pat[i], src_u)
        if m:
            v = m.group(0)
            item.append(v)
    return item

# get provider by phoneNum
def getProvider(phoneNum, result):
    url = "http://www.sjgsd.com/n/?q=%s" %phoneNum
    print url
    text = getPageCode(url)
    item = parseString(text, result)
    result.append((phoneNum, item))
    return item


# get provider by phoneNum
def getIPProvider(ip, result):
    url = "http://www.ip138.com/ips138.asp?ip=%s&action=2" % ip
    print url
    text = getPageCode(url)

    item = parseIPString(text, result)
    result.append((ip, item))
    print str(item[0])
    return item
# write result to file
def writeResult(result):
    f = open("result.log", "w")
    for num, item in result:
        f.write("%s:\t" %num)
        for i in item:
            f.write("%s,\t" %i)
        f.write("\n")
    f.close()

if __name__ == "__main__":
    result = []
    #341376:[Tue, 22 Mar 16 13:27:16 +0800][DEBUG] register, userName:13462649334, nickName:建议, psd:txf1996, regionId:102, uuid:864394109016214, ip:60.168.174.21

    for line in open("/Users/guxiangyu/Documents/git/scrapy-multi-sources/utils/registered_user", "r"):
        line = line.strip(" \t\r\n")
        prefix = line.find("userName:")
        phoneNum = None
        phone_area = None
        if prefix and prefix > 0:
            phoneNum = line[prefix+len("userName:"):]
        if phoneNum:
            phone_area = getProvider(phoneNum, result)
            print phone_area[0]
            #print("%s is finished" %phoneNum)
        prefix = line.find("ip:")
        ip_area = None
        if prefix and prefix > 0:
            ip = line[prefix+len("ip:"):]
            if ip:
                ip = getIPProvider(ip,result)
                print ip
        time.sleep(1)
    #writeResult(result)