# -*-coding:utf-8-*-*
import os
import re
import sys
import fileinput
from urllib import urlencode
def filter_invalid_str(text):
    """
	过滤非BMP字符
    """
    reload(sys)
    sys.setdefaultencoding("utf8")
    value = text
    if not isinstance(text, unicode):
        value = text.decode("utf8")
    try:
        # UCS-4
        highpoints = re.compile(u'[\U00010000-\U0010ffff]')
    except re.error:
        # UCS-2
        highpoints = re.compile(u'[\uD800-\uDBFF][\uDC00-\uDFFF]')
    value =  highpoints.sub(u'', value)
    if not isinstance(text, unicode):
        return value.encode('utf8')
    else:
        return value

if __name__ == '__main__':
    file = 'test_emoji_data'
    fp = open(file,"r")
    print "code",sys.maxunicode
    for line in fp.readlines():
        line = line.strip()
        print filter_invalid_str(line)
    #print filter_emoji(strv)


