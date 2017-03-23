# -*- coding: utf-8 -*-

import os, sys
import urllib
import socket
socket.setdefaulttimeout(60.0)
from cStringIO import StringIO
from PIL import Image
import requests
import urllib

def get_size(content):
    try:
        img = Image.open(StringIO(content))
        return img.size
    except Exception as e:
        return None

def get_size_format(content):
    try:
        img = Image.open(StringIO(content))
        return (img.size, img.format)
    except Exception as e:
        return None

def open_image(url):
    url = urllib.unquote(url)
    headers = {
        'User-Agent':u'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/47.0.2526.73 Safari/537.36'
    }
    for i in range(2):
        try:
            r = requests.get(url=url, headers=headers, allow_redirects=True, timeout=10)
        except Exception as e:
            continue
        if r.headers.get('content-length', 0) <= 120:
            return None
        return r.content
    return None

#c = open_image(sys.argv[1])
#print get_size_format(c)
