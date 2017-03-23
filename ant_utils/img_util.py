# -*- coding: utf-8 -*-
import logging
import os, sys
import urllib
import urlparse
import socket
import time
from httplib import HTTPResponse
from cStringIO import StringIO
from PIL import Image
import requests

class FakeSocket():
    def __init__(self, data):
        self._file = StringIO(data)
    def makefile(self, *args, **kwargs):
        return self._file

def open_image2(url, buff_size=4096, connect_timeout=5, read_timeout=4, 
        full_read_timeout=30, logger=logging):
    t = urlparse.urlparse(url)
    path = t.path or '/'
    if len(t.netloc.split(':')) == 2:
        HOST, PORT = t.netloc.split(':')
    else:
        HOST, PORT = t.netloc, 80
    UA = ('Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36'
            ' (KHTML, like Gecko) Chrome/54.0.2840.87 Safari/537.36')
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(connect_timeout)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    try:
        s.connect((HOST, PORT))
    except Exception as e:
        logger.exception(e)
        return None
    s.send("GET %s HTTP/1.1\r\n" % path)
    s.send("Host:%s\r\n" % HOST)
    s.send("Accept:text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8\r\n")
    s.send("User-Agent:%s\r\n" % UA)
    s.send('\r\n')
    data = []
    begin = time.time()
    while 1:
        if time.time() - begin > full_read_timeout:
            logger.error('reach full_read_timeout')
            data = []
            break
        try:
            msg = s.recv(buff_size)
            #print len(msg), len(data), time.time() - begin
            if len(msg) == 0:
                break
            data.append(msg)
        except socket.timeout as e:
            if e.args[0] == 'timed out':
                break
            else:
                logger.error('open image [%s] socket timeout: %s' % (url, str(e)))
                data = []
                break
        except socket.error as e:
            logger.error('open image [%s] socket error: %s' % (url, str(e)))
            data = []
            break
    s.shutdown(1)
    s.close()
    
    data = ''.join(data)
    if data:
        resp = HTTPResponse(FakeSocket(data))
        resp.begin()
        if resp.status == 200:
            return resp.read(len(data))
    return None

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
    for i in range(1):
        try:
            r = requests.get(url=url, headers=headers, allow_redirects=True, timeout=6)
        except Exception as e:
            continue
        if r.headers.get('content-length', 0) <= 120:
            return None
        return r.content
    return None

#c = open_image(sys.argv[1])
#print get_size_format(c)
