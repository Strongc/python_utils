# coding: utf8
""" open url
"""
import logging
import urlparse
import socket
import time
from httplib import HTTPResponse
from cStringIO import StringIO
from response import Response

_DEFAULT_HEADERS = (
        ('Accept', '*/*'),
        ('User-Agent', ('Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36'
                '(KHTML, like Gecko) Chrome/54.0.2840.87 Safari/537.36')),
        ('Connection', 'close')
        )

class _FakeSocket():
    def __init__(self, data):
        self._file = StringIO(data)
    def makefile(self, *args, **kwargs):
        return self._file

def url_open(url, headers=_DEFAULT_HEADERS, connect_timeout=5, read_timeout=4, 
        full_timeout=10, logger=logging.getLogger(__name__)):
    """ open url
    """
    resp = Response()

    t = urlparse.urlparse(url)
    path = t.path or '/'
    if len(t.netloc.split(':')) == 2:
        host, port = t.netloc.split(':')
    else:
        host, port = t.netloc, 80

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(connect_timeout)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    try:
        s.connect((host, port))
        s.send("GET %s HTTP/1.1\r\n" % path)
        s.send("Host:%s\r\n" % host)
        for k,v in headers:
            s.send("%s:%s\r\n" % (k, v))
        s.send('\r\n')
    except socket.timeout as e:
        logger.exception('connection timeout: [%s]' % str(e))
        resp.status_code = Response.CONNECTION_TIMEOUT
        return resp
    except socket.error as e:
        logger.exception('socket error while send: [%s]' % str(e))
        resp.status_code = Response.SOCKET_ERROR
        return resp

    data = []
    begin = time.time()
    s.settimeout(read_timeout)
    while 1:
        if time.time() - begin > full_timeout:
            logger.exception('full timeout')
            resp.status_code = Response.FULL_TIMEOUT
            break
        try:
            msg = s.recv(4096)
            if len(msg) == 0:
                break
            data.append(msg)
        except socket.timeout as e:
            logger.exception('read timeout: [%s]' % str(e))
            resp.status_code = Response.READ_TIMEOUT
            break
        except socket.error as e:
            logger.exception('socket error while read: [%s]' % str(e))
            resp.status_code = Response.SOCKET_ERROR
            break
    try:
        s.shutdown(1)
        s.close()
    except socket.error as e:
        logger.exception('socket error while close: [%s]' % str(e))

    resp.content = ''.join(data)
    if resp.status_code != 200:
        return resp
    
    try:
        http_resp = HTTPResponse(_FakeSocket(resp.content))
        http_resp.begin()
    except Exception as e:
        logger.exception('read http response error: [%s]' % str(e))
        resp.status_code = Response.HTTP_RESPONSE_ERROR
        return resp
    
    resp.status_code = http_resp.status
    resp.content = http_resp.read(len(resp.content))
    location = http_resp.getheader('location', '')
    if location and str(http_resp.status)[0] == '3':
        redirect_url = urlparse.urljoin(url, location)
        logger.info('redirect to: [%s]' % redirect_url)
        new_full_timeout = full_timeout - (time.time() - begin)
        return url_open(url=redirect_url, headers=headers, 
                connect_timeout=connect_timeout, read_timeout=read_timeout, 
                full_timeout=new_full_timeout, logger=logger)
    return resp

