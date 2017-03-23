# coding: utf8

class Response(object):
    """ local defined code in addtion to http status code
    """
    CONNECTION_ERROR = 7001
    CONNECT_TIMEOUT = 7002
    PROXY_ERROR = 7004
    SSL_ERROR = 7005
    TIMEOUT = 7006
    NOT_HTML = 7010
    CHARSET_UNKNOWN = 7100
    ERROR = 7777

    __slots__ = ('status_code', 'explain', 'url', 'content')

    def __init__(self, status_code=200, content=''):
        self.status_code = status_code
        self.explain = ''
        self.url = ''
        self.content = content

