# coding: utf8
import requests
from response import Response

_DEFAULT_UA = ('Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36'
        '(KHTML, like Gecko) Chrome/54.0.2840.87 Safari/537.37')
_DEFAULT_ACCEPT = ('Accept:text/html,application/xhtml+xml,'
        'application/xml;q=0.9,image/webp,*/*;q=0.8')

def image_open(url, timeout=30):
    session = requests.Session()
    session.max_redirects = 2
    resp = Response()
    resp.url = url
    try:
        headers = {'User-Agent': _DEFAULT_UA, 'Accept': _DEFAULT_ACCEPT}
        headers['Connection'] = 'close'
        r = session.head(url=url, headers=headers, timeout=timeout)
        content_type = r.headers.get('content-type', '')
        r = session.get(url=url, headers=headers, timeout=timeout)
        resp.status_code = r.status_code
        resp.url = r.url
        for chunk in r.iter_content(chunk_size=40960):
            resp.content += chunk
    except requests.exceptions.ConnectionError as e:
        resp.status_code = Response.CONNECTION_ERROR
        resp.explain = str(e)
    except requests.exceptions.ProxyError as e:
        resp.status_code = Response.PROXY_ERROR
        resp.explain = str(e)
    except requests.exceptions.SSLError as e:
        resp.status_code = Response.SSL_ERROR
        resp.explain = str(e)
    except requests.exceptions.Timeout as e:
        resp.status_code = Response.TIMEOUT
        resp.explain = str(e)
    except Exception as e:
        resp.status_code = Response.ERROR
        resp.explain = str(e)
    return resp

