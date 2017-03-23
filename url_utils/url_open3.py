# coding: utf8
import requests
from proxies import get_mayi_proxy
from response import Response

_DEFAULT_UA = ('Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36'
        '(KHTML, like Gecko) Chrome/54.0.2840.87 Safari/537.37')

def url_open(url, use_proxy=0, timeout=30):
    session = requests.Session()
    session.max_redirects = 2
    resp = Response()
    resp.url = url
    try:
        headers = {'User-Agent': _DEFAULT_UA}
        headers['Accept'] = 'text/html'
        if use_proxy:
            mp = get_mayi_proxy(use_proxy)
            headers['Connection'] = 'close'
            headers['Proxy-Authorization'] = mp.header
            r = session.get(url=url, proxies=mp.proxies, headers=headers, 
                    timeout=timeout)
        else:
            headers['Connection'] = 'close'
            r = session.head(url=url, headers=headers, timeout=timeout)
            content_type = r.headers.get('content-type', '')
            if content_type and content_type.find('text/html') == -1:
                resp.status_code = Response.NOT_HTML
                return resp
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

