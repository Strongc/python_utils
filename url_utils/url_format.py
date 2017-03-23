# -*- coding: utf-8 -*-

import sys
import re
import urlparse
import urllib

_tlds = set(['com', 'org', 'net', 'int', 'edu', 'gov', 'mil', 'arpa'])

_cctlds = set(['ac', 'ad', 'ae', 'af', 'ag', 'ai', 'al', 'am', 'an', 'ao', 'aq', 'ar', 'as', 'at', 'au', 'aw', 'ax', 'az', 'ba', 'bb', 'bd', 'be', 'bf', 'bg', 'bh', 'bi', 'bj', 'bm', 'bn', 'bo', 'bq', 'br', 'bs', 'bt', 'bv', 'no', 'bw', 'by', 'bz', 'ca', 'cc', 'cd', 'cf', 'cg', 'ch', 'ci', 'ck', 'cl', 'cm', 'cn', 'co', 'cr', 'cu', 'cv', 'cw', 'cx', 'cy', 'cz', 'de', 'dj', 'dk', 'dm', 'do', 'dz', 'ec', 'ee', 'eg', 'eh', 'er', 'es', 'et', 'eu', 'eus', 'fi', 'fj', 'fk', 'fm', 'fo', 'fr', 'ga', 'gb', 'gd', 'ge', 'gf', 'gg', 'gh', 'gi', 'gl', 'gal', 'gm', 'gn', 'gp', 'gq', 'gr', 'gs', 'gt', 'gu', 'gw', 'gy', 'hk', 'hm', 'hn', 'hr', 'ht', 'hu', 'id', 'ie', 'il', 'im', 'in', 'io', 'iq', 'ir', 'is', 'it', 'je', 'jm', 'jo', 'jp', 'ke', 'kg', 'kh', 'ki', 'km', 'kn', 'kp', 'kr', 'kw', 'ky', 'kz', 'la', 'lb', 'lc', 'li', 'lk', 'lr', 'ls', 'lt', 'lu', 'lv', 'ly', 'ma', 'mc', 'md', 'me', 'mg', 'mh', 'mk', 'ml', 'mm', 'mn', 'mn', 'mo', 'mp', 'mq', 'mr', 'ms', 'mt', 'mu', 'mv', 'mw', 'mx', 'my', 'mz', 'na', 'nc', 'ne', 'nf', 'ng', 'ni', 'nl', 'no', 'np', 'nr', 'nu', 'nz', 'om', 'pa', 'pe', 'pf', 'pg', 'ph', 'pk', 'pl', 'pm', 'pn', 'pr', 'ps', 'pt', 'pw', 'py', 'qa', 're', 'ro', 'rs', 'ru', 'su', 'rw', 'sa', 'sb', 'sc', 'sd', 'se', 'sg', 'sh', 'si', 'sj', 'no', 'sk', 'sl', 'sm', 'sn', 'sr', 'ss', 'st', 'su', 'sv', 'sx', 'sy', 'sz', 'tc', 'td', 'tf', 'tg', 'th', 'tj', 'tk', 'tl', 'tp', 'tm', 'tn', 'to', 'tp', 'tl', 'tr', 'tt', 'tv', 'tw', 'tz', 'ua', 'ug', 'uk', 'us', 'gov', 'uy', 'uz', 'va', 'vc', 've', 'vg', 'vi', 'vn', 'vu', 'wf', 'ws', 'ye', 'yt', 'za', 'zm', 'zw', 'dz', 'am', 'bd', 'by', 'bg', 'cn', 'cn', 'eg', 'eu', 'ge', 'gr', 'hk', 'in', 'in', 'in', 'in', 'in', 'in', 'in', 'in', 'in', 'in', 'in', 'in', 'in', 'in', 'in', 'ir', 'iq', 'jo', 'kz', 'mo', 'mo', 'mk', 'my', 'mn', 'ma', 'om', 'pk', 'ps', 'qa', 'ru', 'sa', 'rs', 'sg', 'sg', 'kr', 'lk', 'lk', 'sd', 'sy', 'tw', 'tw', 'th', 'tn', 'ua', 'ae', 'ye', 'academy', 'accountant', 'adult', 'aero', 'agency', 'apartments', 'archi', 'associates', 'audio', 'bar', 'bargains', 'best', 'bike', 'biz', 'black', 'blackfriday', 'blog', 'blue', 'builders', 'cam', 'camera', 'camp', 'cancerresearch', 'cards', 'cars', 'center', 'cheap', 'christmas', 'church', 'click', 'clothing', 'club', 'codes', 'coffee', 'college', 'coop', 'dance', 'date', 'dating', 'design', 'diet', 'directory', 'download', 'education', 'email', 'events', 'exposed', 'faith', 'farm', 'fit', 'flowers', 'gift', 'glass', 'global', 'gop', 'green', 'guitars', 'guru', 'help', 'hiphop', 'hiv', 'holdings', 'host', 'hosting', 'house', 'info', 'ink', 'international', 'jobs', 'kim', 'land', 'lighting', 'link', 'loan', 'lol', 'love', 'meet', 'men', 'menu', 'mobi', 'moe', 'mov', 'museum', 'name', 'ngo', 'ninja', 'ong', 'onl', 'ooo', 'organic', 'pharmacy', 'photo', 'photos', 'pics', 'pink', 'plumbing', 'porn', 'post', 'pro', 'properties', 'property', 'red', 'rich', 'science', 'sex', 'sexy', 'singles', 'social', 'solar', 'sucks', 'systems', 'tattoo', 'tel', 'today', 'top', 'travel', 'video', 'voting', 'wiki', 'work', 'wtf', 'xxx', 'XYZ', 'futbol', 'juegos', 'uno', 'kaufen', 'desi', 'shiksha', 'moda', 'bar', 'coop', 'pharmacy', 'pub', 'realtor', 'ventures', 'asia', 'krd', 'tokyo', 'alsace', 'berlin', 'brussels', 'bzh', 'cat', 'cymru', 'eus', 'frl', 'gal', 'gent', 'irish', 'istanbul', 'istanbul', 'london', 'paris', 'saarland', 'scot', 'vlaanderen', 'wales', 'wien', 'miami', 'nyc', 'quebec', 'kiwi', 'melbourne', 'sydney', 'lat', 'ru', 'bar', 'bible', 'biz', 'church', 'club', 'college', 'com', 'design', 'download', 'green', 'hiv', 'info', 'ist', 'kaufen', 'kiwi', 'lat', 'moe', 'name', 'net', 'ninja', 'OOO', 'org', 'pro', 'wiki', 'xyz', 'aero', 'asia', 'cat', 'eus', 'coop', 'edu', 'gov', 'int', 'jobs', 'mil', 'mobi', 'museum', 'post', 'tel', 'tokyo', 'travel', 'xxx', 'alsace', 'berlin', 'brussels', 'bzh', 'cymru', 'frl', 'gal', 'gent', 'irish', 'istanbul', 'kiwi', 'krd', 'miami', 'nyc', 'paris', 'quebec', 'saarland', 'scot', 'vlaanderen', 'wales', 'wien', 'arpa', 'nato', 'example', 'invalid', 'local', 'localhost', 'onion', 'test', 'africa', 'bcn', 'lat', 'eng', 'sic', 'geo', 'mail', 'web', 'shop', 'art', 'eco', 'kid', 'kids', 'music'])

_cnslds = set(['ah', 'bj', 'cq', 'fj', 'gd', 'gs', 'gz', 'gx', 'ha', 'hb', 'he', 'hi', 'hk', 'hl', 'hn', 'jl', 'js', 'jx', 'ln', 'mo', 'nm', 'nx', 'qh', 'sc', 'sd', 'sh', 'sn', 'sx', 'tj', 'tw', 'xj', 'xz', 'yn', 'zj'])


def get_domain(url):
    ns = urlparse.urlparse(url).netloc.split('.')
    if len(ns) <= 2:
        return '.'.join(ns)
    if ns[-2] in _cctlds or ns[-2] in _tlds or ns[-2] in _cnslds:
        return '.'.join(ns[-3:])
    else:
        return '.'.join(ns[-2:])

def get_top_domain(url):
    netloc = urlparse.urlparse(url).netloc
    a = netloc.split('.')
    if a[0] == 'www':
        return '.'.join(netloc.split('.')[1:])
    else:
        return netloc

def get_doc_type(url):
    path = urlparse.urlparse(url).path
    t = path.split('/')[-1].split('.')
    return t[-1].lower() if len(t) > 1 else ''

def format_url(url):
    url = url.strip('/')
    return url
