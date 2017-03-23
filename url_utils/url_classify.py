# -*- coding: utf-8 -*-

import sys
import re
import urlparse
import urllib
import datetime

_cls_reos = []
_reo_bbs = re.compile(r'\W(bbs|forum|blog|tieba)', re.I)
_cls_reos.append((_reo_bbs, 'bbs'))
_reo_sport = re.compile(r'\W(sport|sports|tiyu|zuqiu|football)\W', re.I)
_cls_reos.append((_reo_sport, 'sport'))
_reo_ent = re.compile(r'\W(ent|entertainment|yule|star|mingxing|bagua)\W', re.I)
_cls_reos.append((_reo_ent, 'ent'))
_reo_live = re.compile(r'\W(jiaju|live)\W', re.I)
_cls_reos.append((_reo_ent, 'live'))
_reo_fen = re.compile(r'\W(58|ganji|baixing|fenlei|bendibao|xinxi|bianmin|114hy)\W', re.I)
_cls_reos.append((_reo_fen, 'fen'))
_reo_job = re.compile(r'\W(hr|zhaoping|renshi|job|zhaopins)\W', re.I)
_cls_reos.append((_reo_job, 'job'))
_reo_house = re.compile(r'\W(fang|dichan|house|fangdichan|loushi|fangwu|fangchan|xinloupan)\W', re.I)
_cls_reos.append((_reo_house, 'house'))
_reo_buy = re.compile(r'\W(buy|ershou|2shou|mall|shop|shoping|shopping|taobao|1688|chushou|chuzu)\W', re.I)
_cls_reos.append((_reo_buy, 'buy'))
_reo_car = re.compile(r'\W(car|cars|auto|qiche|che|vehical|cheliang)\W', re.I)
_cls_reos.append((_reo_car, 'car'))
_reo_health = re.compile(r'\W(health|jiankang|yangsheng|yiyao)\W', re.I)
_cls_reos.append((_reo_health, 'health'))
_reo_baby = re.compile(r'\W(baby|byby|baobao)\W', re.I)
_cls_reos.append((_reo_baby, 'baby'))
_reo_yy = re.compile(r'\W(yiyuan|hospital)', re.I)
_cls_reos.append((_reo_yy, 'yy'))
_reo_fashion = re.compile(r'\W(fashion|shishang)\W', re.I)
_cls_reos.append((_reo_fashion, 'fashion'))
_reo_digital = re.compile(r'\W(digi|shuma|it|digital)\W', re.I)
_cls_reos.append((_reo_digital, 'digital'))
_reo_vedio = re.compile(r'\W(v|vedio|shipin|video)\W', re.I)
_cls_reos.append((_reo_vedio, 'vedio'))
_reo_stock = re.compile(r'\W(stock|gupiao)\W', re.I)
_cls_reos.append((_reo_stock, 'stock'))
_reo_travel = re.compile(r'\W(travel|lvyou|tour|tourism)\W', re.I)
_cls_reos.append((_reo_travel, 'travel'))
_reo_food = re.compile(r'\W(food|meishi|eating|shipin|chihuo|chi)\W', re.I)
_cls_reos.append((_reo_food, 'food'))
_reo_ad = re.compile(r'\W(yiyuan|hospital|about|pinpai|peixun|big5|banking|changfang|games|company|xiezilou|qiye|yiliao|waiyu|jiazheng|xuexiao|zhuangxiu|jiadian)', re.I)
_cls_reos.append((_reo_ad, 'ad'))
_reo_others = re.compile(r'\W(weather|tianqi|marry|games)', re.I)
_cls_reos.append((_reo_others, 'others'))
_reo_pic = re.compile(r'\W(pic|photo|tupian|slide|classad)\W', re.I)
_cls_reos.append((_reo_pic, 'pic'))

def get_class(url):
    url = ' ' + url + ' '
    if url.find('gov.cn') != -1:
        return 'gov'
    for reo,cls in _cls_reos:
        if reo.search(url):
            return cls
    return 'news'

_date_reos = []
_date_reos.append((re.compile(u'\D(\d\d\d\d\d\d\d\d)\D', re.I), '%Y%m%d'))
_date_reos.append((re.compile(u'\D(\d\d\d\d\d\d)\D', re.I), '%Y%m'))
_date_reos.append((re.compile(u'\D(\d\d\d\d\d?\d\d?\d)\D', re.I), '%Y%m%d'))
_date_reos.append((re.compile(u'\D(\d\d\d\d[-/\._]+?\d\d[-/\._]*?\d\d)\D', re.I), r'%Y%m%d'))
_date_reos.append((re.compile(u'\D(\d\d\d\d[-/\._]+?\d\d)\D', re.I), r'%Y%m'))
_date_reos.append((re.compile(u'\D(\d\d\d\d[-/\._]+?\d?\d[-/\._]+?\d?\d)\D', re.I), r'%Y%m%d'))
_date_reos.append((re.compile(u'\D(\d\d\d\d[-/\._]+?\d?\d)\D', re.I), r'%Y%m'))
_date_reos.append((re.compile(u'\D((19|20)\d\d)\D', re.I), r'%Y'))
_reo_digit = re.compile(ur'[\D]+', re.I|re.DOTALL)
_reo_space = re.compile(ur'\s+', re.I|re.DOTALL)

def get_date(url):
    now = datetime.datetime.now()
    for reo, fmt in _date_reos:
        m = reo.search(' ' + urlparse.urlparse(url).path + ' ')
        if m:
            try:
                x = _reo_digit.sub(' ', m.group(1)).strip()
                x = _reo_space.sub('', x).strip()
                dt = datetime.datetime.strptime(x, fmt)
                if dt.month == 1 and fmt.find('%m') == -1:
                    dt = dt.replace(month=now.month)
                if dt.day == 1 and fmt.find('%d') == -1:
                    dt = dt.replace(day=now.day)
                if dt.year >= 1990 and dt.year <= now.year:
                    return dt
            except Exception as e:
                pass
    return now

