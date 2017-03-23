
# -*- coding: utf-8 -*-

import sys
import re
import datetime

patterns = []
patterns.append((u'[\uFF1A:\D]\d\d\d\d[-/\s]+?\d?\d[-/\s]+?\d?\d\s+?\d\d[:\s]+?\d\d[:\s]+?\d\d\D', r'_Y_m_d_H_M_S'))
patterns.append((u'[\uFF1A:\D]\d\d\d\d年\d?\d月\d?\d日\s+?\d\d[:\s]+?\d\d[:\s]+?\d\d\D', r'_Y_m_d_H_M_S__2'))
patterns.append((u'[\uFF1A:\D]\d?\d[-/\s]+?\d?\d\s+?\d\d[:\s]+?\d\d[:\s]+?\d\d\D', r'_m_d_H_M_S'))
patterns.append((u'[\uFF1A:\D]\d\d\d\d[-/\s]+?\d?\d[-/\s]+?\d?\d\s+?\d\d[:\s]+?\d\d\D', r'_Y_m_d_H_M__2'))
patterns.append((u'[\uFF1A:\D]\d\d\d\d\s*年\s*\d?\d\s*月\s*\d?\d\s*日\s*\d\d:\d\d\D', r'_Y_m_d_H_M'))
patterns.append((u'[\uFF1A:\D]\d?\d月\d?\d日\s*\d\d:\d\d\D', r'_m_d_H_M__2'))
patterns.append((u'[\uFF1A:\D]\d\d\d\d[-/\s\.]+?\d?\d[-/\s\.]+?\d?\d', r'_Y_m_d'))
patterns.append((u'[\uFF1A:\D]\d\d[-/\s\.]+?\d?\d[-/\s\.]+?\d?\d\D', r'_y_m_d'))
patterns.append((u'[\uFF1A:\D]\d\d\d\d\s*年\s*\d?\d\s*月\s*\d?\d\s*日', r'_Y_m_d__2'))
patterns.append((u'[\uFF1A:\D]\d?\d月\d?\d日\d?\d时\d?\d分\D', r'_m_d_H_M'))
patterns.append((u'[\uFF1A:\D]\d?\d[-/\s]+?\d?\d\D', r'_m_d'))
patterns.append((u'[\uFF1A:\D]\d?\d\s*月\s*\d?\d\s*日', r'_m_d__2'))
patterns.append((u'[\uFF1A:\D]\d\d[:\s]+?\d\d\D', r'_H_M'))
patterns.append((u'\D\d+(分钟|小时)前\D', r'__'))

patterns.sort(key=lambda x:-len(x[0]))
exp = '|'.join(['(?P<%s>%s)' % (x[1], x[0]) for x in patterns])

re_date = re.compile(ur'(%s)' % exp, re.I|re.DOTALL)
re_digit = re.compile(ur'[\D]+', re.I|re.DOTALL)
re_space = re.compile(ur'\s+', re.I|re.DOTALL)

def picktime(s):
    if not s:
        return None
    s = ' ' + s.strip().strip(u'()[]（）').strip() + ' '
    m = re_date.search(s)
    if m:
        for k,v in m.groupdict().items():
            if v:
                x = re_digit.sub(' ', v).strip()
                x = re_space.sub(' ', x).strip()
                f = k.split('__')[0].replace('_', ' %').strip()
                try:
                    now = datetime.datetime.now()
                    dt = datetime.datetime.strptime(x, f) if f else now

                    if dt.year == 1900:
                        dt = dt.replace(year=now.year)

                    if dt > now:
                        dt = dt.replace(year=now.year - 1)
                    return dt
                except Exception as e:
                    pass
    return None

#print picktime(sys.argv[1].decode('utf')).strftime('%Y%m%d')
