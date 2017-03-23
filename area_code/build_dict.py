# -*- coding: utf-8 -*-

from dawg import RecordDAWG, DAWG
import sys
sys.path.append('../../')
from python_utils.ant_utils.mysql import *
def format_region(name):
    #过滤辖区
    if name.find(u'市辖区') >= 0:
        return ""
    if name.find(u'新区') >= 0 or name.find(u'自治州') >= 0 or name.find(u'自治县') >= 0:
        short = name.strip(u'新区').strip(u'自治州').strip(u'自治县') #类似于北新区和新区是不能动的，不能太短
        if len(short) <= 1:
            return name
    short = name.strip(u'区').strip(u'县').strip(u'市')
    if len(short) <= 1:
        return name
    return short


def get_region_list():
    db = get_online_db()
    reload(sys)
    sys.setdefaultencoding('utf-8')
    nameIdMap = {}
    for r in db.select("region", ['id', 'name', 'short_name', 'bdtt_id'], ''):#''where id = 320411'):
        region_id, name, short_name, bdtt_id = r
        name = name.decode('utf8')
        if short_name and len(short_name) > 0:
            short_name = short_name.decode('utf8')
        else:
            short_name = format_region(name)
        region_id_online = bdtt_id
        if not region_id_online or region_id_online == "":
            region_id_online = "0"
        if len(name) >= 2:
            if name not in nameIdMap.keys():
                nameIdMap[name] = []
            nameIdMap[name].append([region_id, region_id_online])
        #print name, region_id, region_id_online
        if short_name != name and len(short_name) >= 2:
            if short_name not in nameIdMap.keys():
                nameIdMap[short_name] = []
            #print short_name, region_id, region_id_online
            nameIdMap[short_name].append([region_id, region_id_online])
    return nameIdMap

if __name__ == '__main__':
    #"654226", "和布克赛尔蒙古自治县", "和布克赛尔县", "0", "312216", "65", "amqp://guest:guest@120.27.247.47:5672/%2F"
    build_file = 'region_all.dawg'


    #generate dict
    format = ">2I"
    keys = []
    values = []

    nameIdMap = get_region_list()
    for k in nameIdMap.keys():
        t = nameIdMap[k]
        for v in t:
            if  v[0] and  v[1]:
                keys.append(k)
                values.append([int(v[0]), int(v[1])])

    print len(values), len(keys)
    for x in range(0, len(values)):
        print keys[x], values[x]
    data = zip(keys, values)
    record = RecordDAWG(format, data)
    with open(build_file, 'wb') as f:
        record.write(f)

