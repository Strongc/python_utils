# -*- coding: utf-8 -*-
from dawg import RecordDAWG, DAWG
import sys
def build_dict(path, path_build):
    format = ">2I"
    keys = []
    values = []
    file_handler = open(path, 'rb')
    for line in file_handler:
        line = line.strip('/r/n')
        arr = line.split('\t')
        try:
            if len(arr) == 3:
                keys.append(arr[0].decode("utf-8"))
                values.append([int(arr[1]), int(arr[2])])
            else:
                continue
        except:
            continue
    data = zip(keys, values)
    record = RecordDAWG(format, data)
    with open(path_build, 'wb') as f:
        record.write(f)

def load_dict(path):
    format = ">2I"
    try:
        d = RecordDAWG(format)
        d.load(path)
        return d
    except Exception, e:
        print "load dict error:..", e.message
        return None

def get_region_id(dict, name):
    regionids = []
    pids = []
    name = name.replace(u'主城区', u'市区')

    #using prefix match
    for key in dict.iterprefixes(name):
        items = dict[key]
        for item in items:
            old_id = item[0]
            new_id = item[1]
            if new_id%100 != 0: #is town
                regionids.append([old_id, new_id])
            elif new_id%100 == 0 and new_id% 10000 != 0: #is city
                pids.append([old_id, new_id])
            else:
                pids.append([old_id, new_id])
    
    for key in dict.iterkeys(name):
        items = dict[key]
        for item in items:
            old_id = item[0]
            new_id = item[1]
            if new_id%100 != 0: #is town
                regionids.append([old_id, new_id])
            elif new_id%100 == 0 and new_id% 10000 != 0: #is city
                pids.append([old_id, new_id])
            else:
                pids.append([old_id, new_id])
    if len(regionids) == 0:
        return pids
    else:
        return regionids

if __name__ == '__main__':
    #build_dict(u'./area_id.text', u'./area_id.dawg')
    dict = load_dict(sys.argv[2])
    #print get_region_id(dict, u'上虞市')
    #print get_region_id(dict, u'上虞区xxx')
    #print get_region_id(dict, u'香港')
    #print get_region_id(dict, u'上饶')
    #print sys.argv[1]
    print get_region_id(dict, sys.argv[1].decode('utf-8'))
#format = ">HHH"
#keys = [u'foo', u'bar', u'foobar', u'foo', u'中文', u'河北']
#values = [(1, 2, 3), (2, 1, 0), (3, 3, 3), (2, 1, 5), (2,2,5), (3,3,5)]
# format = "@255p255pi"
# keys = [u'中文', u'美女', u'foobar', u'foo']
# values = [('美女', 'cde', 3), ('河北', '萧山', 4), ('河北', '萧山', 4), ('河北', '萧山', 4)]
# data = zip(keys, values)
# record_dawg = RecordDAWG(format, data)
#
# #print record_dawg['foo']
# print record_dawg[u'中文'][0][0]
# #rint record_dawg[u'河北']
#
#
# print record_dawg.has_keys_with_prefix(u'中')
# print record_dawg.has_keys_with_prefix(u'小')
#
# print record_dawg.prefixes(u'中文美女')
#
# with open('words.dawg', 'wb') as f:
#     record_dawg.write(f)
#
#
# d = RecordDAWG(format)
# d.load('words.dawg')
#
# for key in d.iterkeys(u'中'):
#     print key, "iterkeys"
# for key in d.iterprefixes(u'中文字'):
#     print key, "iterprefixes"
# print str(d[u'中文'])

