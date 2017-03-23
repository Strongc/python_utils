# -*- coding: utf-8 -*-
from dawg import RecordDAWG#, DAWG
import sys
class AreaCode(object):
    def __init__(self, dict_path):
        reload(sys)
        sys.setdefaultencoding('utf8')
        self.dict_path = dict_path
        self.format = ">2I"
        try:
            self.dict = RecordDAWG(self.format)
            self.dict.load(dict_path)
        except Exception, e:
            print "load dict error:",dict_path, e.message

    def format_input(self, name):
        return  name.replace(u'主城区', u'市辖区')

    def is_same_prov(self, r1, r2):
        if r1 >= 100000 and r2 >= 100000\
                and r1 <= 900000 and r2 <= 900000 \
                and str(r1)[:2] == str(r2)[:2]:
            return True
        return False

    def is_same_city(self, r1, r2):
        if r1 >= 100000 and r2 >= 100000\
                and r1 <= 900000 and r2 <= 900000\
                and str(r1)[:4] == str(r2)[:4]:
            return True
        return False

    #get by prov and city
    def get_extract_area_code(self, provname, name, level=1):

        code = self.get_area_code(provname, level=level)
        prov_id = 0
        for x in code:
            if len(x) >= 3 and x[2] <= 1:
                prov_id = x[0]

        code = self.get_area_code(name, level=level)
        region_id = 0
        region_ids = []
        for x in code:
            if len(x) == 3 and x[2] == 0:
                if self.is_same_prov(x[0], prov_id):
                    return  x[0]
        for x in code:
            if len(x) == 3:
                if self.is_same_prov(x[0], prov_id):
                    return x[0]
        return prov_id #没找到，只能用省级id

    def get_same_area_code(self, region_id_refer, name,  level=1):
        code =self.get_area_code(name, level=level)
        #优先精确匹配的同地级市id
        for x in code:
            if len(x) == 3 and x[2] == 0:
                if self.is_same_city(x[0], region_id_refer):
                    return x[0]
        #精确匹配的省级id
        for x in code:
            if len(x) == 3 and x[2] == 0:
                if self.is_same_prov(x[0], region_id_refer):
                    return x[0]
        #精确匹配的ID
        for x in code:
            if len(x) == 3 and x[2] == 0:
                return x[0]
        #非精确匹配的ID
        for x in code:
            if len(x) == 3 and x[2] == 1:
                if self.is_same_city(x[0], region_id_refer):
                    return x[0]
        for x in code:
            if len(x) == 3 and x[2] == 1:
                if self.is_same_prov(x[0], region_id_refer):
                    return x[0]
        for x in code:
            if len(x) == 3 and x[2] == 1:
                return x[0]
        for x in code:
            if len(x) == 3 and x[2] == 2:
                return x[0]
        return 0  #0 - means unknown

    #level: 0 精确
    #level: 1 长串 比如大兴安岭地区1234->大兴安岭地区; 上虞区人民法院->上虞|上虞区
    #level: 2 子串 比如大兴安 -> 大兴安岭地区；上->上城区|上虞区
    def get_area_code(self, name, level=0):
        name = self.format_input(name)
        regionids = []
        region_pids = []
        type = 0 #精确匹配
        #完美匹配
        if self.dict.has_key(name):
            items = self.dict[name]
            for item in items:
                region_id = item[0]
                region_id_online = item[1]
                if region_id % 100 != 0:  # 县级
                    regionids.append([region_id, region_id_online,type])
                elif region_id % 100 == 0 and region_id % 10000 != 0:  # 地级市和省级
                    region_pids.append([region_id, region_id_online, type])
                else:
                    region_pids.append([region_id, region_id_online, type])

        # 使用前缀匹配： 比如大兴安岭地区1234->大兴安岭地区; 上虞区人民法院->上虞|上虞区
        if len(regionids) <= 0 or level >= 1:
            type = 1 #前缀
            for key in self.dict.iterprefixes(name):
                items = self.dict[key]
                for item in items:
                    region_id = item[0]
                    region_id_online = item[1]
                    if region_id % 100 != 0:  # 县级
                        regionids.append([region_id, region_id_online,type])
                    elif region_id % 100 == 0 and region_id % 10000 != 0:  # 地级市和省级
                        region_pids.append([region_id, region_id_online,type])
                    else:
                        region_pids.append([region_id, region_id_online,type])
        # 子串匹配：比如大兴安 -> 大兴安岭地区
        if len(regionids) <= 0 or level >= 2:
            type = 2 #字串
            for key in self.dict.iterkeys(name):
                items = self.dict[key]
                for item in items:
                    region_id = item[0]
                    region_id_online = item[1]
                    if region_id % 100 != 0:  # 县级
                        regionids.append([region_id, region_id_online,type])
                    elif region_id % 100 == 0 and region_id % 10000 != 0:  # 地级市和省级
                        region_pids.append([region_id, region_id_online,type])
                    else:
                        region_pids.append([region_id, region_id_online, type])

        if len(regionids) == 0:
            return region_pids
        elif len(region_pids) > 0: #优先上一级
            return region_pids+regionids
        else:
            return regionids

if __name__ == '__main__':
    acdict = AreaCode('region_all.dawg')
    print acdict.get_area_code(u'上饶县', level=1)
    print acdict.get_same_area_code(361100, u"上饶市",level=1)
    #print acdict.get_extract_area_code(u'北京市', u'上虞', level =1)