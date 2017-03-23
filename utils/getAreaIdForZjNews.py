# -*- coding: utf-8 -*-

from area_utils import *
import sys
reload(sys)
sys.setdefaultencoding('utf-8')
if __name__ == '__main__':

    #build_dict(u'./area_id.text', u'./area_id.dawg')
    dict = load_dict('area_id.dawg')
    info_data = sys.argv[1]
    regionHandler = open (info_data, "r")
    regionLine = regionHandler.readlines()
    #城市名
    for line  in regionLine:
        line = line.strip()
        arr = line.split('\t')
        region_name = arr[0]
        region_id = get_region_id(dict, region_name.decode('utf-8'))
        print region_name, region_id[0][0], region_id[0][1]