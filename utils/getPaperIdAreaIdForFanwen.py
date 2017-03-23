# -*- coding: utf-8 -*-

from area_utils import *
import sys
reload(sys)
sys.setdefaultencoding('utf-8')
if __name__ == '__main__':

    #build_dict(u'./area_id.text', u'./area_id.dawg')
    dict = load_dict('area_id.dawg')
    info_data = "paperId_areaId.fix"
    regionHandler = open (info_data, "r")
    regionLine = regionHandler.readlines()
    #城市 id	日期	昨日活跃用户	昨日新增用户	周活跃用户（过去7天启动程序用户）	周活跃度（周活跃用户/总用户）	月活跃用户（过去30天启动程序用户）	月活跃度（月活跃用户/总用户）
    #全国    -1      2016/1/1        4400    180     5409    12.10%

    #日期	昨日新增用户	总用户	昨日活跃用户	日活跃度（昨日活跃用户/总用户）
    # 周活跃用户（过去7天启动程序用户）	周活跃度（周活跃用户/总用户）
    # 月活跃用户（过去30天启动程序用户）	月活跃度（月活跃用户/总用户）
    for line  in regionLine:
        line = line.strip()
        arr = line.split('\t')
        if (len(arr) < 2):
            continue
        paper_id = arr[0]
        region_name = arr[1]
        region_id = get_region_id(dict, region_name.decode('utf-8'))
        print paper_id, region_id[0][0], region_id[0][1], region_name