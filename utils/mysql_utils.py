# -*- coding: utf-8 -*-
import sys
import MySQLdb
from article import *
from config import *
import re
import datetime
def connectToMysql(host, user, passwd, db, port=3306):
    try:
        conn = MySQLdb.connect(host=host, user=user, passwd=passwd, db=db, port=port, charset='utf8')
        cur = conn.cursor()
    except Exception, ex:
        print ("exception when connect to mysql: host=%s, user=%s, passwd=%s, db=%s, port=%s"%(host, user, passwd, db, port))
        return None
    return conn

def closeConn(conn):
    conn.close()

def testDb(conn):
    try:
        if executeSql(conn, "show tables") is not None:
            return True
        else:
            return False
    except:
        return False

def executeSql(conn, sql):
    try:
        cur = conn.cursor()
        cur.execute(sql)
        return cur.fetchall()
    except Exception, ex:
        print "execute sql failed...", sql, ex
        return None

def is_allow_auto_update(conn, region_id):
    sql = "select is_allow_auto_update from local_region where new_region_id=%d"%region_id
    result = executeSql(conn, sql)
    if result and len(result) > 0:
        if len(result[0]) > 0:
           if result[0][0] == 1 or result[0][0] == True:
               return True
    return False

#DEDUP
def DedupByMysql(conn, key):
    key = key.replace('\"', "")
    sql = 'select count(*) from local_mp_data_dedup_keys  where dedup_key = "%s"'%key
    result = executeSql(conn, sql)

    if result is not None and len(result) > 0:
        if result[-1] is not None and len(result[-1]) > 0:
            return int(result[-1][0])
    return 0

#ADD DUP KEY
def AddKeyToMysql(conn, key):
    key = key.replace('\"', "")
    sql = 'insert into local_mp_data_dedup_keys set dedup_key ="%s"' % key
    executeSql(conn, sql)
    conn.commit()
    return

#FOR ADD IMAGE
"""
    PARAS: [URL, [[width,height],type], bucketkey, order]
    imgType:
"""
def AddImgTosql(paras, news_id, imgType, session):
    if paras is None or len(paras) == 0:
        print 'invalid images'
        return

    para_url = paras[0]
    para_width = paras[1][0][0]
    para_height = paras[1][0][1]
    para_type = paras[1][1]
    para_order = paras[3] #3-order
    para_imgtype = imgType

    if not para_url:
        return
    b = NewsImage(news_id=news_id,
            imgUrl=para_url,
            width = para_width,
            height = para_height,
            list_img_order = para_order,
            is_list_img = para_imgtype
            )

    session.add(b)
    session.commit()

#SET BLOCK
def setBlockId(item):
    block_id = 5# 单图

    if item[u'news_image'] is None or item[u'news_image'] == u'':
        block_id = 4 #无图
    if len(item[u'news_image'].split(u'%D%W')) > 1:
        block_id = 6

    if item[u'cat'] in [130,133]:
        print u'joke'
        tmp_content = re.sub(u'\s*', u'', item[u'news_intro'])
        if item[u'allimage']:
            img_num = len(item[u'allimage'].split(u'%D%W'))
        else:
            img_num = 0
        if img_num == 0:
            block_id = 9
        elif len(tmp_content) == 0 and img_num == 1:
            block_id = 10
        else:
            block_id = 11
    return block_id

#def setStatus(item):
"""
    item: NEWS ITEM - check all item keys
    img_head_paras:PARAS: [URL,  [[width,height],type], bucket, order]
    img_all_paras: PARAS: [URL,  [[width,height],type], bucket, order]
"""
def AddToMysql(item, session, img_head_paras, img_all_paras):
    block_id = setBlockId(item)
    #status = setStatus(item)
    ctime = datetime.datetime.fromtimestamp(float(item["update_time"]))
    try:
        a = News(title=item["news_title"].encode("utf-8"),
            new_data_id=item["_id"],
            data_id=item["_id"],
            source_url=item["originalUrl"],
            news_date=ctime,
            source_title=item["typeName"].encode("utf-8"),
            region_id=item["region_id"],
            region_id_new=item["region_id_new"],
            source_type=item['source_type'],  #新闻
            paper_id = item['paper_id'],
            block_id = block_id, #单图or无图
            status = item['status'], #不发布
            region_type=item['region_type'], #热点
            news_all_images=item["allimage"].encode("utf-8"),
            description = item[u'news_intro'].encode("utf-8"),
            news_category = item[u'news_category'],
            collection_name = item[u'collection_name'],
            mp_id = item[u'mp_id'],
            mp_name = item[u'pinyin']
            )
    except Exception as e:
        print e
    try:
        session.add(a)
        session.commit()
    except Exception as e:
        print e
    news_id = a.id #added
    if news_id == None or news_id == '':
        print 'news_id is none'
        return '' 
    news_sequence = a.sequence
    print "insert to mysql local_news ok...", item["_id"], news_id, block_id, news_sequence

    #默认-美女/视频， 30800 ,其他30700
    if 'version' not in item:
        if item['cat'] in [138, 146]:
            item['version'] = '30800'
        else:
            item['version'] = '30700'

    #recommand用来兼容3.7版本, 3.8版本停用
    if 'is_recommand' not in item:
        #如果cat=0 or cat=-1 且status =1 自动推荐-支持3.7 ， 新闻非热点
        if (item[u'cat']== 0 or item[u'cat'] == -1) and item[u'status'] == 1 and item['source_type']==1 and item['region_id_new']!= 999999:
            item['is_recommand'] = 1
        else:
            item['is_recommand'] = 0

    c = LocalNewsAssist(
            #news_id = item[u'_id'],
            tag_id = item[u'cat'],
            region_id_new = item[u'region_id_new'],
            mp_id = item['mp_id'],
            news_id = news_id,
            is_recommand = 0,
            sequence = news_sequence,
            status = item['status'],
            version = item['version'],
            is_delete = 0)
    session.add(c)
    session.commit()
    print "insert to mysql local_news_assist ok!",c.id
    if img_head_paras or img_all_paras :
        AddImgsToSql(session, news_id, img_head_paras, img_all_paras )

    if 'time_mask' in item:
        d = LocalNewsVideo(news_id = news_id,
                duration = item['time_mask']
                )
        session.add(d)
        session.commit()
        print 'insert to mysql local_news_video ok!', d.id

    return news_id

def AddImgsToSql(session, news_id, img_head_paras, img_all_paras):
    # 保存图片
    if img_head_paras:
        for paras in img_head_paras:
            AddImgTosql(paras, news_id, 1, session)
    if img_all_paras:
        for paras in img_all_paras:
            AddImgTosql(paras, news_id, 0, session)

if __name__ == '__main__':
    conn = connectToMysql(u'rdsv48e18t98h6331x8d.mysql.rds.aliyuncs.com',
                          u'bdtt_algorithm', u'Chengzi123', u'bdtt', 3306)
    print "connect to mysql ok"

    print testDb(conn)

    sql = "select MAX(id) from local_news_image"
    result = executeSql(conn, sql)
    #print result[0][0]
    sql = 'select data_id from local_news where data_id is not null and source_url != "" order by data_id desc limit 10'
    result = executeSql(conn, sql)

    print result

    sql = 'update local_news set news_all_images="http://img.benditoutiao.com/e50859e0-e12d-11e5-91ad-00163e0022cd%D%Whttp://img.benditoutiao.com/e50fdb02-e12d-11e5-91ad-00163e0022cd%D%Whttp://img.benditoutiao.com/e5146d52-e12d-11e5-91ad-00163e0022cd%D%Whttp://img.benditoutiao.com/e51d62d6-e12d-11e5-91ad-00163e0022cd%D%Whttp://img.benditoutiao.com/e52161ba-e12d-11e5-91ad-00163e0022cd%D%Whttp://img.benditoutiao.com/e5350e9a-e12d-11e5-91ad-00163e0022cd%D%Whttp://img.benditoutiao.com/e53ffac6-e12d-11e5-91ad-00163e0022cd%D%Whttp://img.benditoutiao.com/e548f6ee-e12d-11e5-91ad-00163e0022cd%D%Whttp://img.benditoutiao.com/e54fe616-e12d-11e5-91ad-00163e0022cd%D%Whttp://img.benditoutiao.com/e5c8ba28-e12d-11e5-91ad-00163e0022cd%D%Whttp://img.benditoutiao.com/e5cf7af2-e12d-11e5-91ad-00163e0022cd%D%Whttp://img.benditoutiao.com/e5d56872-e12d-11e5-91ad-00163e0022cd%D%Whttp://img.benditoutiao.com/e5daa4ae-e12d-11e5-91ad-00163e0022cd%D%Whttp://img.benditoutiao.com/e5e11032-e12d-11e5-91ad-00163e0022cd%D%Whttp://img.benditoutiao.com/e5e92542-e12d-11e5-91ad-00163e0022cd%D%Whttp://img.benditoutiao.com/e5f586d4-e12d-11e5-91ad-00163e0022cd%D%Whttp://img.benditoutiao.com/e5fd5ad0-e12d-11e5-91ad-00163e0022cd%D%Whttp://img.benditoutiao.com/e60fbc20-e12d-11e5-91ad-00163e0022cd%D%Whttp://img.benditoutiao.com/e61c8a36-e12d-11e5-91ad-00163e0022cd%D%W" where data_id = "56d80b3d17a444e229c44480" and paper_id is null'
    result = executeSql(conn, sql)
    conn.commit()
    closeConn(conn)
