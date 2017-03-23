# -*- coding: utf-8 -*-

import os, sys
import time, datetime
from mysql import get_db
from sqlalchemy import Column, String , Integer
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
CWD = os.path.dirname(os.path.abspath(__file__))
sys.path.append('%s/../utils' % CWD)
print '%s/../utils' % CWD
from article import News, LocalNewsAssist, NewsImage

g_db = get_db()
    
#g_engine = create_engine('mysql+mysqldb://bdtt:Chengzi123@rdsv48e18t98h6331x8d.mysql.rds.aliyuncs.com:3306/bdtt?charset=utf8')
g_engine = create_engine('mysql+mysqldb://bdtt:Chengzi123@rdsv48e18t98h6331x8d.mysql.rds.aliyuncs.com:3306/bdtt_algorithm?charset=utf8')
g_db_session = sessionmaker(bind=g_engine)

def save(doc):
    images = [s for s in doc['sections'] if s['type'] == 'img']
    region_type = 2 if not doc['region_id'] else 1
    block_id = 5 if images else 4
    news_category = 0
    status = 0 if images else 1
    news_date = datetime.datetime.strptime(doc['publish_time'], '%Y-%m-%d %H:%M:%S')

    n = News(
        title = doc['title'],
        source_url = doc['url'],
        source_title = doc['source'],
        region_type = region_type,
        news_category = news_category,
        block_id = block_id,
        status = status,
        source_type = 1,
        topic_type = 0,
        news_date = news_date,
        collection_name = doc['collection_name'],
        new_data_id = doc['data_id'],
        region_id_new = doc['region_id'])

    session = g_db_session()
    session.add(n)
    session.commit()
    doc['meta_id'] = n.id

    if status == 1:
        a = LocalNewsAssist(
            tag_id = 0,
            region_id_new = doc['region_id'],
            mp_id = 0,
            news_id = n.id,
            is_recommand = 0,
            sequence = n.sequence)
        session.add(a)
        session.commit()

def update_status(meta_id, status):
    sql = 'UPDATE local_news SET status=%s WHERE id=%s'
    args = (status, meta_id)
    g_db.execute(sql, args)
    if status == 1:
        r = g_db.select('local_news', ['region_id_new', 'sequence'], where="WHERE id=%d" % meta_id)
        if len(r) == 1:
            region_id, sequence = r[0]
            session = g_db_session()
            a = LocalNewsAssist(
                tag_id = 0,
                region_id_new = region_id,
                mp_id = 0,
                news_id = meta_id,
                is_recommand = 0,
                sequence = sequence)
            session.add(a)
            session.commit()

def save_images(meta_id, images):
    session = g_db_session()
    for idx,img in enumerate(images):
        m = NewsImage(
            news_id = meta_id,
            imgUrl = img['src'],
            width = img['width'],
            height = img['height'],
            list_img_order = idx,
            is_list_img = 1 if idx == 0 else 0
            )
        session.add(m)
        session.commit()
