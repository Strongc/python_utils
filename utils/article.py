# -*- coding: utf-8 -*-
from sqlalchemy import Column, String , Integer, SmallInteger, BigInteger
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.dialects.mysql.base import TINYINT,TIMESTAMP,BIGINT
Base = declarative_base()

#for preview
class Article(Base):
    __tablename__ = 'articles'

    id = Column(Integer, primary_key=True)
    title = Column(String)
    url = Column(String)
    body = Column(String)
    publish_time = Column(String)
    source_site = Column(String)
    all_images = Column(String)
    top_image = Column(String)

class News(Base):
    __tablename__ = 'local_news'
    id = Column(BigInteger, primary_key=True)
    data_id = Column(String)
    new_data_id = Column(String)
    title = Column(String)
    description = Column(String)
    news_date = Column(TIMESTAMP)
    source_url = Column(String)
    keywords = Column(String)
    paper_id = Column(String)
    source_title = Column(String)
    region_id = Column(String)
    source_type = Column(Integer)
    status = Column(Integer)
    block_id = Column(Integer)
    region_type = Column(Integer)
    news_all_images = Column(String)
    region_id_new = Column(String)
    sequence = Column(Integer)
    news_category = Column(SmallInteger)
    collection_name = Column(String)
    mp_id = Column(BIGINT)
    mp_name = Column(String)
class NewsImage(Base):
    __tablename__ = 'local_news_image'
    id = Column(BigInteger, primary_key=True)
    news_id = Column(BigInteger)
    imgUrl = Column(String)
    width = Column(Integer)
    height = Column(Integer)
    is_list_img = Column(TINYINT)
    list_img_order = Column(SmallInteger)


class LocalNewsAssist(Base):
    __tablename__ = u'local_news_assist'
    id = Column(BigInteger, primary_key=True)
    tag_id = Column(Integer)
    region_id_new = Column(Integer)
    mp_id = Column(Integer)
    news_id = Column(BigInteger)
    is_recommand = Column(TINYINT)
    sequence = Column(Integer)
    status = Column(TINYINT)
    version = Column(Integer)
    is_delete = Column(TINYINT)

class LocalNewsKeyword(Base):
    __tablename__ = u'local_news_keyword'
    id = Column(Integer, primary_key=True)
    keyword = Column(String)
    news_id = Column(Integer)
    is_manual = Column(Integer)
    news_date = Column(TIMESTAMP)
    source_title = Column(String)
    title = Column(String)
    region_id = Column(Integer)

class LocalNewsVideo(Base):
    __tablename__ = u'local_news_video'
    id = Column(BigInteger, primary_key=True)
    news_id = Column(BigInteger)
    duration = Column(String)
