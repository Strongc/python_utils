# -*- coding: utf-8 -*-

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from pymongo import *
import redis

# 初始化数据库连接:
#engine = create_engine('mysql+mysqldb://bdtt:Chengzi123@rdsv48e18t98h6331x8do.mysql.rds.aliyuncs.com:3306/bdtt?charset=utf8')
engine = create_engine('mysql+mysqldb://bdtt:Chengzi123@rm-bp16w04r2zu0w499a.mysql.rds.aliyuncs.com:3306/bdtt_crawler_gzh?charset=utf8mb4')
# 创建DBSession类型:
DBSession = sessionmaker(bind=engine)
#hostMongo = 'mongodb://bdtt:Chengzi123@121.43.56.137:27017/bdtt'
dbMongo = 'bdtt'
#colName = "raw_crawled_news"
colName = 'news'
# 初始化redis数据库连接
#Redis = redis.StrictRedis(host='0f6beec32dac43de.m.cnhza.kvstore.aliyuncs.com',password="0f6beec32dac43de:Chengzi123", port=6379,db=4)
Redis = redis.StrictRedis(host='117.149.19.19',password="0f6beec32dac43de:Chengzi123", port=199,db=4)
