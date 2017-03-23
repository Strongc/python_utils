# -*- coding: utf-8 -*-
from pymongo import *
from bson import ObjectId
import time
import datetime
def ConnectMongo(url, dbName):
    try:
        client = MongoClient(url)
        db = client[dbName]
        return db
    except:
        print "connect to mongodb:", url ,"failed..."

def GetCollection(db, colName):
    try:
        return db[colName]
    except:
        print "get collection:", colName, "failed..."

#insert into mongo
def InsertMongoItem(col, item):
   if len(item) >0:
       #try:
           col.insert(item)
       #except:
       #    print "insert into mongodb failed..."



#insert into mongo
def InsertMongoItems(col, items):
    for item in items:
        if not item.has_key(u'article_id'):
            item[u'article_id'] = time.time()
        InsertMongoItem(col, item)

#save into mongo
def SaveMongo(col, items):
    for item in items:
        if len(item) > 0:
            col.save(item)

def SaveMongoItem(col, item):
    if len(item) > 0:
        col.save(item)

#update mongo
def UpdateMongo(col, cond, set, upsert, multi ):
    col.update(cond, set, upsert, multi)

#clear mongo
def ClearMongo(col):
    col.remove()

#get mongo
#where: map for conditions
def GetMongoItems(collection, where = None, start = 0):
    if where is None or len(where) == 0:
        return collection.find({}).skip(start)
    else:
        return collection.find(where).skip(start)

def RemoveMongoItem(col, item):
    #print item
    col.remove({u'_id':item[u'_id']})
#remove items
def RemoveMongoItems(col, items):
    for item in items:
        RemoveMongoItem(col, item)

#mongodb id hash
def ObjectIdHash(objectid, modnum):
    return hash(objectid)%modnum

def timestamp_from_objectid(objectid):
  result = 0
  try:
    result = time.mktime(objectid.generation_time.timetuple())
  except:
    pass
  return result

def object_id_from_datetime(from_datetime, span_days=None, span_hours=None, span_minutes=None, span_weeks=None):
    '''根据时间手动生成一个objectid，此id不作为存储使用'''

    print from_datetime

    dateArray = datetime.datetime.utcfromtimestamp(from_datetime)
    vtime = dateArray - datetime.timedelta(days=span_days)
    return ObjectId.from_datetime(generation_time=vtime)

if __name__ == '__main__':
    #test connection
    hostMongo = 'mongodb://bdtt:Chengzi123@121.43.56.137:27017/bdtt'
    db = ConnectMongo(hostMongo, "bdtt")
    num = db.news.find({}).count()
    print "news count...", num
    #test get collection
    collection = GetCollection(db, u'news')
    print "from collection", collection.find({}).count()
    where = {"paperID":"10100"}
    items = GetMongoItems(collection, where)

    #insert into news_test
    col2 = GetCollection(db, u'news_test')
    #test clear
    ClearMongo(col2)

    print col2.find({}).count()
    #print items
    #test insert
    InsertMongoItems(col2, items)
    print col2.find({}).count()

    items = GetMongoItems(col2)
    print items.count()
    RemoveMongoItem(col2, items)
    print col2.find({}).count()




