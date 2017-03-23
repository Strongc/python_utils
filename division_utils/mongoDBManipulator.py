# -*- coding: utf-8 -*-

'''
usage:
    handler = MongoDBManipulator(my_logger, my_db) #db must be authenticated with admin user
    news = get_news()
    handler.create(news)
    see examples mongoDBManipulatorTest.py also
'''

import time
import pymongo
import bson
import logging
import re
import os
import threading

import sys
sys.setrecursionlimit(10000)

class MongoDBManipulator(object):

    '''
    @Param logger - logging instance
    @Param db - mongo database instance
    '''
    def __init__(self, logger, db):
        self.logger = logger
        self.db = db
        self.news = None
        self.collection_name = ''
        self.pattern = re.compile(r'^[a-z]+_(19|20)\d+')
        self.lock = threading.RLock()
        self.collections = set()

    '''
    @Param collection. collection to insert news
    @Param news to insert. update_time field must be valid
    @Return tuple(collection_name, _id). return ('', None) for failure
    @Excetpion RuntimeError if collection is invalid 
    '''
    def create_with_collection(self, collection, news):
        if collection == '':
            raise RuntimeError("collection is invalid!")
        #protect critical region
        collection_name = "{0}".format(collection)
        self._create_collection(collection_name)
        id = None

        if '_id' in news:
            self.logger.info("news created with _id key [{0}]".format(news['_id']))
            id = news['_id']
        try:
            ret = self.db[collection_name].insert_one(news)
            self.logger.debug("news inserted into new collection %s with [%s]" %(collection_name, str(ret.inserted_id)))
            id = ret.inserted_id
        except pymongo.errors.DuplicateKeyError as e:
            self.logger.error(e)
            self.logger.error("fails to insert news to collection %s" %collection_name)
            #collection_name = ''

        return tuple((collection_name, id))

    '''
    @Param news to insert. update_time field must be valid
    @Return tuple(collection_name, _id). return ('', None) for failure
    @Excetpion KeyError if update_time is invalid
    '''
    def create(self, news):
        id = None
        if '_id' in news:
            self.logger.info("news created with _id key [{0}]".format(news['_id']))
            id = news['_id']
            #self.save(news)
        #else:
        collection_name = self._check(news)
        #insert news
        try:
            ret = self.db[collection_name].insert_one(news)
            self.logger.debug("news inserted into new collection %s with [%s]" %(collection_name, str(ret.inserted_id)))
            id = ret.inserted_id
        except pymongo.errors.DuplicateKeyError as e:
            self.logger.error(e)
            self.logger.error("fails to insert news to collection %s" %collection_name)
            #collection_name = ''

        return tuple((collection_name, id))

    '''
    @Param news to insert. update_time field must be valid
    @Return tuple(status, collection_name, _id).
    @Status:
    @    0: success;   1: duplicatedKey    2:other error
    @Excetpion KeyError if update_time is invalid
    '''
    def create_with_status(self, news):
        id = None
        if '_id' in news:
            self.logger.info("news created with _id key [{0}]".format(news['_id']))
            id = news['_id']
            #self.save(news)
        #else:
        collection_name = self._check(news)
        #insert news
        try:
            ret = self.db[collection_name].insert_one(news)
            self.logger.debug("news inserted into new collection %s with [%s]" %(collection_name, str(ret.inserted_id)))
            id = ret.inserted_id
            status = 0
        except pymongo.errors.DuplicateKeyError as e:
            self.logger.warn(e)
            status = 1
        except Exception,e:
            self.logger.error(e)
            self.logger.error("fails to insert news to collection %s" % collection_name)
            collection_name = ''
            status = 2

        return tuple((status, collection_name, id))

    '''
    @Param - filter should be {'_id' : my_id}
    @Param - update action like some field which must contains update_time field
    for example : {'update_time':'2016-06-11T13:24:57', 'news_title': 'my_title'}
    @Return - a list of tuple(collection, id)
    @Exception - KeyError if 'update_time' field is invalid
    @Exception - pymongo.errors.OperationFailure if specified news not found
    '''
    def update(self, collection, filter, update):
        if 'update_time' not in update:
            self.logger.error('update_time not exist')
            raise KeyError('update_time not exist')

        #fetch news from 'collection' and save to new collection
        cursor = self.find(collection, filter)
        if cursor.count() == 0:
            msg = 'news with filter[%s] not found in collection[%s]' %(str(filter), collection)
            self.logger.error(msg)
            raise pymongo.errors.OperationFailure(msg)

        ret = []
        for news in cursor:
            for key in update:
                news[key] = update[key]
            collection_name, id  = self.create(news)
            ret.append(tuple((collection_name, id)))

        return ret

    '''
    @Param - collection name of collection to manipulate
    @Param - filter should be ['_id' : my_id]
    @Return - An instance of pymongo.results.DeleteResult
    @Exception - pymongo.errors.OperationFailure if specified collection not exist
    '''
    def delete(self, collection, filter):
        if collection not in self.db.collection_names():
            raise pymongo.errors.OperationFailure('collection[%s] not exist for delete operation with filter[%s]' %(collection, str(filter)))

        return self.db[collection].delete_many(filter)

    '''
    @Param - news to save
    comment : DEPRECATED  since version 3.0. suggest update/insert/create
    @Exception KeyError if _id or update_time not exist
    '''
    def save(self, news):
        if '_id'  not in news:
            raise KeyError('_id field not exist')
        collection_name = self._check(news)
        self._save(collection_name, news)
        return tuple((collection_name, news['_id']))

    def _check(self, news):
        if "update_time" not in news or news['update_time'] is None:
            raise KeyError('update_time filed not exist')

        news_date = news['update_time']
        collection_name = self._get_collection_from_newsdate(news_date)
        self.logger.info('got collection %s' %collection_name)
        match = self.pattern.search(collection_name)
        if match is None:
            raise KeyError('update_time[%s] filed is invalid' %str(news_date))

        self._create_collection(collection_name)
        return collection_name

    def _create_collection(self, collection_name):
        #create collection if it does not exist
        if collection_name in self.collections:
            return
        
        cols = set(self.db.collection_names())
        if collection_name in cols:
            with self.lock:
                self.collections.add(collection_name)
        else:
            self.logger.info("Collection %s not in database and will be added" %collection_name)
            self.lock.acquire()
            try:
                self.db.create_collection(collection_name)
                self.collections.add(collection_name)
            except pymongo.errors.OperationFailure as e:
                self.logger.info(e)
            except pymongo.errors.CollectionInvalid as e:
                self.logger.info(e)

            #create shard on such collection
            try:
                ret = self.db.client.admin.command("shardCollection",
                        "%s.%s"%(self.db.name, collection_name), key={"_id" : pymongo.HASHED})
                if u'ok' not in ret or ret[u'ok'] != 1.0:
                    self.logger.error("Fail to create shard on collection[%s]" %collection_name)
            except pymongo.errors.OperationFailure as e:
                self.logger.error(e)
                err_msg = str(e)
                if err_msg.find("create an index") != -1:
                    #create an index first
                    try:
                        self.logger.debug("create indexes on collection[%s]" %collection_name)
                        self.db[collection_name].create_index([("_id", pymongo.HASHED)])
                        #Try enable shard again
                        self.db.client.admin.command("shardCollection",
                                "%s.%s"%(self.db.name, collection_name), key={"_id" : pymongo.HASHED})
                    except pymongo.errors.OperationFailure as e:
                        self.logger.error(e)
                    except Exception as e:
                        self.logger.error(e)
            self.lock.release()

    def _get_collection_from_newsdate(self, news_date):
        collection_name = ''
        try:
            timestamp = int(news_date)
            collection_name = self._get_collection_name(timestamp)
        except ValueError as e:
            self.logger.info(e)
            #check again
            pos = news_date.strip().find(' ')
            if pos != -1:
                collection_name = 'normal_' + news_date[:pos].replace('-', '').replace('_', '')

            pos = news_date.strip().find('T')
            if pos != -1:
                #format "2016-05-15T21:05:00"
                collection_name = 'normal_' + news_date[:pos].replace('-', '').replace('_', '')

        return collection_name


    '''
    @Param collection - collection name string type
    @Param filter - something like {'_id' : my_id}
    @Return an instance of Cursor 
    @Exception - pymongo.errors.OperationFailure if specified collection not exist
    '''
    def find(self, collection, filter):
        if collection not in self.db.collection_names():
            self.logger.error('collection[%s] not exist in db[%s]' %(collection, self.db.name))
            raise pymongo.errors.OperationFailure('collection[%s] not exist in db[%s]' %(collection, self.db.name))

        return self.db[collection].find(filter)


    def _get_collection_name(self, timestamp, prefix = 'normal_'):
        name = prefix + time.strftime("%Y%m%d", time.localtime(timestamp))
        return name

    def _save(self, collection_name, news):
        self.db[collection_name].save(news)

class MongoDbInstance(object):
    def __init__(self, paras=None, addr=u'114.55.66.100', port=27017,\
                 database=u'bdtt', user=u'admin', passwd=u'Cqkj10xwdmx!', logger=None):

        if paras is None:
            paras = {u'addr': addr,
                     u'port': port,
                     u'dataBase': database,
                     u'user': user,
                     u'pswd': passwd}
        self.paras = paras
        if logger:
            self.logger = logger
        else:
            self.setlog(u'MongoDbInstance')
        self.reconnect()

    def reconnect(self):
        try:
            self.db = self.connMongo(**self.paras)
            self.handler = MongoDBManipulator(self.logger, self.db)
        except Exception:
            self.logger.error("connect to mongodb failed...")

    # set logger
    def setlog(self, logName): # self.__class__.__name__
        if not os.path.exists(u'log'):
            os.mkdir(u'log')
        logging.basicConfig(
                    filename = "log/log." + logName,
                    format = "%(levelname)-10s 0x%(thread)-16x%(asctime)s %(message)s",
                    level = logging.DEBUG)
        self.logger = logging.getLogger(logName)
    #connect
    def connMongo(self, addr, port, dataBase, user, pswd):
        client = pymongo.MongoClient(addr, port)
        db = client[dataBase]
        try:
            ret = client.admin.authenticate(user, pswd)
            if not ret:
                print "authentication fails on user admin. Please check your authentication. Exit now..."
                exit(1)
        except pymongo.errors.OperationFailure as e:
            print "authentication fails. Exiting now ..."
            exit(1)
        return db
