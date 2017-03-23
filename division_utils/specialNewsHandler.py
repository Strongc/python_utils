#!/bin/env python
# -*- coding: utf-8 -*- 

import pymongo
import time
import datetime
import os
import logging
from optparse import OptionParser
import commands
import bson
import re
import threading
import Queue
import random
from multiprocessing import cpu_count 
from signal import signal, SIGINT

import site
#site.addsitedir("/usr/lib/python2.6/site-packages/PyMySQL-0.7.3-py2.6.egg/")
import pymysql


logLevelMapping = {
        "DEBUG" : logging.DEBUG,
        "INFO"  : logging.INFO,
        "WARNING" : logging.WARNING,
        "ERROR" : logging.ERROR,
        "CRITICAL" : logging.CRITICAL
        }

def getNewsIdFromTable(logger, cursor, tableName, id = None):
    newsIds = dict()
    extra = ''
    if id:
        extra = " AND b.id > %d" %id
    query1 = "SELECT b.id, a.data_id FROM local_news a, %s b WHERE b.news_id = a.id %s GROUP BY a.data_id" %(tableName, extra)
    query2 = "SELECT b.id, a.data_id FROM local_news_bak a, %s b WHERE b.news_id = a.id %s GROUP BY a.data_id" %(tableName, extra)
    logger.info("Execute SQL query %s" %query1)
    logger.info("Execute SQL query %s" %query2)
    try:
        cursor.execute(query1)
        for row in cursor:
            newsIds[row[0]] = row[1]

        cursor.execute(query2)
        for row in cursor:
            newsIds[row[0]] = row[1]
    except Exception as e:
        logger.error(e)

    logger.debug("Table %s has unique %d news data_id" %(tableName, len(newsIds)))
    return newsIds

class ChangedItem(object):
    def __init__(self, dataId, newDataId, connection):
        self.dataId = dataId
        self.newDataId = newDataId
        self.connection = connection

class MySQLHandler(threading.Thread):
    def __init__(self, logger, status = True, 
            host = 'rdsv48e18t98h6331x8d.mysql.rds.aliyuncs.com', port = 3306, 
            user = 'bdtt', pwd = 'Chengzi123', db = 'bdtt', 
            charset = 'utf8', queueSize = 200):
        threading.Thread.__init__(self)
        self.logger = logger
        self._status = status
        self.host = host
        self.port = port
        self.user = user
        self.pwd = pwd
        self.db = db
        self.charset = charset
        self.queue = Queue.Queue(queueSize)
        self.conn = pymysql.connect(host = self.host, user = self.user, password = self.pwd, 
                database = self.db, port = self.port, charset = self.charset, autocommit = True)
        self.cursor = self.conn.cursor()

    def addColumnsToTable(self, tableName):
        query = "ALTER TABLE %s ADD COLUMN collection_name %s DEFAULT '%s'" %(tableName, u'varchar(128)', u'')
        self.logger.info("Execute SQL statement[%s]" %query)
        self.cursor.execute(query)
        query = "ALTER TABLE %s ADD COLUMN new_data_id %s DEFAULT '%s'" %(tableName, u'varchar(128)', u'')
        self.logger.info("Execute SQL statement[%s]" %query)
        self.cursor.execute(query)

    def init(self):
        tableNewsOK = False
        tableNewsBakOK = False
        self.cursor.execute("SHOW COLUMNS FROM local_news")
        for col in self.cursor:
            if u'collection_name' == col[0]:
                self.logger.info("COLUMN collection_name already added in TABLE local_news")
                tableNewsOK = True

        self.cursor.execute("SHOW COLUMNS FROM local_news_bak")
        for col in self.cursor:
            if u'collection_name' == col[0]:
                self.logger.info("COLUMN collection_name already added in TABLE local_news_bak")
                tableNewsBakOK = True

        if tableNewsOK and tableNewsBakOK:
            return 

        try:
            self.conn.begin()
            if not tableNewsOK:
                self.addColumnsToTable('local_news')

            if not tableNewsBakOK:
                self.addColumnsToTable('local_news_bak')

        except Exception as e:
            self.logger.critical(e)
            self.logger.critical('Add column to table local_news failed. exit..')
            self.cursor.close()
            self.conn.rollback()
            self.conn.close()
            exit(1)
        self.conn.commit()

    def getCursor(self):
        #should add a mutex
        return self.cursor

    def terminate(self):
        self._status = False

    def put(self, item):
        self.queue.put(item)

    def queryAndUpdateTable(self, tableName, item):
        dataId = item.dataId
        newDataId = item.newDataId
        connection = item.connection

        collectionName = ''
        query = "SELECT collection_name FROM %s WHERE data_id='%s'" %(tableName, dataId)
        ret = self.cursor.execute(query)
        for row in self.cursor:
            collectionName = row[0]
        self.logger.debug("execute query with command[%s] and result is %d collection_name=%s" %(query, ret, collectionName))
        if ret > 0 and collectionName.find("handwork") == -1:
            command = "UPDATE %s SET collection_name='%s', new_data_id='%s' WHERE data_id='%s'" %(tableName, connection, newDataId, dataId)
            self.logger.debug("update table %s with command [%s]" %(tableName, command))
            try:
                status = self.cursor.execute(command)
                if status < 1:
                    self.logger.error("Fail to execute SQL statement %s" %command)
            except Exception as e:
                self.logger.error(e)


    def handle(self):
        try:
            item = self.queue.get(timeout = 10)
            self.queryAndUpdateTable('local_news', item)
            self.queryAndUpdateTable('local_news_bak', item)
        except Queue.Empty as e:
            self.logger.info("Exception is caught due to no news in queue. Continue...")
        except Exception as e:
            self.logger.error(e)

    def run(self):
        self.logger.debug("MySQL thread starts")
        while self._status:
            self.handle()
        #handle left over tasks
        while not self.queue.empty():
            self.logger.debug("%d news left to handle in MySQL" %self.queue.qsize())
            self.handle()
        self.cursor.close()
        #self.conn.commit()
        self.conn.close()
        self.logger.debug("Exit now...")

class NewsInserter(threading.Thread):
    def __init__(self, logger, username, password, host, port, database, status = True, queueSize = 300):
        threading.Thread.__init__(self)
        self.logger = logger
        self.username = username
        self.password = password
        self.host = host
        self.port = port
        self.database = database
        self._status = status
        self.pattern = re.compile(r'^[a-z]+_(19|20)\d+')
        self.queue = Queue.Queue(queueSize)
        self.client = None
        self.db = None
        self.name = ''
        self.sqlHandler = MySQLHandler(self.logger)

    def init(self):
        self.client = pymongo.MongoClient(self.host, self.port)
        self.db = self.client[self.database]
        try:
            #authenticate to obtain super priority
            ret = self.client.admin.authenticate(self.username, self.password)
            if not ret:
                self.logger.critical("authentication fails on user %s. Please check your authentication. Exit now..." %self.username)
                exit(1)
        except pymongo.errors.OperationFailure as e:
            self.logger.critical(e)
            self.logger.critical("authentication fails. Exiting now ...")

        self.sqlHandler.init()
        self.sqlHandler.start()

    def put(self, item):
        self.queue.put(item)

    def getQsize(self):
        return self.queue.qsize()

    def getSQLClient(self):
        return self.sqlHandler

    def getName(self):
        return self.name

    def getNews(self):
        return self.queue.get(timeout = 6)

    def terminate(self):
        self._status = False

    def cleanUp(self):
        self.sqlHandler.terminate()

    def getCollectionName(self, timeStamp, prefix = "normal_"):
        dateFormat = prefix + time.strftime("%Y%m%d", time.localtime(timeStamp))
        return dateFormat

    def handle(self):
        try:
            prefix, doc = self.getNews()
        except Queue.Empty as e:
            self.logger.info("No News in queue. Return directly")
            return
        
        newsId = str(doc['_id'])
        newCollectionName = ''
        if len(newsId) < 8:
            self.logger.critical("OjbectId[%s] is invalid!" %newsId)
            return

        try:
            timestamp = int(newsId[0:8], 16)
            newCollectionName = self.getCollectionName(timestamp, prefix)
        except ValueError as e:
            self.logger.error(e)
            self.logger.critical("OjbectId[%s] is invalid!" %newsId)
            return

        match = self.pattern.search(newCollectionName)
        if match is None:
            self.logger.error("news with objectId[%s] is not clarified by newCollectionName %s" %(newsId, newCollectionName))
            return

        if newCollectionName not in self.db.collection_names():
            self.logger.info("Collection %s not in database from news id[%s]" %(newCollectionName, newsId))
            #add collection
            self.logger.info("Collection %s will be added into database" %newCollectionName)
            try:
                self.db.create_collection(newCollectionName)
            except pymongo.errors.OperationFailure as e:
                self.logger.info(e)
            except pymongo.errors.CollectionInvalid as e:
                self.logger.info(e)

            #create shard on such collection
            try:
                ret = self.db.client.admin.command("shardCollection",
                        "%s.%s"%(self.db.name, newCollectionName), key={"_id" : pymongo.HASHED})
                if u'ok' not in ret or ret[u'ok'] != 1.0:
                    self.logger.error("Fail to create shard on collection[%s]" %newCollectionName)
            except pymongo.errors.OperationFailure as e:
                self.logger.error(e)
                errMsg = str(e)
                self.logger.debug(errMsg)
                if errMsg.find("create an index") != -1:
                    #create an index first
                    #index = pymongo.IndexModel([("article_id", pymongo.ASCENDING), ("_id", pymongo.ASCENDING)])
                    try:
                        self.logger.debug("create indexes on collection[%s]" %newCollectionName)
                        self.db[newCollectionName].create_index([("_id", pymongo.HASHED)])
                        #Try enable shard again
                        self.db.client.admin.command("shardCollection",
                                "%s.%s"%(self.db.name, newCollectionName), key={"_id" : pymongo.HASHED})
                    except pymongo.errors.OperationFailure as e:
                        self.logger.error(e)
                    except Exception as e:
                        self.logger.error(e)

        newCollection = self.db[newCollectionName]
        try:
            ret = newCollection.insert_one(doc)
            newId = str(ret.inserted_id)
            self.logger.debug("news with id[%s] inserted into new collection %s new id[%s]" %(newsId, newCollectionName, newId))

            item = ChangedItem(newsId, newId, newCollectionName)
            #deliever news to child threads handling
            delievered = False
            while not delievered:
                try:
                    self.sqlHandler.put(item)
                    delievered = True
                except Queue.Full as e:
                    self.logger.info("threads %s is full. Wait for a while")
                    time.sleep(0.1)
        except pymongo.errors.DuplicateKeyError as e:
            self.logger.error(e)

    def run(self):
        self.name = str(hex(threading.current_thread().ident))
        self.logger.info("%s starts" %self.name)
        #main loop
        while self._status:
            self.handle()

        #we are not done yet. handle left over news
        while not self.queue.empty():
            self.logger.info("%d news left to handle in NewsInserter" %self.queue.qsize())
            self.handle()

        self.logger.info("thread exited now...")

class CollectionDivision(object):
    def __init__(self, database, collection, username, password, filename, host, port):
        self.database = database
        self.collectionName = collection
        self.username = username
        self.password = password
        self.filename = filename
        self.host = host
        self.port = int(port)
        self.client = None
        self.db = None
        self.logger = logging.getLogger(self.filename)
        self.childThread = None
        self.useractionId = None
        self.commentId = None
        self.favoriteId = None
        self.specialNewsId = dict()
        self.logger.info("start with configuration database[%s]\
                collection[%s]\
                username[%s]\
                password[%s]\
                host[%s]\
                port[%s]" %(database, collection, username, password, host, port))

    def init(self):
        #self.client = pymongo.MongoClient(self.host, self.port)
        self.client = pymongo.MongoClient('10.117.9.242', 27017)
        self.db = self.client[self.database]
        self.collection = self.db[self.collectionName]
        try:
            #authenticate to obtain super priority
            #ret = self.client.admin.authenticate(self.username, self.password)
            ret = self.db.authenticate('bdtt_ro', 'Chengzi123')
            if not ret:
                self.logger.critical("authentication fails on user %s. Please check your authentication. Exit now..." %self.username)
                eixt(1)
        except pymongo.errors.OperationFailure as e:
            self.logger.critical(e)
            self.logger.critical("authentication fails. Exiting now ...")
            exit(1)

        #check last processed document id. 
        useractionId = None
        commentId = None
        favoriteId = None
        fileName = 'data' + os.sep+ 'useractionId'
        if os.path.exists(fileName):
            with open(fileName) as fp:
                for line in fp:
                    useractionId = int(line.strip())

        fileName = 'data' + os.sep+ 'commentId'
        if os.path.exists(fileName):
            with open(fileName) as fp:
                for line in fp:
                    commentId = int(line.strip())

        fileName = 'data' + os.sep+ 'favoriteId'
        if os.path.exists(fileName):
            with open(fileName) as fp:
                for line in fp:
                    favoriteId = int(line.strip())

        if not os.path.exists('data'):
            os.mkdir('data')

        #init news handler thread
        self.childThread = NewsInserter(self.logger, self.username, self.password,
                    self.host, self.port, self.database)

        #get news data_id from table local_favorites, local_news_comment, local_user_action
        cursor = self.childThread.getSQLClient().getCursor()
        newsIds = getNewsIdFromTable(self.logger, cursor, "local_favorites", favoriteId)
        self.specialNewsId["favorites_"] = newsIds
        newsIds = getNewsIdFromTable(self.logger, cursor, "local_news_comment", commentId)
        self.specialNewsId["comment_"] = newsIds
        newsIds = getNewsIdFromTable(self.logger, cursor, "local_user_action", useractionId)
        self.specialNewsId["useraction_"] = newsIds

        self.logger.debug("Start thread")
        self.childThread.init()
        self.childThread.start()

    def cleanUp(self, sigNo = None):
        #keyword for program terminateion
        self.logger.info("Program is about to exit.")

        #wait for child task done
        leftTask = self.childThread.getQsize()
        self.logger.info("%d tasks in progress" %leftTask)
        while leftTask > 0:
            self.logger.info("%d tasks in progress. Wait for a while before clean up" %leftTask)
            time.sleep(1)
            leftTask = self.childThread.getQsize()

        self.logger.info("Task is done. exit now...")
        self.client.admin.logout()

        #store latest news id 
        if self.useractionId:
            fileName = 'data' + os.sep+ 'useractionId'
            with open(fileName, 'w') as fp:
                self.logger.info('store latest useractionId[%d]' %self.useractionId)
                fp.write("%d" %self.useractionId)

        if self.commentId:
            fileName = 'data' + os.sep+ 'commentId'
            with open(fileName, 'w') as fp:
                self.logger.info('store latest commentId[%d]' %self.commentId)
                fp.write("%d" %self.commentId)

        if self.favoriteId:
            fileName = 'data' + os.sep+ 'favoriteId'
            with open(fileName, 'w') as fp:
                self.logger.info('store latest favoriteId[%d]' %self.favoriteId)
                fp.write("%d" %self.favoriteId)

        self.childThread.terminate()

        self.logger.debug("Joining NewsInserter threads")
        self.childThread.join()
        self.childThread.cleanUp()

        self.logger.debug("Joining MySQL threads")
        self.childThread.getSQLClient().join()

        if sigNo:
            self.logger.info("Caught sinal no = %d" %sigNo)
            exit()

    def getDocuments(self, query = dict()):
        return self.collection.find(query)

    def removeCollections(self):
        for collction in self.db.collection_names(False):
            if collction != 'news':
                self.db.drop_collection(collction)
                self.logger.info("drop collection %s" %collction)

    def run(self):
        tables = ['useraction_', 'comment_', 'favorites_']
        #for table in self.specialNewsId:
        for table in tables:
            newsIds = self.specialNewsId[table]
            ids = newsIds.keys()
            ids.sort()
            for id in ids:
                objectId = newsIds[id]
                self.logger.debug("handle table %s with id %d" %(table, id))
                query = {'_id' : bson.ObjectId(objectId)} 
                self.logger.debug("query news with condition %s" %str(query))
                cursor = self.getDocuments(query)
                try:
                    for doc in cursor:
                        delievered = False
                        while not delievered:
                            try:
                                self.childThread.put(tuple((table, doc)))
                                delievered = True
                            except Queue.Full as e:
                                self.logger.info(e)
                                self.logger.info("thread is full. Wait for a while.")
                                time.sleep(0.1)

                except pymongo.errors.CursorNotFound as e:
                    self.logger.info(e)
                    self.logger.info("re-connecto to server");
                except TypeError as e:
                    self.logger.info(e)
                    self.logger.info("recevied SIGINT when iterate cursor")
                    self.cleanUp(SIGINT)

                if table == 'favorites_':
                    self.favoriteId = id

                if table == 'comment_':
                    self.commentId = id

                if table == 'useraction_':
                    self.useractionId = id

def initOptions():
    usage = "Usage %prog [options]" 
    parser = OptionParser(usage = usage)
    parser.add_option('-d', '--database', dest = 'database', default = 'bdtt',
            help = "database that stores documents")
    parser.add_option('-c', '--collection', dest = 'collection', default = 'news',
            help = "mongo collection that stores documents")
    parser.add_option('-u', '--username', dest = 'username', default = 'admin',
            help = "mongo user to access database")
    parser.add_option('-p', '--password', dest = 'password', default = 'Cqkj10xwdmx!',
            help = "mongo password to access database")
    parser.add_option('-f', '--filename', dest = 'filename', default = 'specialnews',
            help = "log filename")
    parser.add_option('-l', '--loglevel', dest = 'loglevel', default = 'DEBUG',
            choices = ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
            help = "log level")
    parser.add_option('-s', '--server', dest = 'host', default = 'localhost',
            help = "mongo server to operate")
    parser.add_option('-t', '--tcpport', dest = 'port', default = '27017',
            help = "mongo server port")

    return parser.parse_args()

def init():
    options, args = initOptions()

    #create logging directory
    if not os.path.exists("log"):
        os.mkdir('log')

    #backup log file if it reaches maximum size 10M
    logFile = "log" + os.sep + "log." + options.filename
    if os.path.exists(logFile) and os.path.getsize(logFile) > 10*1024*1024:
        backupFile = time.strftime("%Y%m%d%H%M%S", time.localtime(time.time())) + ".tar.gz"
        status = commands.getstatusoutput("cd log&&tar cvzf " + backupFile + " " + os.path.basename(logFile) + "&&cd -")
        os.remove(logFile)
    #init logging
    logging.basicConfig(
            filename = "log/log." + options.filename,
            format = "%(levelname)-10s 0x%(thread)-16x%(asctime)s %(message)s",
            level = logLevelMapping[options.loglevel]
            )
    handler = CollectionDivision(options.database, options.collection, options.username,
            options.password, options.filename, options.host, options.port)
    #register SIGINT handler
    signal(SIGINT, lambda sigNo, frame: handler.cleanUp(sigNo))
    return handler;

def start(handler):
    handler.init()
    handler.run()
    #handler.removeCollections()
    handler.cleanUp()

if __name__ == "__main__":
    handler = init()
    start(handler);

