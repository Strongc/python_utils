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
        query = "ALTER TABLE %s ADD COLUMN collection_name %s DEFAULT '%s'" %(tableName, u'char(64)', u'')
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
                self.logger.info("add columns to local_news")
                self.addColumnsToTable('local_news')

            if not tableNewsBakOK:
                self.logger.info("add columns to local_news_bak")
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

        query = "SELECT id FROM %s WHERE data_id='%s'" %(tableName, dataId)
        ret = self.cursor.execute(query)
        if ret > 0:
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
            newsToHandle = self.queue.get(timeout = 10)
            #item = self.queue.get(timeout = 10)
            for item in newsToHandle:
                self.queryAndUpdateTable('local_news', item)
                self.queryAndUpdateTable('local_news_bak', item)
        except Queue.Empty as e:
            self.logger.info("Exception is caught due to no news in queue. Continue...")
        except Exception as e:
            self.logger.error(e)

    def run(self):
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
        self.newsInCollection = {}

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

    def checkSpecialNews(self, news):
        prefix = ''
        #handwork news is_handwork_news=1
        if news.has_key('is_handwork_news') and news['is_handwork_news'] == 1:
            prefix = 'handwork_'

        return prefix

    def handleSpecialNews(self, newsId, prefix):
        connectionName = ''
        if len(newsId) < 8:
            self.logger.critical("OjbectId[%s] is invalid!" %newsId)
            return connectionName

        try:
            timestamp = int(newsId[0:8], 16)
            connectionName = self.getCollectionName(timestamp, prefix)
        except ValueError as e:
            self.logger.error(e)
            self.logger.critical("OjbectId[%s] is invalid!" %newsId)

        return connectionName

    def handleNormalNews(self, news):
        connectionName = ''
        #check keyword update_time
        if "update_time" not in news or news['update_time'] is None:
            self.logger.error("news with objectId %s has no key update_time" %(str(news['_id'])))
            return connectionName

        newsDate = news['update_time']
        try:
            timeStamp = int(newsDate)
            connectionName = self.getCollectionName(timeStamp)
        except ValueError as e:
            self.logger.info(e)
            #check again
            pos = newsDate.strip().find(' ')
            if pos != -1:
                #format "2016-05-15 22:17:06.797"
                connectionName = 'normal_' + newsDate[:pos].replace('-', '').replace('_', '')
            pos = newsDate.strip().find('T')
            if pos != -1:
                #format "2016-05-15T21:05:00"
                connectionName = 'normal_' + newsDate[:pos].replace('-', '').replace('_', '')

        return connectionName


    def processBatchNews(self, connection, newsToHandle):
        connectionName = connection
        self.logger.info("insert batchSize[%d] news to collection[%s]" %(len(newsToHandle), connectionName))
        newCollection = self.db[connectionName]
        try:
            t1 = time.time()
            ret = newCollection.insert_many(newsToHandle, ordered = False)
            t2 = time.time()
            self.logger.debug("insert_many takes %.6f seconds" %(t2 - t1))
        except pymongo.errors.DuplicateKeyError as e:
            self.logger.error(e)
        except Exception as e:
            self.logger.error(e)

        newsToMySQL = []
        for news in newsToHandle:
            id = str(news['_id'])
            item = ChangedItem(id, id, connectionName)
            newsToMySQL.append(item)

        #deliever news to child threads handling
        delievered = False
        while not delievered:
            try:
                self.sqlHandler.put(newsToMySQL)
                delievered = True
            except Queue.Full as e:
                self.logger.info(e)
                self.logger.info("threads %s is full. Wait for a while")
                time.sleep(0.1)

    def handle(self):
        doc = None
        try:
            doc = self.getNews()
        except Queue.Empty as e:
            self.logger.info("No News in queue. Return directly")
            return
        
        newsId = str(doc['_id'])
        newCollectionName = ''
        prefix = self.checkSpecialNews(doc)
        if prefix != '':
            newCollectionName = self.handleSpecialNews(newsId, prefix)
        else:
            newCollectionName = self.handleNormalNews(doc)

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

        newsToHandle = []
        if newCollectionName not in self.newsInCollection:
            self.newsInCollection[newCollectionName] = list()
            self.newsInCollection[newCollectionName].append(tuple((time.time(), doc)))
            return

        if len(self.newsInCollection[newCollectionName]) >= 100: #100 is enough? 
            newsToHandle = [record[1] for record in self.newsInCollection[newCollectionName]]
            self.newsInCollection[newCollectionName] = list()
            self.newsInCollection[newCollectionName].append(tuple((time.time(), doc)))
        else:
            self.newsInCollection[newCollectionName].append(tuple((time.time(), doc)))
            return

        self.processBatchNews(newCollectionName, newsToHandle)

    #insert news passed in 3 minutes ago to release memory
    def handleOutdateMsg(self):
        for connection in self.newsInCollection:
            if len(self.newsInCollection[connection]) > 0:
                if time.time() - self.newsInCollection[connection][0][0] > 3 * 60: #3 minutes ago
                    newsToHandle = [record[1] for record in self.newsInCollection[connection]]
                    self.processBatchNews(connection, newsToHandle)
                    self.newsInCollection[connection] = list()

    def run(self):
        self.name = str(hex(threading.current_thread().ident))
        self.logger.info("%s starts" %self.name)
        now = time.time()
        #main loop
        while self._status:
            self.handle()
            if time.time() - now >= 6: #interrupt every 6 seconds to process un-bundle news
                now = time.time()
                self.handleOutdateMsg()

        #we are not done yet. handle left over news
        while not self.queue.empty():
            self.logger.info("%d news left to handle in NewsInserter" %self.queue.qsize())
            self.handle()

        for connection in self.newsInCollection:
            #newsToHandle = self.newsInCollection[connection]
            newsToHandle = [record[1] for record in self.newsInCollection[connection]]
            if len(newsToHandle) == 0:
                continue
            self.processBatchNews(connection, newsToHandle)
            self.newsInCollection[connection] = list()

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
        self.id = None
        self.interruptId = None
        self.childThread = None
        self.logger.info("start with configuration database[%s]\
                collection[%s]\
                username[%s]\
                password[%s]\
                host[%s]\
                port[%s]" %(database, collection, username, password, host, port))

    def init(self):
        self.client = pymongo.MongoClient('10.117.9.242', 27017)
        #self.client = pymongo.MongoClient(self.host, self.port)
        self.db = self.client[self.database]
        self.collection = self.db[self.collectionName]
        try:
            #authenticate to obtain super priority
            ret = self.db.authenticate('bdtt_ro', 'Chengzi123')
            #ret = self.client.admin.authenticate(self.username, self.password)
            if not ret:
                self.logger.critical("authentication fails on user %s. Please check your authentication. Exit now..." %self.username)
                eixt(1)
        except pymongo.errors.OperationFailure as e:
            self.logger.critical(e)
            self.logger.critical("authentication fails. Exiting now ...")
            exit(1)

        #check last processed document id. 
        fileName = 'data' + os.sep + 'latest_id'
        if os.path.exists(fileName):
            with open(fileName) as fp:
                for line in fp:
                    self.id = line.strip()

        if not os.path.exists('data'):
            os.mkdir('data')

        #init news handler thread
        self.childThread = NewsInserter(self.logger, self.username, self.password,
                    self.host, self.port, self.database)

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
        if self.interruptId:
            fileName = 'data' + os.sep + 'latest_id'
            with open(fileName, 'w') as fp:
                newsId = str(self.interruptId)
                self.logger.info('store latest news id[%s]' %newsId)
                fp.write(newsId)

        self.childThread.terminate()

        self.logger.debug("Joining NewsInserter threads")
        self.childThread.join()
        self.childThread.cleanUp()

        self.logger.debug("Joining MySQL threads")
        self.childThread.getSQLClient().join()

        if sigNo:
            self.logger.info("Caught sinal no = %d" %sigNo)
            exit()

    def getDocuments(self, query = dict(), order = pymongo.DESCENDING, limitNo = None, batchSize = 100):
        if limitNo is None:
            return self.collection.find(query).sort('_id', order).batch_size(batchSize)
        else:
            return self.collection.find(query).sort('_id', order).limit(limitNo)

    def removeCollections(self):
        for collction in self.db.collection_names(False):
            if collction != 'news':
                self.db.drop_collection(collction)
                self.logger.info("drop collection %s" %collction)

    def run(self):
        doc = None
        count = 0;
        query = dict()
        if self.id is not None:
            query = {'_id' : {'$gt' : bson.ObjectId(self.id)}}
        self.logger.debug("query news with condition %s" %str(query))
        cursor = self.getDocuments(query, pymongo.ASCENDING, None, 400)
        leftCount = cursor.count()
        currentId = None

        while leftCount > 0:
            self.logger.info("still %d news left to handle" %leftCount)
            try:
                for doc in cursor:
                    delievered = False
                    while not delievered:
                        try:
                            self.childThread.put(doc)
                            delievered = True
                        except Queue.Full as e:
                            self.logger.info(e)
                            self.logger.info("thread is full. Wait for a while.")
                            time.sleep(0.1)

                    currentId = doc['_id']
                    self.interruptId = currentId
                    count = count + 1
                #main loop done. Set condition to False
                leftCount = 0

            except pymongo.errors.CursorNotFound as e:
                self.logger.info(e)
                self.logger.info("re-connecto to server");
                query = {'_id' : {'$gt' : currentId}}
                self.logger.debug("query news with condition %s" %str(query))
                cursor = self.getDocuments(query, pymongo.ASCENDING, None, 400)
                leftCount = cursor.count()

        self.logger.info("%d news divided into collections" %count)

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
    parser.add_option('-f', '--filename', dest = 'filename', default = 'collectionhandle',
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

