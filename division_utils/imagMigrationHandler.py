#!/bin/env python
# -*- coding: utf-8 -*- 

import os
import re
import logging
import logging.handlers
import oss2
import uuid
import bson
import threading
import Queue
import time
import pymongo
import pymysql
import random
from optparse import OptionParser
from signal import signal, SIGINT
from ossManipulator import OssManipulator

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

    def init(self):
        pass

    def getCursor(self):
        #should add a mutex
        return self.cursor

    def terminate(self):
        self._status = False

    def put(self, item):
        self.queue.put(item, timeout = 10)

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
            item = self.queue.get(timeout = 30)
            self.queue.task_done() 
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

class MySQLClient(threading.Thread):
    def __init__(self, logger, parent_sem, child_sem, owner, status = True, queueSize = 100,
            host = 'rdsv48e18t98h6331x8d.mysql.rds.aliyuncs.com', port = 3306,
            user = 'bdtt', pwd = 'Chengzi123', db = 'bdtt', charset = 'utf8'):
        threading.Thread.__init__(self)
        self.logger = logger
        self.parent_sem = parent_sem
        self.child_sem = child_sem
        self.owner = owner
        self._status = status
        self.queue = Queue.Queue(queueSize)
        self.host = host
        self.user = user
        self.pwd = pwd
        self.port = port
        self.db = db
        self.charset = charset
        self.timeout = 0
        self.conn = pymysql.connect(host = self.host, user = self.user, password = self.pwd,
                database = self.db, port = self.port, charset = self.charset, autocommit = True)
        self.cursor = self.conn.cursor()

    def getNewsIdFromTable(self, tableName, id = None):
        newsIds = dict()
        extra = ''
        if id:
            extra = " AND b.id > {0} ".format(id)

        action = ''
        if tableName == "local_user_action":
            action = " AND b.action in (10, 40) "
        query1 = "SELECT b.id, b.news_id, a.new_data_id, a.collection_name FROM local_news a, %s b WHERE b.news_id = a.id %s %s GROUP BY a.data_id" %(tableName, extra, action)
        query2 = "SELECT b.id, b.news_id, a.new_data_id, a.collection_name FROM local_news_bak a, %s b WHERE b.news_id = a.id %s %s GROUP BY a.data_id" %(tableName, extra, action)
        self.logger.info("Execute SQL query %s" %query1)
        self.logger.info("Execute SQL query %s" %query2)

        try:
            self.cursor.execute(query1)
            for row in self.cursor:
                newsIds[row[0]] = tuple((row[1], row[2], row[3]))

            self.cursor.execute(query2)
            for row in self.cursor:
                newsIds[row[0]] = tuple((row[1], row[2], row[3]))
        except Exception as e:
            self.logger.error(e)

        self.logger.debug("Table %s has unique %d news data_id" %(tableName, len(newsIds)))
        return newsIds

    def terminate(self):
        self._status = False

    def put(self, item):
        self.queue.put(item, timeout = 10)

    def handle(self):
        try:
            newsId, imags = self.queue.get(timeout = 10)
            self.queue.task_done() 
            query = 'SELECT id, imgUrl FROM local_news_image WHERE news_id = {0}'.format(newsId)
            ret = self.cursor.execute(query)
            self.logger.info("Execute SQL query [{0}]. {1} records matched".format(query, ret))
            imgUrls = dict()
            for row in self.cursor:
                imgUrls[row[0]] = row[1]

            if len(imgUrls) == 0:
                self.logger.info("No imgUrl found in table local_news_image for news_id = {0}".format(newsId))
                return

            for imgId in imgUrls:
                imgUrl = imgUrls[imgId]
                for change in imags:
                    original = change[0]
                    target = change[1]
                    if imgUrl.find(original) != -1:
                        newUrl = imgUrl.replace(original, target)
                        command = "UPDATE local_news_image SET imgUrl='{0}' WHERE id={1}".format(newUrl, imgId)
                        self.logger.debug('Execute SQL command [{0}]'.format(command))
                        try:
                            self.cursor.execute(command)
                        except Exception as e:
                            self.logger.error(e)
                        break
        except Queue.Empty as e:
            self.logger.info("No message in queue for MySQLClient")
            self.timeout += 1
            if self.timeout >= 3 and self.owner.get_tasks() == 0:
                self.logger.info("local_news_image updates done for current stage. Notify of dispatcher for next stage")
                self.parent_sem.release()
                self.logger.info("Wait signal from dispatcher")
                self.child_sem.acquire()
                self.logger.info("Receive signal from dispatcher. Continue")
                self.timeout = 0
            elif self.timeout >= 60 and self._status:
                self.logger.info("Wait for 10 minutes, still block. Forced to notify of dispatcher for next stage")
                self.parent_sem.release()
                self.logger.info("Wait signal from dispatcher")
                self.child_sem.acquire()
                self.logger.info("Receive signal from dispatcher. Continue") 
                self.timeout = 0
            else:
                self.logger.info("timeout {0} times. Tasks : {1}".format(self.timeout, self.owner.get_tasks()))
        except Exception as e:
            self.logger.error(e)

    def run(self):
        self.logger.info("MySQL thread starts")
        while self._status:
            self.handle()

        #handle left over tasks
        while (not self.queue.empty()) or (self.owner.get_tasks() > 0):
            self.logger.debug("%d news left to handle in MySQLClient" %self.queue.qsize())
            self.handle()

        self.logger.info("MySQL thread exits")

    def cleanUp(self):
        pass
        #self.cursor.close()
        #self.conn.close()

class NewsImagUpdateHandler(threading.Thread):
    def __init__(self, logger, username, password, host, port, database, status = True, queueSize = 100):
        threading.Thread.__init__(self)
        self.logger = logger
        self.username = username
        self.password = password
        self.host = host
        self.port = port
        self.database = database
        self._status = status
        self.queue = Queue.Queue(queueSize)
        self.ossHandler = OssManipulator(oss_endpoint = "oss-cn-hangzhou-internal.aliyuncs.com")
        self.client = None
        self.db = None
        self.news = None
        self.imags = None
        self.collection = None
        self.pattern = re.compile(r'http://img.benditoutiao.com/([\w/\-\.]+)')
        self.collectionPattern = re.compile('^[a-z]+_(19|20)\d+')
        self.mySqlHandler = None
        self.updateHandler = None

    def init(self, handler, updater):
        self.mySqlHandler = handler
        self.updateHandler = updater
        self.client = pymongo.MongoClient(self.host, self.port)
        self.db = self.client[self.database]
        try:
            ret = self.client.admin.authenticate(self.username, self.password)
            if not ret:
                self.logger.critical("authentication fails on user %s. Please check your authentication. Exit now" %self.username)
                exit(1)
        except pymongo.errors.OperationFailure as e:
            self.logger.critical(e)
            self.logger.critical("authentication fails. Exiting now ...")

    def put(self, item):
        self.queue.put(item, timeout = 30)

    def getQsize(self):
        return self.queue.qsize()

    def terminate(self):
        self._status = False

    def cleanUp(self):
        self.updateHandler.terminate()
        self.updateHandler.join() 
        self.mySqlHandler.terminate()
        self.mySqlHandler.join()
        self.mySqlHandler.cleanUp()

    def run(self):
        self.logger.debug("NewsImagUpdateHandler thread starts")
        while self._status:
            self.handle()

        #handle left over tasks
        while not self.queue.empty():
            self.logger.debug("%d news left to handle in NewsImagUpdateHandler" %self.queue.qsize())
            self.handle()
        self.logger.debug("NewsImagUpdateHandler exits now...")

    def getCollectionName(self, timeStamp, prefix = "normal_"):
        dateFormat = prefix + time.strftime("%Y%m%d", time.localtime(timeStamp))
        return dateFormat

    def runShardCommand(self, newCollectionName):
        if newCollectionName == '':
            self.logger.error("empty collection. No sharding")
            return

        try:
            ret = self.db.client.admin.command("shardCollection",
                    "%s.%s"%(self.db.name, newCollectionName), key={"_id" : pymongo.HASHED})
            if u'ok' not in ret or ret[u'ok'] != 1.0:
                self.logger.error("Fail to create shard on collection[%s]" %newCollectionName)
        except pymongo.errors.OperationFailure as e:
            self.logger.error(e)
            errMsg = str(e)
            if errMsg.find("create an index") != -1: 
                try:
                    self.logger.debug("create indexes on collection[%s]" %newCollectionName)
                    self.db[newCollectionName].create_index([("_id", pymongo.HASHED)])
                    self.db.client.admin.command("shardCollection",
                            "%s.%s"%(self.db.name, newCollectionName), key={"_id" : pymongo.HASHED})
                except pymongo.errors.OperationFailure as e:
                    self.logger.error(e)
                except Exception as e:
                    self.logger.error(e)

    def handle(self):
        try:
            newsId, prefix, self.news = self.queue.get(timeout = 10)
            self.queue.task_done() 
            id = str(self.news['_id'])
            self.logger.info('handle news[{0}]'.format(id))
            newCollectionName = ''
            if len(id) < 8:
                self.logger.critical("OjbectId[%s] is invalid!" %id) 
                return

            try:
                timestamp = int(id[0:8], 16)
                newCollectionName = self.getCollectionName(timestamp, prefix)
            except ValueError as e:
                self.logger.error(e) 
                self.logger.critical("OjbectId[%s] is invalid!" %id)
                return

            match =  self.collectionPattern.search(newCollectionName)
            if match is None:
                self.logger.error("news with objectId[%s] is not clarified by newCollectionName %s" %(id, newCollectionName))
                return

            if newCollectionName not in self.db.collection_names():
                self.logger.info("Collection %s not in database from news id[%s]" %(newCollectionName, id))
                #add collection
                self.logger.info("Collection %s will be added into database" %newCollectionName)
                try:
                    self.db.create_collection(newCollectionName)
                except pymongo.errors.OperationFailure as e:
                    self.logger.info(e)
                    return
                except pymongo.errors.CollectionInvalid as e:
                    self.logger.info(e)
                    return
                self.runShardCommand(newCollectionName)
            self.collection = newCollectionName

            #step1 find imags in news
            self.findImgInfo()
            #step2 update oss info
            newImags = self.updateOssInfo()
            #step3 update mongoDB
            self.updateNews2Mongo()

            #update url link info in MySQL
            if len(newImags) > 0:
                delievered = False
                while not delievered:
                    try:
                        self.mySqlHandler.put(tuple((newsId, newImags)))
                        delievered = True
                    except Queue.Full as e:
                        self.logger.info(e)
                        self.logger.info("thread is full. Wait for a while.")
                        time.sleep(0.1)

            #update collection info in MySQL
            item = ChangedItem(id, id, newCollectionName)
            delievered = False
            while not delievered:
                try:
                    self.updateHandler.put(item)
                    delievered = True
                except Queue.Full as e:
                    self.logger.info("threads %s is full. Wait for a while")   
                    time.sleep(0.1)

        except Queue.Empty as e:
            self.logger.info("Exception is caught due to {0} news in queue. Continue...".format(self.getQsize()))
        except Exception as e:
            self.logger.info("Caugth exception : {0}".format(type(e)))
            self.logger.error(e)

    def findImgInfo(self):
        self.imags = set()
        for key in self.news:
            if not isinstance(self.news[key], str) and not isinstance(self.news[key], unicode):
                continue

            body = self.news[key]
            if body.find('img.benditoutiao.com') != -1:
                match = self.pattern.findall(body)
                for url in match:
                    domain = url.strip().split('/')
                    folder = ''
                    if len(domain) == 2:
                        folder = domain[0]
                        ossKey = domain[1]
                    else:
                        ossKey = domain[0]
                    self.imags.add(tuple((folder, ossKey)))

    def updateOssInfo(self):
        newImags = set()
        if len(self.imags) == 0:
            self.logger.info('No imag in news[{0}]'.format(self.news['_id']))
            return newImags

        for imag in self.imags:
            try:
                suffix = ''
                if imag[1].find('.') != -1:
                    suffix = imag[1].split('.')[-1]
                url, ossKey, ret = self.ossHandler.update_remote_file(imag[0], imag[1], self.collection, suffix)
            except oss2.exceptions.NoSuchKey as e:
                self.logger.error(e)
                continue

            original = imag[0] + '/' + imag[1] if imag[0] != '' else imag[1]
            #target = self.collection + '/' + ossKey
            target = ossKey
            newImags.add(tuple((original, target)))

            for key in self.news:
                if not isinstance(self.news[key], str) and not isinstance(self.news[key], unicode): 
                    continue

                if self.news[key].find('img.benditoutiao.com') != -1:
                    self.logger.debug('imag location updated from {0} to {1} for news {2} in section[{3}]'.format(original, target, self.news['_id'], key))
                    self.news[key] = self.news[key].replace(original, target)

        return newImags

    def updateNews2Mongo(self):
        newCollection = self.db[self.collection]
        try:
            #newCollection.save(self.news)
            newCollection.delete_one({"_id":self.news["_id"]})
            newCollection.insert_one(self.news)
            self.logger.info("news {0} saved to {1}".format(self.news['_id'], self.collection))
        except pymongo.errors.OperationFailure as e:
            self.logger.error(e)
        except pymongo.errors.DuplicateKeyError as e:
            self.logger.error(e)
        except Exception as e:
            self.logger.error(e)

class ImagMigrationHandler(object):
    def __init__(self, logger, database, username, password, host, port):
        self.logger = logger
        self.database = database
        self.username = username
        self.password = password
        self.host = host
        self.port = int(port)
        self.client = None
        self.db = None
        self.childThreads = list()
        self.useractionId = None
        self.commentId = None
        self.favoriteId = None
        self.parent_sem = threading.Semaphore(0)
        self.child_sem = threading.Semaphore(0)
        self.mysql = None
        self.logger.info("start with configuration database[{0}] username[{1}] password[{2}]\
                host[{3}] port[{4}]".format(database, username, password, host, port))

    def init(self):
        self.client = pymongo.MongoClient(self.host, self.port)
        self.db = self.client[self.database]
        try:
            ret = self.client.admin.authenticate(self.username, self.password)
            if not ret:
                self.logger.critical("authentication fails on user %s. Please check your authentication. Exit now..." %self.username)
                eixt(1)
        except pymongo.errors.OperationFailure as e:
            self.logger.critical(e)
            self.logger.critical("authentication fails. Exiting now ...")
            exit(1)

        #check last processed document id.
        fileName = 'data' + os.sep+ 'imagOfuseractionId'
        if os.path.exists(fileName):
            with open(fileName) as fp:
                for line in fp:
                    self.useractionId = line.strip()

        fileName = 'data' + os.sep+ 'imagOfcommentId'
        if os.path.exists(fileName):
            with open(fileName) as fp:
                for line in fp:
                    self.commentId = line.strip()

        fileName = 'data' + os.sep+ 'imagOffavoriteId'
        if os.path.exists(fileName):
            with open(fileName) as fp:
                for line in fp:
                    self.favoriteId = line.strip()

        if not os.path.exists('data'):
            os.mkdir('data')

        #query news info
        mysql = MySQLClient(self.logger, self.parent_sem, self.child_sem, self)
        updateHandler = MySQLHandler(self.logger)
        for i in range(0, 8):
            childThread = NewsImagUpdateHandler(self.logger, self.username, self.password,
                self.host, self.port, self.database)
            childThread.init(mysql, updateHandler)
            childThread.start()
            self.childThreads.append(childThread)

        mysql.start()
        updateHandler.start()
        self.mysql = mysql

    def run(self):
        tables = {'useraction_':'local_user_action', 'comment_':'local_news_comment', 'favorites_':'local_favorites'}
        attributes = {'useraction_':'useractionId', 'comment_':'commentId', 'favorites_':'favoriteId'}
        orders = ['useraction_', 'comment_', 'favorites_']
        #for table in self.specialNewsId:
        for table in orders:
            #newsIds = self.specialNewsId[table]
            self.logger.info("Wait for signal from child to next stage")
            self.parent_sem.acquire()
            newsIds = self.mysql.getNewsIdFromTable(tables[table], getattr(self, attributes[table]))
            self.child_sem.release()
            self.logger.info("Notify child to next stage")
            ids = newsIds.keys()
            ids.sort()
            for id in ids:
                newsId, objectId, collection = newsIds[id]
                self.logger.debug("handle table {0} with id {1} objectid {2} in collection {3}".format(table, id, objectId, collection))
                if collection not in self.db.collection_names():
                    self.logger.critical('collection {0} not in Mongo DB!'.format(collection))
                    continue

                if collection.find('handwork') != -1:
                    self.logger.info("handwork news stored consisitent. No need to re-store, ignore newsId : {0}".format(newsId))
                    continue

                if collection == '' or objectId == '':
                    self.logger.critical("collection={0}, objectId={2}".format(collection, objectId))
                    continue

                try:
                    query = {'_id' : bson.ObjectId(objectId)}
                except bson.errors.InvalidId as e:
                    self.logger.critical(e)
                    continue
                except Exception as e:
                    self.logger.critical(e)
                    continue

                cursor = self.db[collection].find(query)
                try:
                    for doc in cursor:
                        delievered = False
                        while not delievered:
                            try:
                                #no = random.randint(0, 3)
                                child = min(self.childThreads, key=lambda x:x.getQsize())
                                child.put((newsId, table, doc))
                                #self.childThreads[no].put(tuple((newsId, table, doc)))
                                delievered = True
                            except Queue.Full as e:
                                self.logger.info(e)
                                self.logger.info("thread is full. Wait for a while.")
                                time.sleep(0.1)
                except pymongo.errors.CursorNotFound as e:
                    self.logger.info(e)
                except TypeError as e:
                    self.logger.info(e)
                    self.logger.info("recevied SIGINT when iterate cursor")
                    self.cleanUp(SIGINT)

                setattr(self, attributes[table], id)

    def get_tasks(self):
        return sum([childThread.getQsize() for childThread in self.childThreads], 0)

    def _join_and_clean(self):
        for child in self.childThreads:
            if (not child.is_alive()) or (child.getQsize() == 0):
                child.terminate()
                child.join()

        self.childThreads = [child for child in self.childThreads if child.is_alive()]

    def cleanUp(self, sigNo = None):
        self.logger.info("Program is about to exit.")
        self.child_sem.release()
        self.mysql.terminate()

        #wait for child task done
        last_child = self.childThreads[0]
        leftTask = self.get_tasks()
        self.logger.info("%d tasks in progress" %leftTask)
        while leftTask > 0:
            self.logger.info("{0} tasks in [{1}] child threads. Wait for a while before clean up".format(leftTask, len(self.childThreads)))
            time.sleep(1)
            self._join_and_clean()
            leftTask = self.get_tasks()

        self.logger.info("Task is done. exit now...")
        self.client.admin.logout()

        if self.useractionId:
            fileName = 'data' + os.sep+ 'imagOfuseractionId'
            with open(fileName, 'w') as fp:
                self.logger.info('store latest useractionId[{0}]'.format(self.useractionId))
                fp.write("{0}".format(self.useractionId))

        if self.commentId:
            fileName = 'data' + os.sep+ 'imagOfcommentId'
            with open(fileName, 'w') as fp:
                self.logger.info('store latest commentId[{0}]'.format(self.commentId))
                fp.write("{0}".format(self.commentId))

        if self.favoriteId:
            fileName = 'data' + os.sep+ 'imagOffavoriteId'
            with open(fileName, 'w') as fp:
                self.logger.info('store latest favoriteId[{0}]'.format(self.favoriteId))
                fp.write("{0}".format(self.favoriteId))

        for childThread in self.childThreads:
            childThread.terminate()
            self.logger.debug("Joining NewsImagUpdateHandler threads")
            childThread.join()

        last_child.cleanUp()
        if sigNo:
            self.logger.info("Caught sinal no = {0}".format(sigNo))
            exit()

def initOptions():
    usage = "Usage %prog [options]" 
    parser = OptionParser(usage = usage)
    parser.add_option('-d', '--database', dest = 'database', default = 'bdtt',
            help = "database that stores documents")
    parser.add_option('-u', '--username', dest = 'username', default = 'admin',
            help = "mongo user to access database")
    parser.add_option('-p', '--password', dest = 'password', default = 'Cqkj10xwdmx!',
            help = "mongo password to access database")
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
    filename = "log.imagMigrationHandler"
    logFile = "log" + os.sep + filename
    #init logging
    logHandler = logging.handlers.RotatingFileHandler(logFile, maxBytes=100 * 1024 * 1024, backupCount=5)
    format = "%(levelname)-10s 0x%(thread)-16x%(asctime)s %(message)s"
    formatter = logging.Formatter(format)
    logHandler.setFormatter(formatter)
    logger = logging.getLogger(filename)
    logger.setLevel(logLevelMapping[options.loglevel])
    logger.addHandler(logHandler)
    handler = ImagMigrationHandler(logger, options.database, options.username,
            options.password, options.host, options.port)
    #register SIGINT handler
    signal(SIGINT, lambda sigNo, frame: handler.cleanUp(sigNo))
    return handler;

def start(handler):
    handler.init()
    handler.run()
    handler.cleanUp()

if __name__ == "__main__":
    handler = init()
    start(handler);

