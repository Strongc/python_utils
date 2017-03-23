#!/bin/env python
# -*- coding: utf-8 -*- 

import os
import random
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

class MySQLClient(threading.Thread):
    def __init__(self, logger, name, status = True, queueSize = 100,
            host = 'rdsv48e18t98h6331x8d.mysql.rds.aliyuncs.com', port = 3306,
            user = 'bdtt', pwd = 'Chengzi123', db = 'bdtt', charset = 'utf8'):
        threading.Thread.__init__(self)
        self.logger = logger
        self.name = name
        self._status = status
        self.queue = Queue.Queue(queueSize)
        self.host = host
        self.user = user
        self.pwd = pwd
        self.port = port
        self.db = db
        self.charset = charset
        self.conn = pymysql.connect(host = self.host, user = self.user, password = self.pwd,
                database = self.db, port = self.port, charset = self.charset, autocommit = True)
        self.cursor = self.conn.cursor()

    def terminate(self):
        self._status = False

    def put(self, item):
        self.queue.put(item, timeout = 10)

    def handle(self):
        try:
            newsId = self.queue.get(timeout = 10)
            imagId = None
            collection_name = ''
            table = 'local_news'
            query = "SELECT id, collection_name FROM {0} WHERE data_id = '{1}'".format(table, newsId)
            ret = self.cursor.execute(query)
            if ret < 1:
                table = 'local_news_bak'
                query = "SELECT id, collection_name FROM {0} WHERE data_id = '{1}'".format(table, newsId)
                self.cursor.execute(query)
            for row in self.cursor:
                imagId = row[0]
                collection_name = row[1]

            if imagId is None or imagId == '':
                self.logger.error("news {0} is already removed from MySQL".format(newsId))
                return

            if collection_name.find(self.name) == -1:
                self.logger.info("different collection_name : {0} compared with {1}. News [{2}] restored".format(collection_name,
                    self.name, newsId))
                return

            self.logger.debug("news {0} found in table[{1}] with newsId[{2}]".format(imagId, table, newsId))
            #remove images recoreds from local_news_image
            command = "DELETE FROM local_news_image WHERE news_id = {0}".format(imagId)
            ret = self.cursor.execute(command)
            self.logger.info("Execute command '{0}', result is {1}".format(command, ret))

            #delete record from local_news_assist
            command = "DELETE FROM local_news_assist WHERE news_id = {0}".format(imagId)
            ret = self.cursor.execute(command)
            self.logger.info("Execute command '{0}', result is {1}".format(command, ret))

            #delete records from local_news_keyword
            command = "DELETE FROM local_news_keyword WHERE news_id = {0}".format(imagId)
            ret = self.cursor.execute(command)
            self.logger.info("Execute command '{0}', result is {1}".format(command, ret))

            #remove news record from local_news or local_news_bak
            command = "DELETE FROM {0} WHERE id = {1}".format(table, imagId)
            ret = self.cursor.execute(command)
            self.logger.info("Execute command '{0}', result is {1}".format(command, ret))
        except Queue.Empty as e:
            self.logger.info("No message in queue for MySQLClient")
        except Exception as e:
            self.logger.error(e)

    def run(self):
        self.logger.info("MySQL thread starts")
        while self._status:
            self.handle()

        #handle left over tasks
        while not self.queue.empty():
            self.logger.debug("%d news left to handle in MySQLClient" %self.queue.qsize())
            self.handle()

        self.logger.info("MySQL thread exits")

    def cleanUp(self):
        pass
        #self.cursor.close()
        #self.conn.close()

class NewsImagCleaner(threading.Thread):
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
        self.ossHandler = None
        self.client = None
        self.db = None
        self.news = None
        self.imags = None
        self.pattern = re.compile(r'http://img.benditoutiao.com/([\w/\-\.]+)')
        self.mySqlHandler = None

    def init(self, handler, oss):
        self.mySqlHandler = handler
        self.ossHandler = oss
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
        self.queue.put(item, timeout = 10)

    def getQsize(self):
        return self.queue.qsize()

    def terminate(self):
        self._status = False

    def cleanUp(self):
        self.client.admin.logout() 
        self.mySqlHandler.terminate()
        self.mySqlHandler.join()
        self.mySqlHandler.cleanUp()

    def run(self):
        self.logger.debug("NewsImagCleaner thread starts")
        while self._status:
            self.handle()

        #handle left over tasks
        while not self.queue.empty():
            self.logger.debug("%d news left to handle in NewsImagCleaner" %self.queue.qsize())
            self.handle()
        self.logger.debug("NewsImagCleaner exits now...")

    def handle(self):
        try:
            collection, self.news = self.queue.get(timeout = 10)
            self.logger.info('handle news[{0}]'.format(self.news['_id']))
            #step1 find imags in news
            self.findImgInfo()
            #step2 remove imags from oss server
            self.removeImagFromOss()
            #step3 remove news from mongo
            self.deleteNewsFromMongo(collection)

            newsId = str(self.news['_id'])
            delievered = False
            while not delievered:
                try:
                    self.mySqlHandler.put(newsId)
                    delievered = True
                except Queue.Full as e:
                    self.logger.info(e)
                    self.logger.info("thread [0x%x] is full. Wait for a while." %(self.mySqlHandler.ident))
                    time.sleep(0.1)

        except Queue.Empty as e:
            self.logger.info("Exception is caught due to no news in queue. Continue...")
        except Exception as e:
            self.logger.info("Caugth exception")
            self.logger.error(e)

    def findImgInfo(self):
        self.imags = set()
        folder = ''
        for key in self.news:
            if not isinstance(self.news[key], basestring):
                continue

            body = self.news[key]
            if body.find('img.benditoutiao.com') != -1:
                match = self.pattern.findall(body)
                for url in match:
                    folder = ''
                    domain = url.strip().split('/')
                    if len(domain) == 2:
                        folder = domain[0]
                        ossKey = domain[1]
                    else:
                        ossKey = domain[0]
                        folder = ''
                    self.imags.add(tuple((folder, ossKey)))

    def removeImagFromOss(self):
        if len(self.imags) == 0:
            self.logger.info('No imag in news[{0}]'.format(self.news['_id']))
            return
        folder = ''
        for item in self.imags:
            folder = item[0]
            key = item[1]
            try:
                self.ossHandler.delete_remote_file(folder, key)
                self.logger.debug("delete image : {0} from oss folder : {1}".format(key, folder))
            except oss2.exceptions.NoSuchKey as e:
                self.logger.error(e)
            except Exception as e:
                self.logger.error(e)
        self.logger.info("{0} images removed under folder {1} from OSS server".format(len(self.imags), folder))

    def deleteNewsFromMongo(self, collection):
        try:
            ret = self.db[collection].delete_one({'_id': self.news['_id']})
            self.logger.info("{0} news {1} deleted from {2}".format(ret.deleted_count, self.news['_id'], collection))
        except pymongo.errors.OperationFailure as e:
            self.logger.error(e)
        except Exception as e:
            self.logger.error(e)

class ObsoleteNewsImagHandler(object):
    def __init__(self, logger, database, username, password, host, port, month, name):
        self.logger = logger
        self.database = database
        self.username = username
        self.password = password
        self.host = host
        self.port = int(port)
        self.month = int(month)
        self.name = name
        self.client = None
        self.db = None
        self.childThreads = []
        self.ossHandler = None
        self.handled = set()
        self.total = 0
        self.logger.info("start with configuration database[{0}] username[{1}] password[{2}]\
                host[{3}] port[{4}]".format(database, username, password, host, port))

    def init(self):
        self.client = pymongo.MongoClient(self.host, self.port)
        self.db = self.client[self.database]
        try:
            ret = self.client.admin.authenticate(self.username, self.password)
            if not ret:
                self.logger.critical("authentication fails on user %s. Please check your authentication. Exit now..." %self.username)
                exit(1)
        except pymongo.errors.OperationFailure as e:
            self.logger.critical(e)
            self.logger.critical("authentication fails. Exiting now ...")
            exit(1)

        self.ossHandler = OssManipulator(oss_endpoint = "oss-cn-hangzhou-internal.aliyuncs.com")
        mysql = MySQLClient(self.logger, self.name)
        for i in range(0, 8):
            childThread = NewsImagCleaner(self.logger, self.username, self.password,
                self.host, self.port, self.database)
            childThread.init(mysql, self.ossHandler)
            childThread.start()
            self.childThreads.append(childThread)
        mysql.start()

    def filter(self, collection):
        now = time.strftime('{0}_%Y%m%d'.format(self.name), time.localtime(time.time()-self.month*30*86400)) 
        return collection.find(self.name) != -1 and collection < now

    def run(self):
        collections = self.db.collection_names(False)
        self.logger.debug("There are {0} collections in DB {1}".format(len(collections), self.database))
        collectionsToHandle = [collection for collection in collections if self.filter(collection)]
        self.logger.debug("There are {0} collections to clean up in DB {1}".format(len(collectionsToHandle), self.database))

        id = None
        for collection in collectionsToHandle:
            id = None
            self.logger.info("start to clean news in collection {0}".format(collection))
            query = {}
            cursor = self.db[collection].find(query).sort('_id', pymongo.ASCENDING).batch_size(400)
            leftCount = cursor.count()
            self.total += leftCount
            self.logger.info("{0} news to remove in collection {1}".format(leftCount, collection))
            if leftCount == 0:
                self.logger.info("drop empty collection {0} from mongo directly".format(collection))
                self.handled.add(collection)
                continue

            while leftCount > 0:
                try:
                    for doc in cursor:
                        id = doc['_id']
                        delievered = False
                        while not delievered:
                            try:
                                #no = random.randint(0, 7)
                                child = min(self.childThreads, key=lambda x:x.getQsize())
                                #self.childThreads[no].put(tuple((collection, doc)))
                                child.put(tuple((collection, doc)))
                                delievered = True
                            except Queue.Full as e:
                                self.logger.info(e)
                                self.logger.info("thread [0x%x] is full. Wait for a while." %(child.ident))
                                time.sleep(0.1)
                    leftCount = 0
                except pymongo.errors.CursorNotFound as e:
                    self.logger.info(e)
                    query = {} if id is None else {'_id' : {'$gt' : id}}
                    cursor = self.db[collection].find(query).sort('_id', pymongo.ASCENDING).batch_size(400)
                    leftCount = cursor.count()
                except TypeError as e:
                    self.logger.info(e)
                    self.logger.info("recevied SIGINT when iterate cursor")
                    self.cleanUp(SIGINT)

            self.handled.add(collection)

    def _join_and_clean(self):
        for child in self.childThreads:
            if (not child.is_alive()) or (child.getQsize() == 0):
                child.terminate()
                child.join()

        self.childThreads = [child for child in self.childThreads if child.is_alive()]

    def cleanUp(self, sigNo = None):
        self.logger.info("{0} news removed in this run".format(self.total))
        self.logger.info("Program is about to exit.")

        #wait for child task done
        last_child = self.childThreads[0]
        leftTask = sum([childThread.getQsize() for childThread in self.childThreads], 0)
        self.logger.info("%d tasks in progress" %leftTask)
        while leftTask > 0:
            self.logger.info("{0} tasks in [{1}] child threads. Wait for a while before clean up".format(leftTask, len(self.childThreads)))
            time.sleep(1)
            self._join_and_clean()
            leftTask = sum([childThread.getQsize() for childThread in self.childThreads], 0)

        self.logger.info("do clean up stuff")
        for collection in self.handled:
            try:
                self.db.drop_collection(collection)
                self.logger.info("drop collection : {0} from mongodb".format(collection))
            except pymongo.errors.OperationFailure as e:
                self.logger.error(e)
            except Exception as e:
                self.logger.error(e)

            #try:
            #    self.ossHandler.delete_remote_folder(collection)
            #    self.logger.info("delete OSS foler : {0}".format(collection))
            #except oss2.exceptions.NoSuchKey as e:
            #    self.logger.error(e)

        self.client.admin.logout()

        for childThread in self.childThreads:
            childThread.terminate()
            self.logger.debug("Joining NewsImagCleaner threads")
            childThread.join()
        last_child.cleanUp()

        if sigNo:
            self.logger.info("Caught sinal no = {0}".format(sigNo))
            exit()

        self.logger.info("Task is done. exit now...")

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
    parser.add_option('-m', '--month', dest = 'month', default = '2',
            help = 'How many months before that the news are clean up')
    parser.add_option('-n', '--name', dest = 'name', default = 'normal',
            choices = ['normal', 'useraction', 'comment', 'favorites'],
            help = 'the prefix name of mongoDB collection to clean up')

    return parser.parse_args()

def init():
    options, args = initOptions()

    #create logging directory
    if not os.path.exists("log"):
        os.mkdir('log')

    #backup log file if it reaches maximum size 10M
    filename = "log.obsoleteNewsImagHandler"
    logFile = "log" + os.sep + filename
    #init logging
    logHandler = logging.handlers.RotatingFileHandler(logFile, maxBytes=100 * 1024 * 1024, backupCount=5)
    format = "%(levelname)-10s 0x%(thread)-16x%(asctime)s %(message)s"
    formatter = logging.Formatter(format)
    logHandler.setFormatter(formatter)
    logger = logging.getLogger(filename)
    logger.setLevel(logLevelMapping[options.loglevel])
    logger.addHandler(logHandler)
    handler = ObsoleteNewsImagHandler(logger, options.database, options.username, options.password,
            options.host, options.port, options.month, options.name)
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

