#!/bin/env python
# -*- coding: utf-8 -*- 

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
from signal import signal, SIGINT
import pymysql
import pymongo
import pickle


logLevelMapping = {
        "DEBUG" : logging.DEBUG,
        "INFO"  : logging.INFO,
        "WARNING" : logging.WARNING,
        "ERROR" : logging.ERROR,
        "CRITICAL" : logging.CRITICAL
        }

class ShardRunner(object):
    def __init__(self, username, password, host, port, database, filename):
        self.username = username
        self.password = password
        self.host = host
        self.port = int(port)
        self.database = database
        self.logger = logging.getLogger(filename)
        self.client = None
        self.db = None

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

    def handle(self, newCollectionName):
        self.logger.debug('handle collection {0}'.format(newCollectionName))
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

    def run(self):
        collections = self.db.collection_names()
        suffix = time.strftime("%Y%m%d", time.localtime(time.time()))
        toHandle = [collection for collection in collections if collection.find(suffix) != -1]
        self.logger.info('{0} collections to sharding in this run'.format(len(toHandle)))
        #main loop
        for collection in toHandle:
            self.handle(collection)

        self.logger.info("exited now...")

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
    parser.add_option('-f', '--filename', dest = 'filename', default = 'sharding',
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
    handler = ShardRunner(options.username, options.password, options.host, options.port,
            options.database, options.filename)
    return handler;

def start(handler):
    handler.init()
    handler.run()

if __name__ == "__main__":
    handler = init()
    start(handler);

