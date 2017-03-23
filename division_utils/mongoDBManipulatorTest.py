#!/bin/env python
# -*- coding: utf-8 -*-

'''
unit test for module mongoDBManipulator
data source record.txt
'''

import time
import pymongo
import bson
import logging
import re
import unittest
import os
import bson
from mongoDBManipulator import MongoDBManipulator

class MongoDBManipulatorTest(unittest.TestCase):
    def setUp(self):
        self.client = pymongo.MongoClient('10.25.8.160', 27017)
        self.db = self.client['test']
        try:
            ret = self.client.admin.authenticate('admin', 'Cqkj10xwdmx!')
            if not ret:
                print "authentication fails on user admin. Please check your authentication. Exit now..."
                exit(1)
        except pymongo.errors.OperationFailure as e:
            print "authentication fails. Exiting now ..."
            exit(1)

        #create logging directory 
        if not os.path.exists("log"):
            os.mkdir('log')

        #init logging
        logging.basicConfig(
                filename = "log/log." + self.__class__.__name__,
                format = "%(levelname)-10s 0x%(thread)-16x%(asctime)s %(message)s",
                level = logging.DEBUG)

        self.logger = logging.getLogger(self.__class__.__name__)
        self.handler = MongoDBManipulator(self.logger, self.db)
        self.news = {}
        with open('record.txt', 'r') as fp:
            for line in fp:
                pos = line.strip().find(':')
                key = line.strip()[0:pos].strip()
                value = line.strip()[pos + 1:].strip()
                self.news[key] = value

        #convert _id
        self.id = bson.ObjectId(self.news['_id'])
        self.news['_id'] = self.id
        self.collection = ''


    def tearDown(self):
        if self.collection in self.db.collection_names(False):
            self.db.drop_collection(self.collection)
        self.client.admin.logout()

    def test_check_news_created_withcollction_success(self):
        self.collection = 'my_collection'
        collection, id = self.handler.create_with_collection(self.collection, self.news)
        self.assertEqual(self.id, id)

    def test_check_news_created_withcollction_failure(self):
        with self.assertRaises(RuntimeError):
            self.handler.create_with_collection(self.collection, self.news)

    def test_check_news_created_withcollction_withoutid_success(self):
        self.collection = 'my_collection'
        self.news.pop('_id')
        collection, id = self.handler.create_with_collection(self.collection, self.news)
        self.assertEqual(self.collection, collection)
        self.assertTrue(id != self.id)

    def test_check_news_createwithid_success(self):
        collection, id = self.handler.create(self.news)
        news_date = self.news['update_time'].split('T')[0].replace('-', '')
        self.collection = 'normal_' + news_date
        self.assertEqual(self.collection, collection)
        self.assertEqual(self.id, id)

    def test_check_news_createwithoutid_success(self):
        self.news.pop('_id')
        collection, id = self.handler.create(self.news)
        news_date = self.news['update_time'].split('T')[0].replace('-', '')
        self.collection = 'normal_' + news_date
        self.assertEqual(self.collection, collection)
        self.assertTrue(self.id != id)

    def test_check_news_createwithouttime_failure(self):
        self.news.pop('update_time')
        with self.assertRaises(KeyError):
            self.handler.create(self.news)

    def test_check_news_createwith_invalidtime_failure(self):
        self.news['update_time'] = ''
        with self.assertRaises(KeyError):
            self.handler.create(self.news)


    def test_check_update_success(self):
        collection, id = self.handler.create(self.news)
        update_time = int(time.time())
        self.collection = 'normal_' + time.strftime('%Y%m%d', time.localtime(update_time))
        ret = self.handler.update(collection, {'_id' : id}, {'update_time' : update_time})
        new_collection, new_id = ret[0]
        self.assertEqual(self.collection, new_collection)
        self.assertEqual(id, new_id)
        self.db.drop_collection(collection)


    def test_check_update_without_time_failure(self):
        with self.assertRaises(KeyError):
            self.handler.update(self.collection, {'_id': self.id}, {'news_title': 'my_title'})

    def test_check_update_without_newsin_failure(self):
        update_time = int(time.time())
        self.collection = 'normal_' + time.strftime('%Y%m%d', time.localtime(update_time))
        self.db.create_collection(self.collection)
        with self.assertRaises(pymongo.errors.OperationFailure):
            self.handler.update(self.collection, {'_id' : self.id}, {'update_time' : update_time})

    def test_check_delete_without_collection_failure(self):
        update_time = int(time.time()) + 86400 * 10
        self.collection = 'normal_' + time.strftime('%Y%m%d', time.localtime(update_time))
        self.db.drop_collection(self.collection)
        with self.assertRaises(pymongo.errors.OperationFailure):
            self.handler.delete(self.collection, {'_id' : self.id})

    def test_check_delete_success(self):
        self.collection, id = self.handler.create(self.news)
        ret = self.handler.delete(self.collection, {'_id' : self.id})
        self.assertEqual(ret.deleted_count, 1)

    def test_check_query_nonews_success(self):
        update_time = int(time.time()) + 86400 * 10
        self.collection = 'normal_' + time.strftime('%Y%m%d', time.localtime(update_time))
        self.db.create_collection(self.collection)
        cursor = self.handler.find(self.collection, {'_id' : self.id})
        self.assertEqual(cursor.count(), 0)

    def test_check_query_news_success(self):
        self.collection, id = self.handler.create(self.news)
        cursor = self.handler.find(self.collection, {'_id' : self.id})
        self.assertEqual(cursor.count(), 1)
        for news in cursor:
            self.assertEqual(news['_id'], self.id)


if __name__ == '__main__':
    unittest.main()
