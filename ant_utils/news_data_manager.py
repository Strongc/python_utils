# -*- coding: utf-8 -*-

import os, sys
import time, datetime
import pymongo
from myexception import MyException
from bson.objectid import ObjectId
from hash64 import hash64
from empty_logger import EmptyLogger
CWD = os.path.dirname(os.path.abspath(__file__))
sys.path.append('%s/../division_utils' % CWD)
from mongoDBManipulator import MongoDBManipulator

MONGODB_HOST = '10.25.2.159'
MONGODB_PORT = 27017
MONGODB_USER = 'admin'
MONGODB_PASSWORD = 'Cqkj10xwdmx!'
MONGODB_DB = 'crawler'
mongodb_client = pymongo.MongoClient(MONGODB_HOST, MONGODB_PORT, maxPoolSize=5)
mongodb_client.admin.authenticate(MONGODB_USER, MONGODB_PASSWORD)

class NewsDataManager(object):
    def __init__(self):
        db = mongodb_client['crawler']
        self.handler = MongoDBManipulator(EmptyLogger(), db)

    def mkid(self, url):
        return ObjectId('%024d' % hash64(url))
      
    def save(self, doc):
        doc_mongo = {
            '_id': self.mkid(doc['url']),
            'title': doc['title'],
            'url': doc['url'],
            'update_time': doc['publish_time'],
            'sections': doc['sections']
        }
        collection_name, data_id = self.handler.create(doc_mongo)
        doc['collection_name'] = collection_name
        doc['data_id'] = str(data_id)
        if not collection_name or not data_id:
            raise MyException('save data failed')
        return True

    def get(self, collection_name, _id):
        cursor = self.handler.find(collection_name, {'_id': ObjectId(_id)})
        docs = [doc for doc in cursor]
        return docs[0] if len(docs) == 1 else None

    def update_images(self, collection_name, _id, replaces):
        data = self.get(collection_name, _id)

        if not data:
            raise MyException('can not get data from mongodb: [%s]' % (_id))

        last_text_idx = 0
        for idx,s in enumerate(data['sections']):
            if s['type'] == 'text':
                last_text_idx = idx
        new_sections = []
        for idx,s in enumerate(data['sections']):
            if s['type'] == 'img':
                if s['src'] in replaces:
                    r = replaces[s['src']]
                    if idx > last_text_idx and r['width'] == r['height']:
                        print 'skip image: [%s]' % r['src']
                        continue
                    new_sections.append(r)
            else:
                new_sections.append(s)

        data['sections'] = new_sections
        collection_name, data_id = self.handler.save(data)
        if not collection_name or not data_id:
            raise MyException('update images failed')
        return True

