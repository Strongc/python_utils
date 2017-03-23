# -*- coding: utf-8 -*-

import os, sys
import time, datetime
import pymongo
from hash64 import hash64
from ant_exception import *
form empty_logger import EmptyLogger
from bson.objectid import ObjectId
CWD = os.path.dirname(os.path.abspath(__file__))
sys.path.append('%s/../division_utils' % CWD)
from mongoDBManipulator import MongoDBManipulator

class SaveNewsData(object):
    def __init__(self):
        client = pymongo.MongoClient('10.25.8.160', 27017)
        client.admin.authenticate('admin', 'Cqkj10xwdmx!')
        db = client['test']
        self.handler = MongoDBManipulator(EmptyLogger(), db)
      
    def save(self, doc):
        doc_mongo = {
            'title': doc['title'],
            'url': doc['url'],
            'update_time': doc['publish_time'],
            'sections': doc['sections']
        }
        collection_name, data_id = self.handler.create(doc_mongo)
        doc['collection_name'] = collection_name
        doc['data_id'] = str(data_id)
        return collection_name, data_id

    def get(self, collection_name, _id):
        cursor = self.handler.find(collection_name, {'_id': ObjectId(_id)})
        docs = [doc for doc in cursor]
        return docs[0] if len(docs) == 1 else None

