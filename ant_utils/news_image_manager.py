# -*- coding: utf-8 -*-

import os, sys
import time, datetime
from hash64 import hash64
from empty_logger import EmptyLogger
CWD = os.path.dirname(os.path.abspath(__file__))
sys.path.append('%s/../division_utils' % CWD)
from ossManipulator import OssManipulator

class NewsImageManager(object):
    def __init__(self):
        self.handler = OssManipulator()
      
    def save(self, publish_time, content):
        src, oss_key = self.handler.upload_pic_from_content(publish_time, content)
        return (src, oss_key)

