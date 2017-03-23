#!/bin/env python
# -*- coding: utf-8 -*- 

import os
import time
import sys
import oss2
import uuid
import unittest
from ossManipulator import OssManipulator

class OssManipulatorTest(unittest.TestCase):
    def setUp(self):
        self.handler = OssManipulator()
        self.filename = 'OssManipulatorTest.txt'
        self.file = open(self.filename, 'w')
        self.file.write('OssManipulatorTest\n')
        self.file.close()
        self.size = os.path.getsize('OssManipulatorTest.txt')
        self.folder = 'normal_20990614'
        self.key = ''

    def tearDown(self):
        os.remove(self.filename)
        self.handler.delete_remote_file(self.folder, self.key)

    def test_check_upload_success(self):
        url, key = self.handler.upload_pic_to_oss(self.folder, self.filename)
        self.assertTrue(url.find(self.folder) != -1)
        self.key = key
        self.assertTrue(self.handler.query_remote_file(self.folder, self.key))

    def test_check_upload_with_timestamp_success(self):
        url, key = self.handler.upload_pic_to_oss(time.time(), self.filename)
        self.folder = url.split('/')[-2]
        self.key = key
        self.assertTrue(self.handler.query_remote_file(self.folder, self.key))

    def test_check_upload_with_invalid_timestamp_faiure(self):
        folder = "<span baidu.com 2016-05-15T21:05:00 span>"
        with self.assertRaises(RuntimeError):
            self.handler.upload_pic_to_oss(folder, self.key)
            
    def test_check_upload_with_ISOtime_success(self):
        news_date = '2016-05-15T21:05:00'
        url, key = self.handler.upload_pic_to_oss(news_date, self.filename)
        self.folder = url.split('/')[-2]
        self.key = key
        self.assertTrue(self.handler.query_remote_file(self.folder, self.key))

    def test_check_update_success(self):
        _, key = self.handler.upload_pic_to_oss(self.folder, self.filename)
        new_folder = 'normal_20990615'
        _, self.key = self.handler.update_remote_file(self.folder, key, new_folder)
        self.assertTrue(self.handler.query_remote_file(self.folder, key))
        self.folder = new_folder
        self.assertTrue(self.handler.query_remote_file(self.folder, self.key))

    def test_check_update_with_timestamp_success(self):
        _, key = self.handler.upload_pic_to_oss(self.folder, self.filename)
        new_folder = time.time()
        url, self.key = self.handler.update_remote_file(self.folder, key, new_folder)
        self.assertTrue(self.handler.query_remote_file(self.folder, key))
        self.folder = url.split('/')[-2]
        self.assertTrue(self.handler.query_remote_file(self.folder, self.key))

    def test_check_download_success(self):
        _, self.key = self.handler.upload_pic_to_oss(self.folder, self.filename)
        local_file = 'tmp'
        self.handler.download_remote_file(self.folder, self.key, local_file)
        self.assertTrue(os.path.exists(local_file))
        size = os.path.getsize(local_file)
        os.remove(local_file)
        self.assertEqual(self.size, size)

    def test_check_download_failure(self):
        self.key = str(uuid.uuid1())
        local_file = 'tmp'
        with self.assertRaises(oss2.exceptions.NoSuchKey):
            self.handler.download_remote_file(self.folder, self.key, local_file)

    def test_check_delete_file_success(self):
        _, self.key = self.handler.upload_pic_to_oss(self.folder, self.filename)
        self.handler.delete_remote_file(self.folder, self.key)
        self.assertFalse(self.handler.query_remote_file(self.folder, self.key))

    def test_check_delete_folder_success(self):
        _, self.key = self.handler.upload_pic_to_oss(self.folder, self.filename)
        self.handler.delete_remote_folder(self.folder)
        self.assertFalse(self.handler.query_remote_file(self.folder, self.key))

    def test_check_query_files_success(self):
        _, self.key = self.handler.upload_pic_to_oss(self.folder, self.filename)
        file_list = self.handler.query_remote_files(self.folder)
        self.assertEqual(len(file_list), 1)
        self.assertTrue(file_list[0].split('/')[-1], self.key)

    def test_check_query_files_failure(self):
        file_list = self.handler.query_remote_files(self.folder)
        self.assertEqual(len(file_list), 0)

    def test_check_query_file_success(self):
        _, self.key = self.handler.upload_pic_to_oss(self.folder, self.filename)
        ret = self.handler.query_remote_file(self.folder, self.key)
        self.assertTrue(ret)

    def test_check_query_file_failure(self):
        self.key = str(uuid.uuid1())
        ret = self.handler.query_remote_file(self.folder, self.key)
        self.assertFalse(ret)

if __name__ == '__main__':
    version = sys.version.strip()
    major = version.split('.')[0]
    minor = version.split('.')[1]
    if major <= '2' and minor < '7':
        sys.stderr.write('unittest requires python 2.7 or higer!\n')
        exit(1)
    unittest.main()

