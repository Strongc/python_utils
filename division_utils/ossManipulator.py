# -*- coding: utf-8 -*- 

import os
import logging
import logging.handlers
import oss2
import time
import uuid
import commands
import threading
import re
from itertools import islice

class OssManipulator(object):
    def __init__(self, oss_endpoint = 'oss-cn-hangzhou-internal.aliyuncs.com',
            bucket_name = 'bdtt-api', oss_key_id = 'RBbUeiN8FHcPc0As',
            oss_key_value = '6Not2pLjT1drEr4imxiCXgieZTpXgs', protocol = 'http://',
            url_head = 'http://img.benditoutiao.com/', logger=None):
        self.oss_endpoint = oss_endpoint
        self.bucket_name = bucket_name
        self.oss_key_id = oss_key_id
        self.oss_key_value = oss_key_value
        self.protocol = protocol
        self.url_head = url_head
        self.separator = '/'
        auth = oss2.Auth(self.oss_key_id, self.oss_key_value)
        self.bucket = oss2.Bucket(auth, self.protocol + self.oss_endpoint, self.bucket_name)
        self.pattern = re.compile(r'^[a-z]+_(19|20)\d+')

        #init logging service
        if not logger:
            if not os.path.exists("log"):
                os.mkdir('log')

            filename = 'log.' + self.__class__.__name__
            log_file = 'log/' + filename
            if os.path.exists(log_file) and os.path.getsize(log_file) > 10*1024*1024:
                backup_file = time.strftime("%Y%m%d%H%M%S", time.localtime(time.time())) + ".tar.gz"
                status = commands.getstatusoutput("cd log&&tar cvzf " + backup_file + " " + os.path.basename(log_file) + "&&cd -")
                os.remove(log_file)

            logging.basicConfig(
                    filename = log_file,
                    format = "%(levelname)-10s 0x%(thread)-16x%(asctime)s %(message)s",
                    level = logging.DEBUG
                    )
            self.logger = logging.getLogger(filename)
        else:
            self.logger = logger

    def _get_folder_name_from_timestamp(self, news_date):
        folder = "{0}".format(news_date)
        try:
            timestamp = int(news_date)
            folder = 'normal_' + time.strftime("%Y%m%d", time.localtime(timestamp))
        except ValueError as e:
            self.logger.info(e) 
            #check again 
            pos = news_date.strip().find(' ')
            if pos != -1:
                folder = 'normal_' + news_date[:pos].replace('-', '').replace('_', '')

            pos = news_date.strip().find('T')
            if pos != -1: 
                #format "2016-05-15T21:05:00"
                folder = 'normal_' + news_date[:pos].replace('-', '').replace('_', '')

        #verify it
        match = self.pattern.search(folder)
        if match is None:
            raise RuntimeError("Invalid timestamp!")
        return folder 

    '''
    @Param folder - remote server folder matching pattern "^[a-z]+_(19|20)\d+" or timestamp or ISO-format date string
    @Param local_file - local file name
    @Retrun tuple((url info of remote file, oss_key, ret))
    @Exception - RuntimeError if timestamp invalid
    '''
    def upload_pic_to_oss(self, folder, local_file):
        folder = self._get_folder_name_from_timestamp(folder)
        oss_key = str(uuid.uuid1())
        remote_file = folder + self.separator + oss_key if folder != '' else oss_key
        ret = self.bucket.put_object_from_file(remote_file, local_file)
        retry = 3
        while retry > 0:
            if ret.status in range(200, 400):
                break
            else:
                ret = self.bucket.put_object_from_file(remote_file, local_file)
                retry = retry - 1
        self.logger.debug('upload picture {0}'.format(remote_file))
        self.logger.info('upload picture with oss_key {0} request id {1}, upload status {2}'.format(oss_key, ret.request_id, ret.status))
        ret = ret.status in range(200, 400)
        return tuple((self.url_head + remote_file, oss_key, ret))
    
    '''
    @Param folder - remote server folder matching pattern "^[a-z]+_(19|20)\d+" or timestamp or ISO-format date string
    @Param local_file - local file name
    @Retrun tuple((url info of remote file, oss_key))
    @Exception - RuntimeError if timestamp invalid
    '''
    def upload_pic_from_content(self, folder, content, fmt=None):
        folder = self._get_folder_name_from_timestamp(folder)
        oss_key = str(uuid.uuid1())
        remote_file = folder + self.separator + oss_key if folder != '' else oss_key
        if fmt:
            remote_file += '.' + fmt
        ret = self.bucket.put_object(remote_file, content)
        self.logger.debug('upload picture {0}'.format(remote_file))
        self.logger.info('upload picture with oss_key {0} request id {1}, upload status {2}'.format(oss_key, ret.request_id, ret.status))
        return tuple((self.url_head + remote_file, oss_key))

    '''
    @Param folder - remote server folder matching pattern "^[a-z]+_(19|20)\d+" or timestamp or ISO-format date string
    @Param remote_file - remote server file, a oss_key
    '''
    def delete_remote_file(self, folder, remote_file):
        folder = self._get_folder_name_from_timestamp(folder)
        filename = folder + self.separator + remote_file if folder != '' else remote_file
        self.logger.debug('delete remote file {0}'.format(filename))
        self.bucket.delete_object(filename)

    '''
    @Param folder - original folder
    @Param remote_file - original file, a oss_key
    @Param new_folder - new folder to store picture matching pattern "^[a-z]+_(19|20)\d+" or timestamp or ISO-format date string
    @Return tuple((url info of remote file, oss_key, ret))
    '''
    def update_remote_file(self, folder, remote_file, new_folder):
        new_folder = self._get_folder_name_from_timestamp(new_folder)

        local_file = 'tmp{0}'.format(hex(threading.current_thread().ident))
        self.download_remote_file(folder, remote_file, local_file)
        #self.delete_remote_file(folder, remote_file)
        self.logger.debug('update {0} from folder {1} to {2}'.format(remote_file, folder, new_folder))
        ret = self.upload_pic_to_oss(new_folder, local_file)
        os.remove(local_file)
        return ret
        
    '''
    @Param folder - remote folder
    @Param remote_file - remote file name, a oss_key
    @Param local_file - local_file name
    @Exception - oss2.exceptions.NoSuchKey if specified key not eixst
    '''
    def download_remote_file(self, folder, remote_file, local_file):
        filename = folder + self.separator + remote_file if folder != '' else remote_file
        self.logger.debug('download file from {0} to {1}'.format(filename, local_file))
        self.bucket.get_object_to_file(filename, local_file)

    '''
    @Param folder - remote folder new folder 
    @Param file - remote file name, a oss_key
    @Exception - RuntimeError if folder = '' and file = ''
    '''
    def delete_remote_file(self, folder, file):
        if folder == '' and file == '':
            raise RuntimeError('You are not allowed to delete root bucket!')
        filename = folder + self.separator + file if folder != '' else file
        self.logger.debug('delete file {0} from direcotry {1}'.format(file, folder))
        self.bucket.delete_object(filename)

    '''
    @Param folder - remote folder
    @Exception - RuntimeError if folder = '' or '/' 
    '''
    def delete_remote_folder(self, folder):
        if folder == '' or folder == '/':
            raise RuntimeError('You are not allowed to delete root bucket!')
        self.logger.debug('delete folder {0}'.format(folder))
        file_list = self.query_remote_files(folder)
        file_list = [item.split('/')[-1] for item in file_list]
        self.logger.debug('still {0} files in {1}'.format(len(file_list), folder))
        while len(file_list) > 0:
            files = file_list[0:100]
            self.delete_batch_files(folder, files)
            file_list = file_list[100:] if len(file_list) > 100 else []
        self.bucket.delete_object(folder)

    '''
    @Param folder - remote folder
    @Param file_list - a list of remote file name such as oss_key
    '''
    def delete_batch_files(self, folder, file_list):
        self.logger.debug('delete {0} files from direcotry {1}'.format(len(file_list), folder))
        key_list = [folder + self.separator + obj for obj in file_list] if folder != '' else file_list
        self.bucket.batch_delete_objects(key_list)

    '''
    @Param prefix - remote folder
    @Return - a list of remote files under folder
    '''
    def query_remote_files(self, prefix):
        if prefix != '' and prefix.find('/') == -1:
            prefix += '/'
        items = []
        for obj in oss2.ObjectIterator(self.bucket, prefix=prefix, delimiter='/'):
            if not obj.is_prefix():
                items.append(prefix + self.separator + obj.key)
        return items

    '''
    @Param prefix - remote folder
    @Param oss_key - remote file name
    @Return - True if file exists else False
    '''
    def query_remote_file(self, prefix, oss_key):
        if prefix != '' and prefix.find('/') == -1:
            prefix += '/'
        oss_key = prefix + oss_key
        self.logger.debug('query file {0} under direcotry {1}'.format(oss_key, prefix))
        ret = False
        for obj in oss2.ObjectIterator(self.bucket, prefix=prefix, delimiter='/'):
            self.logger.debug('query result :directory[{0}] key[{1}]'.format(obj.is_prefix(), obj.key))
            if not obj.is_prefix() and obj.key == oss_key:
                ret = True
        msg = 'exists' if ret else 'does not exist'
        self.logger.info('query file {0} {1} under direcotry {2}'.format(oss_key, msg, prefix))
        return ret

