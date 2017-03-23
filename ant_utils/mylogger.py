# -*- coding: utf-8 -*-

import sys
import logging

class MyLogger(object):
    def __init__(self, myname, file_path=''):
        self.name = '[%16s]  ' % myname
        if file_path:
            logging.basicConfig(
                level = logging.INFO,
                format = '%(levelname)-10s 0x%(thread)-16x %(asctime)-15s %(message)s',
                stream = sys.stderr,
                filename = file_path,
                filemode = 'a'
            )
        else:
            logging.basicConfig(
                level = logging.INFO,
                format = '%(levelname)-10s 0x%(thread)-16x %(asctime)-15s %(message)s',
                stream = sys.stderr
            )
        self.logger = logging
    
    def format_message(self, message):
        if type(message) is unicode:
            return self.name + message.encode('utf8')
        else:
            return self.name + str(message)

    def debug(self, message):
        self.logger.debug(self.format_message(message))

    def info(self, message):
        self.logger.info(self.format_message(message))
    
    def warning(self, message):
        self.logger.warning(self.format_message(message))
    
    def error(self, message):
        self.logger.error(self.format_message(message))
    
    def exception(self, message):
        self.logger.exception(self.format_message(message))

