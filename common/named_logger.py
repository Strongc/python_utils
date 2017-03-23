# -*- coding: utf-8 -*-
import logging
from logging.handlers import RotatingFileHandler

class NamedLogger(object):
    def __init__(self, name, file_path=''):
        self.logger = logging.getLogger(name)
        if file_path:
            handler = RotatingFileHandler(
                    file_path, maxBytes=1024*1024*32, backupCount=10)
        else:
            handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter(
                '[%(exname)42s %(levelname)-10s 0x%(thread)-16x %(asctime)-15s] %(message)s'))
        if not self.logger.handlers:
            self.logger.addHandler(handler)
        self.logger.setLevel(logging.INFO)
        self.name = name
        self.extra = {'exname': self.name}

    def debug(self, message):
        self.logger.debug(message, extra=self.extra)

    def info(self, message):
        self.logger.info(message, extra=self.extra)
    
    def warning(self, message):
        self.logger.warning(message, extra=self.extra)
    
    def error(self, message):
        self.logger.error(message, extra=self.extra)
    
    def exception(self, message):
        self.logger.exception(message, extra=self.extra)

