#!/bin/env python
# -*- coding: utf-8 -*- 

import commands
import json
import logging
import os
import sys
import re

def check_link_url(link_url, logger):
    #filter '\' character
    logger.info('Got raw link url : {0}'.format(link_url))
    link_url = link_url.replace('\\', '')
    msg = ""
    command = "curl -i -XGET '{0}'".format(link_url)
    ret, out = commands.getstatusoutput(command)
    logger.debug('response returns code {0} for link_url : {1}'.format(ret, link_url))
    if ret != 0:
        msg = "Fails to execute request for url : {0}".format(link_url)
    else:
        pattern = re.compile(r'HTTP/1.1[ \t]*(\d+)')
        mat = pattern.search(out)
        if mat:
            code = int(mat.group(1))
            if 200 <= code < 300:
                msg = 'response is OK for link_url : {0}'.format(link_url)
                logger.info(msg)
            else:
                msg = 'response is NOK with code {0} for link_url : {1}'.format(code, link_url)
                logger.info(msg)
                ret = code
        else:
            msg = 'No response for link_url : {0}'.format(link_url)
            ret = 404
            logger.error(msg)
    return (ret, msg)

def check_functionality(url, logger):
    msg = ''
    command = "curl -XPOST '{0}'".format(url)
    logger.debug('check function point with url : {0}'.format(command))
    ret, out = commands.getstatusoutput(command)
    if ret != 0:
        msg = "Fails to check function point with url : {0}".format(command)
        logger.info(msg)
        return (ret, msg)
    else:
        logger.debug("check return code")
        pos = out.find('{')
        obj = out[pos:]
        try:
            opcode = json.loads(obj)
        except ValueError as e:
            msg = "request does not return valid JSON object!"
            logger.error(msg)
            return (1, msg)
        if opcode[u'code'] != 1 and opcode[u'msg'] != u'success':
            msg = "request responses ERROR with url : {0}".format(url)
            logger.error(msg)
            return (1, msg)

        data = opcode[u'data']
        if len(data) == 0 or u'items' not in data:
            msg = "No items content in request responses ERROR with url : {0}".format(url)
            logger.error(msg)
            return (1, msg)

        item = data[u'items']
        if len(item) == 0 or u'scroll_news' not in item[0]:
            msg = "No scroll_news content in request responses ERROR with url : {0}".format(url)
            logger.error(msg)
            return (1, msg)

        scroll_news = item[0][u'scroll_news']
        if len(scroll_news) == 0 or u'link_url' not in scroll_news[0]:
            msg = "No link_url content in request responses ERROR with url : {0}".format(url)
            logger.error(msg)
            return (1, msg)

        link_url = scroll_news[0][u'link_url']
        logger.info("check link_url")
        return check_link_url(link_url, logger)

    #create logging directory
    if not os.path.exists("log"):
        os.mkdir('log')


if __name__ == '__main__':
    #create logging directory
    if not os.path.exists("log"): 
        os.mkdir('log')

    #backup log file if it reaches maximum size 10M
    logFile = "log" + os.sep + "log.phpmonitor"
    if os.path.exists(logFile) and os.path.getsize(logFile) > 10*1024*1024:
        backupFile = time.strftime("%Y%m%d%H%M%S", time.localtime(time.time())) + ".tar.gz"
        status = commands.getstatusoutput("cd log&&tar cvzf " + backupFile + " " + os.path.basename(logFile) + "&&cd -")
        os.remove(logFile)
    #init logging
    logging.basicConfig(
            filename = logFile,
            format = "%(levelname)-10s 0x%(thread)-16x%(asctime)s %(message)s",
            level = logging.DEBUG 
            )
    logger = logging.getLogger('php_monitor')
    logger.info('script php_monitor starts')

    urls = []
    urls.append('http://web370.benditoutiao.com/channel/newslist&&channel_id=2&&region_type=1&&action=new&&source_type=1')
    for url in urls:
        ret, msg = check_functionality(url, logger)
        if ret != 0:
            logger.error('PHP function checkpoint fails with code {0} by url : {1}'.format(ret, url))
            mail = "echo 'url [{0}] responses with code {1}'|mail -s 'PHP function checkpoint fails'  \
                    xujian@benditoutiao.com lishanghao@benditoutiao.com guxiangyu@benditoutiao.com".format(url, ret)
            logger.info('email sent for notification')
            ret, out = commands.getstatusoutput(mail)
            logger.debug('status = {0}, out = {1} for command {2}'.format(ret, out, mail))

    logger.info('script exit\r\n')
