#!/bin/env python
# -*- coding: utf-8 -*- 

import time
import os
import logging
import commands
import re
import pymysql
import pexpect
from optparse import OptionParser

def filter(out, child, logger):
    result = out.split('\r\n')
    sep = ''
    title = ''
    msg = ''
    for row in result:
        if row == '':
            continue

        if row[0] == '+':
            sep = row
        if row.find('Id') != -1:
            title = row

    access_list = [row for row in result if row.find('|') != -1]
    alert_list = []
    for line in access_list:
        access_info = [info for info in line.split('|') if info != '']
        logger.debug('{0}'.format(access_info))
        id, user, host, db, command, totaltime, state, info = access_info
        try:
            totaltime = int(totaltime.strip())
        except ValueError as e:
            logger.error("Fails to retreive time info from id={0} user={1} host={2} db={3}, command={4}".format(
                id, user, host, db, command.strip()))
            continue

        if command.strip() == "Query": 
            logger.debug("Query connection : {0}".format(line))

        if totaltime >= 300 and command.strip() == "Query":
            alert_list.append(line)
            logger.critical("This connection consumes much resource! id={0} user={1} host={2} db={3}".format(
                id, user, host, db))
            logger.info("connection {0} will be terminated".format(id))
            try:
                #child.sendline("kill {0};".format(id))
                #child.expect("mysql>")
                logger.info("kill connection {0}".format(id))
            except pexpect.exceptions.EOF as e:
                logger.error("Fail to kill connection : {0}".format(id))

    msg = sep + "\r\n" + title + "\r\n" + sep + "\r\n" + "\r\n".join(alert_list)
    return msg

def initOptions():
    usage = "Usage %prog [options]"
    parser = OptionParser(usage = usage)
    parser.add_option('-s', '--server', dest = 'host', 
            default = 'rm-bp175g6f130am762w.mysql.rds.aliyuncs.com',
            help = "mysql server instance")
    parser.add_option('-u', '--username', dest = 'username', default = 'bdtt',
            help = "mysql user to access database")
    parser.add_option('-p', '--password', dest = 'password', default = 'Chengzi123',
            help = "mysql password")
    parser.add_option('-d', '--database', dest = 'database', default = 'bdtt_test',
             help = "mysql database")
    return parser.parse_args()

def init():
    #create logging directory
    if not os.path.exists("log"):
        os.mkdir('log')

    #backup log file if it reaches maximum size 10M
    logFile = "log" + os.sep + "log.sqlmonitor"
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

    return logging.getLogger('sqlmonitor')

if __name__ == "__main__":
    options, args = initOptions()
    command = "mysql -h {0} -u{1} -p{2} {3}".format(options.host, 
            options.username, options.password, options.database)
    logger = init()
    logger.info("sqlmonitor script starts")
    logger.info("prepare to login to MySQL with command : {0}".format(command))
    msg = ''
    try:
        child = pexpect.spawn(command)
        #child.logfile = logger
        child.expect("mysql>")
        child.sendline("show processlist;")
        child.expect("mysql>")
        out = child.before
        logger.debug(out)
        logger.info("start to monitor MySQL connections")
        msg = filter(out, child, logger)
    except pexpect.exceptions.EOF as e:
        logger.critical("Fail to login to MySQL server! Exit now")
        exit(1)

    mail = "echo 'connections with time consumed over than 5 minutes :\r\n{0}'|mail -s 'MySQL monitor result' \
            pengxuelin@benditoutiao.com guxiangyu@benditoutiao.com xujian@benditoutiao.com".format(msg)
    logger.info("MySQL monitor result with killed list: \n{0}".format(msg))
    records = len(msg.split("\r\n"))
    logger.debug("result lenth : {0}".format(records))
    if records > 4:
        commands.getstatusoutput(mail)
    child.close()
    logger.info("sqlmonitor script exits\r\n")
