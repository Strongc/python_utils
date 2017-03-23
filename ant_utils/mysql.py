
# -*- coding: utf-8 -*-

import MySQLdb

class MySql(object):
    def __init__(self, host, port, user, passwd, db, charset='utf8'):
        self.connect(host, port, user, passwd, db, charset)
    
    def connect(self, host, port, user, passwd, db, charset='utf8'):
        self.args = (host, port, user, passwd, db, charset)
        self.db = MySQLdb.connect(host=host, port=port, user=user, passwd=passwd, db=db, charset=charset, connect_timeout=10)
    
    def reconnect(self):
        try:
            self.db.ping()
        except:
            self.db.close()
            self.connect(*self.args)

    def select(self, table, columns, where='', others=''):
        self.reconnect()
        cursor = self.db.cursor()
        sql = "SELECT %s FROM %s %s %s" % (','.join(columns), table, where, others)
        cursor.execute(sql)
        return cursor.fetchall()

    def execute(self, sql, args):
        self.reconnect()
        cursor = self.db.cursor()
        r = cursor.execute(sql, args)
        self.db.commit()
        cursor.close()
        return r

    def executemany(self, sql, args):
        self.reconnect()
        cursor = self.db.cursor()
        r = cursor.executemany(sql, args)
        self.db.commit()
        cursor.close()
        return r

def get_test_db():
    return MySql('rm-bp16w04r2zu0w499ao.mysql.rds.aliyuncs.com', 3306, 'bdtt', 'Chengzi123', 'bdtt_algorithm')

def get_online_db():
    return MySql('rm-bp16w04r2zu0w499ao.mysql.rds.aliyuncs.com', 3306, 'bdtt', 'Chengzi123', 'bdtt_crawler')

def get_db():
    return get_test_db()

