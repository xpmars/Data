# -*- coding: utf-8 -*-

__author__ = 'chonglou'

import MySQLdb

from base import BaseObject


class DBManager(BaseObject):
    """
    mysql数据库链接管理类
    """

    def __init__(self, host, username, password, database, port=3306):
        super(DBManager, self).__init__()
        self.__db = MySQLdb.connect(host, username, password, database, port)
        self.__cursor = self.__db.cursor()
        self.__rowcount = 0

    def close(self):
        self.__cursor.close()
        self.__db.close()

    def execute(self, query, args=None):
        self.__rowcount = self.__cursor.execute(query, args)
        return self.__rowcount

    def executemany(self, query, args):
        self.__rowcount = self.__cursor.executemany(query, args)
        return self.__rowcount

    def fetchone(self):
        if self.__rowcount == 0:
            self.logger.info("jobs count from mysql is 0, do nothing!")
            return None
        else:
            return self.__cursor.fetchone()

