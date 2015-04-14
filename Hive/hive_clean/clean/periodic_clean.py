# -*- coding: utf-8 -*-

__author__ = 'chonglou'

import os
import ConfigParser
from datetime import datetime, timedelta
from subprocess import check_output, CalledProcessError

from db_manager import DBManager
from base import BaseObject


root_path = os.path.dirname(os.path.split(os.path.realpath(__file__))[0])
log_file_path = root_path + "/hive_clean.log"
config_path = root_path + "/db_config.ini"


class PeriodicClean(BaseObject):
    """
    数据清理可执行类,直接调用静态run方法
    """
    def __init__(self):
        super(PeriodicClean, self).__init__()

    @staticmethod
    def run():
        param_map = PeriodicClean.read_config()
        db = DBManager(param_map["host"], param_map["username"],
                       param_map["password"], param_map["database"],
                       int(param_map["port"]))
        db.execute("select * from hive_data_periodic_clean")
        data = db.fetchone()
        while data is not None:
            table_attr = TableAttr(data)
            clean_operation = CleanOperation(table_attr)
            clean_operation.execute()
            data = db.fetchone()
        db.close()

    @staticmethod
    def read_config():
        """
        读取数据库配置文件
        :return:
        """
        db_configure = ConfigParser.ConfigParser()
        db_configure.read(config_path)
        host = db_configure.get("mysql", "host")
        database = db_configure.get("mysql", "database")
        username = db_configure.get("mysql", "username")
        password = db_configure.get("mysql", "password")
        port = db_configure.get("mysql", "port")

        param_db = {}
        param_db["host"] = host
        param_db["database"] = database
        param_db["username"] = username
        param_db["password"] = password
        param_db["port"] = port

        return param_db


class TableAttr(BaseObject):
    """
    清理表属性参数实体类
    """
    def __init__(self, tuple_data):
        print tuple_data
        self.database = tuple_data[1]
        self.table_name = tuple_data[2]
        self.partition = tuple_data[3]
        self.date_format = tuple_data[4]
        self.location = tuple_data[5]
        self.periodic = tuple_data[6]


class CleanOperation(BaseObject):
    """
    清理操作工具类，根据从数据库中查询的属性参数，计算并构建数据清除命令，
    并执行
    """
    def __init__(self, table_attr):
        super(CleanOperation, self).__init__()
        self.__table_attr = table_attr
        self.__cmdList = []

    def __build_commands(self):
        """
        构建清理命令
        :return:
        """
        drop_sql = "use %s;ALTER TABLE %s DROP PARTITION (%s='%s')"
        rm_cmd = "dfs -rm -r %s/%s=%s;"
        date_value = self.__build_periodic()
        self.__cmdList.append(["hive", "-e", drop_sql % (self.__table_attr.database, self.__table_attr.table_name,
                                                        self.__table_attr.partition, date_value)])

        self.__cmdList.append(["hive", "-e", rm_cmd % (self.__table_attr.location,
                                                      self.__table_attr.partition, date_value)])

    def __build_periodic(self):
        """
        计算要清理的分区参数值
        :return:
        """
        today = datetime.now()
        remain_days = timedelta(days=(0-self.__table_attr.periodic-1))
        return (today + remain_days).strftime(self.__table_attr.date_format)

    def execute(self):
        """
        执行命令
        :return:
        """
        self.__build_commands()
        print self.__cmdList
        for cmd in self.__cmdList:
            try:
                check_output(cmd, stderr=open(log_file_path, "a"))
            except CalledProcessError, e:
                self.logger.error("command is :" + ' '.join(cmd) +
                                  " output is:" + e.output + " return code is:" + str(e.returncode))

