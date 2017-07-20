# -*- coding:utf-8 -*-

import os
import yaml

import leveldb


class Base(object):

    def __init__(self):
        """ 初始化设置 """

        basedir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "../")
        configFile = os.path.join(basedir, "etc/config.yaml")
        
        with open(configFile) as fd:
            self.config = yaml.load(fd)


    def init_leveldb(self, cluster, topic):
        """ 初始化LevelDB

        :返回: LevelDB数据库客户端实例
        """

        db = leveldb.LevelDB(os.path.join(self.config["collector"]["datadir"], cluster + "_" + topic))
        
        return db


base = Base()
