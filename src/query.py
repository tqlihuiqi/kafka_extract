# -*- coding:utf-8 -*-

import os

import leveldb

from src.base import base
from src.utils import datetime_to_timestamp, timestamp_to_datetime


class TopicQuery(object):

    @staticmethod
    def run(cluster, topic, startDate, endDate):
        """ 查询指定kafka集群中的Topic Logsize数据

        :参数 cluster: 指定查询Kafka集群名称
        :参数 topic: 指定查询Kafka Topic名称
        :参数 startDate: 指定查询起始时间"%Y-%m-%d %H:%M:%S"
        :参数 endDate: 指定查询结束时间"%Y-%m-%d %H:%M:%S"
        """

        try:
            startTimestamp = datetime_to_timestamp(startDate)
            endTimestamp = datetime_to_timestamp(endDate)

        except ValueError:
            raise ValueError("The date format is not correct.")

        database = os.path.join(base.config["collector"]["datadir"], cluster + "_" + topic)
        result = []

        if not os.path.isdir(database):
            return result

        db = base.init_leveldb(cluster=cluster, topic=topic)
        
        for key, _ in db.RangeIter():
            if int(key) >= startTimestamp and int(key) <= endTimestamp:
                result.append(timestamp_to_datetime(int(key)))

        return result