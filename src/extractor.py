# -*- coding:utf-8 -*-

import json
import sys

import leveldb

from kafka import KafkaConsumer
from kafka.common import TopicPartition

from src.base import base
from src.transfer import DataTransfer
from src.utils import datetime_to_timestamp, timestamp_to_datetime


class TopicExtractor(object):

    def __init__(self, startDate, endDate, output, cluster, topic, diskPath=None, avroSchema=None, targetBrokers=None, targetTopic=None, compressType=None):
        """ 初始化Extract实例配置

        :参数 startDate: 指定查询起始时间"%Y-%m-%d %H:%M:%S"
        :参数 endDate: 指定查询结束时间"%Y-%m-%d %H:%M:%S"
        :参数 cluster: 指定Kafka集群名称
        :参数 topic: 指定Kafka Topic名称
        :参数 output: 输出类型 disk/kafka
            :disk:
                :参数 diskPath: 数据导出目录, 默认: None
                :参数 avroSchema: 导出AVRO格式的数据，指定AVRO Schema转换成文本数据, 默认: None
            :kafka:
                :参数 targetBrokers: 写入目标Kafka Broker地址列表
                :参数 targetTopic: 写入目标Kafka Topic名称
                :参数 compressType: 写入目标Kafka Topic的数据压缩类型, 可选参数("lz4", "snappy", "gzip")
        """

        if startDate >= endDate:
            raise ValueError("Invalid date range.")

        try:
            startKey = str(datetime_to_timestamp(startDate)).encode("utf-8")
            endKey = str(datetime_to_timestamp(endDate)).encode("utf-8")
        
        except ValueError:
            raise ValueError("Invalid data format.")

        db = base.init_leveldb(cluster=cluster, topic=topic)

        try:
            self.startPosition = json.loads(db.Get(startKey))
            self.stopPosition = json.loads(db.Get(endKey))

        except KeyError:
            raise KeyError("No offset is available.")

        if output == "kafka" and not compressType:
            compressType = "snappy"

        self.output = output
        self.cluster = cluster
        self.topic = topic
        self.compressType = compressType
        self.targetBrokers = targetBrokers
        self.targetTopic = targetTopic
        self.diskPath = diskPath
        self.avroSchema = avroSchema
        self.progress = {}


    def get_progress(self):
        """ 进度条提示 """

        for partition, items in self.progress.items():
            sys.stdout.write("Partition %s: %s/%s Offsets." % (partition, *items) + "\n")

        sys.stdout.write("\033[F" * len(self.progress))


    def run(self):
        """ 抽取指定kafka集群中的Topic Logsize数据
            将抽出的数据输出给transfer进行处理
        """
        
        brokers = base.config["collector"]["clusters"][self.cluster]["brokers"]
        consumer = KafkaConsumer(bootstrap_servers=brokers, enable_auto_commit=False, group_id="kafka_extract")
        consumer.assign([ TopicPartition(self.topic, int(partition)) for partition in self.stopPosition ])
        finish = {}  

        for partition, stopLogsize in self.stopPosition.items():
            tp = TopicPartition(self.topic, int(partition))
            finish[partition] = False

            try:
                startLogsize = self.startPosition[partition]
                consumer.seek(tp, startLogsize)
                self.progress[partition] = [startLogsize, stopLogsize]
           
            except KeyError:
                consumer.seek_to_beginning(tp)
                self.progress[partition] = [0, stopLogsize]

        if self.startPosition == self.stopPosition:
            return

        with DataTransfer(output=self.output, cluster=self.cluster, topic=self.topic, diskPath=self.diskPath, avroSchema=self.avroSchema, targetBrokers=self.targetBrokers, targetTopic=self.targetTopic, compressType=self.compressType) as dt:
            for message in consumer:
                partition = str(message.partition)
                offset = message.offset + 1

                if offset <= self.stopPosition[partition]:
                    dt.transfer(message)
                    self.progress[partition][0] = offset
                    self.get_progress()
                
                if offset >= self.stopPosition[partition]:
                    finish[partition] = True

                    if False not in finish.values():
                        sys.stdout.write("\n" * len(self.stopPosition))
                        sys.stdout.flush()
                        break


