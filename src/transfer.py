# -*- coding:utf-8 -*-

import json
import os

from io import BytesIO

import fastavro as avro

from kafka import KafkaProducer

from src.base import base


class DataTransfer(object):

    def __init__(self, output, cluster, topic, diskPath=None, avroSchema=None, targetBrokers=None, targetTopic=None, compressType="snappy"):
        """ 初始化传输设置

        :参数 cluster: 指定Kafka集群名称
        :参数 topic: 指定Kafka Topic名称

        :参数 output: 输出类型 disk/kafka
            :disk:
                :参数 diskPath: 数据导出目录, 默认: None
                :参数 avroSchema: 导出AVRO格式的数据，指定AVRO Schema转换成文本数据, 默认: None
            :kafka:
                :参数 targetBrokers: 写入目标Kafka Broker地址列表, 默认: None
                :参数 targetTopic: 写入目标Kafka Topic名称, 默认: None
                :参数 compressType: 写入目标Kafka Topic的数据压缩类型, 默认: snappy 可选参数("lz4", "snappy", "gzip")
        """

        self.output = output

        if output == "disk":
            if not diskPath or not os.path.isdir(diskPath):
                raise ValueError("Invalid disk path.")

            filename = "%s_%s.data" % (cluster, topic)
            filepath = os.path.join(diskPath, filename)

            if os.path.exists(filepath):
                raise IOError("File already exists.")

            self.handler = open(filepath, "ab+")

            if avroSchema:
                self.avroSchema = eval(avroSchema)
            else:  
                self.avroSchema = None

        elif output == "kafka":
            self.handler = KafkaProducer(bootstrap_servers=targetBrokers.split(","), compression_type=compressType)
            self.targetTopic = targetTopic

        else:
            raise ValueError("Invalid output parameter.")


    def avro_decode(self, data):
        """ AVRO 转换文本 

        :参数 data: AVRO 二进制数据
        :返回: 文本数据
        """

        buff = BytesIO(data)
        return avro.schemaless_reader(buff, self.avroSchema)


    def to_bytes(self, data):
        """ 将数据转换成bytes类型 
 
        :参数 data: 转换的数据
        :返回: bytes类型的数据
        """

        if isinstance(data, str):
            data = data.encode("utf-8")

        elif isinstance(data, dict) or isinstance(data, list):
            data = json.dumps(data).encode("utf-8")

        elif isinstance(data, bytes):
            pass

        return data + b"\n"


    def transfer(self, message):
        """ 将extractor抽取出来的数据写入指定目标
        
        :参数 message: 抽取出来的Kafka Topic数据
        """

        if self.output == "disk":
            value = message.value

            if self.avroSchema:
                value = self.avro_decode(value)

            self.handler.write(self.to_bytes(value))
        
        elif self.output == "kafka":
            self.handler.send(self.targetTopic, key=message.key, value=message.value)


    def __enter__(self):
        return self


    def __exit__(self, type, value, traceback):
        self.handler.close()

