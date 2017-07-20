# -*- coding:utf-8 -*-

import json
import time

from kafka import KafkaClient
from kafka.common import OffsetRequestPayload

import setproctitle

from src.base import base


class TopicCollector(object):

    def handler(self):
        """ 查询指定Kafka集群Topic中每个Partition当前Logsize, 将Logsize写入LevelDB
            每次收集Logsize数据后会检测retention_day参数，删除过期数据
        """

        clusters = base.config["collector"]["clusters"]
        
        for cluster, metric in clusters.items():
            client = KafkaClient(metric["brokers"], timeout=3)
    
            for topic in metric["topics"]:
                partitions = client.get_partition_ids_for_topic(topic)
                payload = [ OffsetRequestPayload(topic, p, -1, 1) for p in partitions ]
                logsize = { p.partition: p.offsets[0] for p in client.send_offset_request(payload) }
        
                if logsize:
                    key = str(int(time.time())).encode("utf-8")
                    value = json.dumps(logsize).encode("utf-8")
            
                    db = base.init_leveldb(cluster=cluster, topic=topic)
                    db.Put(key, value)
                    deadline = base.config["collector"]["clusters"][cluster]["retention_hour"] * 3600
            
                    for key, _ in db.RangeIter():
                        if time.time() - int(key) > deadline:
                            db.Delete(key)
                        else:
                            break
    
            client.close()


    def run(self):
        """ 启动收集Topic Logsize"""

        setproctitle.setproctitle("KafkaExtractCollector")

        while True:
            self.handler()
            time.sleep(base.config["collector"]["interval_minute"] * 60)
