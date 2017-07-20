## 导出指定时间范围的Kafka Topic数据

系统定期对Topic进行采样，用户可使用命令指定时间范围进行数据导出，导出的数据可以输出到文本文件或指定的Kafka Topic中。支持对AVRO数据导出。


### 安装配置

```shell
cd kafka_extract; pip install -r requirements.txt
```

- etc/config.conf 采集数据相关配置


### 管理收集进程

```shell
cd kafka_extract/sbin; ./collectd start | stop | status
```

### 列出可用topic

```shell
./list --cluster testCluster

Cluster:  testCluster
  Topic:  testTopic1
  Topic:  testTopic2
```

### 模糊查询可导出的数据时间范围

```shell
./query -c testCluster -t testTopic1 -s "2017-07-01 00:00:01" -e "2017-07-30 00:00:01"

2017-07-19 17:25:42
2017-07-19 17:27:15
2017-07-19 17:37:19
2017-07-19 17:47:23
```

### 导出数据到本地文件

```shell
./extract -c testCluster -t testTopic1 -s "2017-07-19 17:37:19" -e "2017-07-19 17:47:23" \ 
          --output disk \
          --disk_dir ./

Partition 0: 1973162837/1973162837 Offsets.
Partition 1: 1973094402/1973094402 Offsets.
Partition 2: 1973095771/1973095771 Offsets.
Partition 3: 1973163811/1973163811 Offsets.
Partition 4: 1973174725/1973174725 Offsets.
Partition 5: 1973183975/1973183975 Offsets.
Partition 6: 1973269887/1973269887 Offsets.
Partition 7: 1973125716/1973125716 Offsets.
```

### 导出数据到指定Kafka Topic

```shell
./extract -c testCluster -t testTopic1 -s "2017-07-19 17:37:19" -e "2017-07-19 17:47:23" \
          --output kafka \
          --target_brokers 10.0.0.1:9092,10.0.0.2:9092 \
          --target_topic targetTopic \
          --compression_type snappy


Partition 0: 1973162837/1973162837 Offsets.
Partition 1: 1973094402/1973094402 Offsets.
Partition 2: 1973095771/1973095771 Offsets.
Partition 3: 1973163811/1973163811 Offsets.
Partition 4: 1973174725/1973174725 Offsets.
Partition 5: 1973183975/1973183975 Offsets.
Partition 6: 1973269887/1973269887 Offsets.
Partition 7: 1973125716/1973125716 Offsets.
```

### 导出AVRO数据到磁盘

```shell
./extract -c testCluster -t testTopic2 -s "2017-07-19 17:38:19" -e "2017-07-19 17:48:23" \ 
          --output disk \
          --disk_dir ./ \
          --avro_schema "{'doc': 'A weather reading.', 'name': 'Weather', 'namespace': 'test', 'type': 'record', 'fields': [{'name': 'station', 'type': 'string'}, {'name': 'time', 'type': 'long'}, {'name': 'temp', 'type': 'int'}]}"

Partition 0: 1837/1837 Offsets.
Partition 1: 1402/1402 Offsets.
Partition 2: 1971/1971 Offsets.
Partition 3: 1790/1790 Offsets.         
```