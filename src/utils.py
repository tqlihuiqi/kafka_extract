# -*- coding:utf-8 -*-

import os
import time

import psutil


def timestamp_to_datetime(timestamp, formatter="%Y-%m-%d %H:%M:%S"):
    """ timestamp格式化为datetime

    :参数 timestamp: 精确到秒的timestamp时间
    :参数 formatter: 格式化后的时间格式，默认: %Y-%m-%d %H:%M:%S
    :返回: CST时间
    """

    ltime = time.localtime(timestamp)
    timeStr = time.strftime(formatter, ltime)
    return timeStr


def datetime_to_timestamp(datetime, formatter="%Y-%m-%d %H:%M:%S"):
    """ datetime格式化为timestamp

    :参数 datetime: 日期时间
    :参数 formatter: 指定传入日期时间的格式，默认: %Y-%m-%d %H:%M:%S
    :返回: timestamp时间
    """

    fmtTime = time.strptime(datetime, formatter)
    return int(time.mktime(fmtTime))


def get_process_pid(pidfile):
    """ 获取Collect程序PID

    :参数 pidfile: PID文件
    :返回: PID/None
    """

    if os.path.exists(pidfile):
        pid = open(pidfile, "r").readline()
        
        if pid and int(pid) in psutil.pids():
            return int(pid)

    return None