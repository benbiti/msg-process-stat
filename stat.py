# !/usr/bin/python
# -*- coding: UTF-8 -*-


# Log prepared like below
# 18/04/23 02:50:22
# [{"partition":23,"fromOffset":6454904,"untilOffset":6455352}]
# --

from kafka import KafkaConsumer
from kafka import TopicPartition
from multiprocessing.dummy import Pool
import struct, sys, os.path
import json
import time
import subprocess

TOPIC="click"
GROUP="STAT"
KAFKA_SERVERS=['kafka0:9092', 'kafka1:9092', 'kafka2:9092']
TIME_STAT=[]
finished_time=0L
LOG_FILE = "/Users/ben/.projects/stdout.log"
STAT_IN=False

def calc_record_process_time(value):
    global finished_time
    record = json.loads(value)
    return finished_time + 3600*8 - long(record["stm"])/1000


def stat_partition(ptm):
    tp = TopicPartition(TOPIC, ptm["partition"])
    fromOffset= ptm["fromOffset"]
    untilOffset = ptm["untilOffset"]

    if fromOffset == untilOffset:
        return 0

    consumer = KafkaConsumer(bootstrap_servers=KAFKA_SERVERS, enable_auto_commit=False)
    consumer.assign([tp])
    consumer.seek(partition=tp, offset=int(fromOffset))

    for data in consumer:
        process_time = calc_record_process_time(data.value)
        TIME_STAT.append(process_time)
        if data.offset >= untilOffset:
            break


def process(line_time, line_offset):

    global finished_time
    line_time_array = line_time.split(" ")
    finished_time = time.mktime(time.strptime(line_time_array[0] + " " + line_time_array[1], "%y/%m/%d %H:%M:%S"))
    offsets_array =line_offset.split("'")
    offset = offsets_array[5]
    partitionmap = json.loads(offset)
    pool = Pool(processes=4)
    results = pool.map(stat_partition, partitionmap)

    output_stat()

def process_log():
    nf = open(LOG_FILE +'.prepared', 'r')
    line = nf.readline()
    while line:
        if (line.find("fromOffset") > -1) and (line.find("insert") > -1):
            process(line_prev, line)

        line_prev = line
        line = nf.readline()
    nf.close()


def avg(stat_list):
    ret = 0L
    for i in stat_list:
        ret += i

    return int(ret/len(stat_list))


def output_stat():
    global TIME_STAT
    if STAT_IN:
        if len(TIME_STAT) > 0:
            print "MAX(s)" + str(max(TIME_STAT))
            print "MIN(s)" + str(min(TIME_STAT))
            print "AVG(s)" + str(avg(TIME_STAT))
        else:
            print "No Data Processed in This Batch"
    else:
        for i in TIME_STAT:
            print i

    TIME_STAT = []


def prepare_log():
    cmd ="cat " + LOG_FILE + " | grep fromOffset -B1 > " + LOG_FILE + '.prepared'
    subprocess.call(cmd, shell=True)

if __name__ == "__main__":
    prepare_log()
    process_log()


