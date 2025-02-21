# -*- coding: utf-8 -*-
import configparser


def get_conf(file):
    __conf = configparser.RawConfigParser()
    __conf.read(file, encoding='utf-8')
    return __conf

# conf = get_conf(file=r'D:\06、code\easyRealtimeDatawarehouse\conf\conf.ini')
# print(conf['db_dwh']['host'])
