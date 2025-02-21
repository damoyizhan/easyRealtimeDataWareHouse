# -*- coding: utf-8 -*-
# **************************************************************************
# author:ZHH
# function: parser command line args
# #**************************************************************************
import argparse


def get_args():
    # 命令行参数解析
    parser = argparse.ArgumentParser()
    parser.add_argument('--action',
                        required=True,
                        type=str,
                        default='',
                        choices=['start', 'stop'],
                        help='job ctl 执行的命令类型')
    
    parser.add_argument('--job_name',
                        required=False,
                        type=str,
                        help=' job name you want to run/stop/maint ')
    
    parser.add_argument('--confile',
                        required=False,
                        type=str,
                        default='conf.ini',
                        help='config file default ./conf/conf.ini')
    
    parser.add_argument('--confdb',
                        required=False,
                        type=str,
                        help='db config msg , job run on this db  ')
    
    parser.add_argument('--log_level',
                        required=False,
                        default='info',
                        type=str,
                        choices=['debug', 'info', 'warning', 'error', 'critical'],
                        help='logging.loglevel')
    
    args = parser.parse_args()
    return args
