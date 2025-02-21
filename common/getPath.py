# -*- coding: utf-8 -*-
# **************************************************************************
# author:ZHH
# function: get project root path
#           获取项目根目录
# #*************************************************************************

from pathlib import Path


def get_logfile_path(task_name, log_type):
    if log_type == 'log':
        logfile_name = task_name + '.log'
    elif log_type == 'ctl':
        logfile_name = task_name + '.ctlog'
    else:
        raise Exception('Wrong log_type {log_type}!!!'.format(log_type=log_type))
    return get_project_path() / 'log' / logfile_name


def get_project_path():
    """
    
    :return: project's full path
    """
    prj_path = Path(__file__).resolve()
    while prj_path.name != 'easyRealtimeDatawarehouse':
        prj_path = prj_path.parent
        if prj_path == prj_path.parent:
            raise Exception('Wrong project name config!!!')
    return prj_path

# print('-----------------')
# print(get_project_path())
