# coding: utf-8
# **************************************************************************
# author:ZHH
# function: db connection managment
# **************************************************************************

import pymysql


def create_conn(conf):
    # 创建连接
    conn = pymysql.connect(host=conf.get("database", "host"),
                           user=conf.get("database", "user"),
                           passwd=conf.get("database", "passwd"),
                           port=int(conf.get("database", "port"))
                           )
    conn.autocommit(True)
    return conn


def create_cursor(conn):
    return conn.cursor()


def close_conn(conn):
    conn.close()
