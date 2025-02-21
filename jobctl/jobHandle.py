# -*- coding: utf-8 -*-
# **************************************************************************
# author:ZHH
# function: job handle class
#           实际执行任务的逻辑
# #*************************************************************************
import datetime
import pathlib
import time

import yaml

from common.getConn import create_conn, create_cursor
from common.getLog import MyLogger


class JobHandle:
    
    def __init__(self, job_name=None, conf=None, prj_path=None, log_file=None, log_level=None):
        
        self.logger = MyLogger(name=pathlib.Path(__file__).name, log_file=log_file, log_level=log_level)
        self.conn = create_conn(conf)  # 连接数据库
        self.cursor = create_cursor(self.conn)
        self.job_name = job_name
        self.prj_path = prj_path
        
        self.sleep = None
        self.status = None
        self.start_time = None
        self.script_path = None
        self.time_interval = None
        
        # 从job 配置表中获取任务的数据字典
        self.get_job_info()
        # 从job 的script 路径获取任务代码并执行检查、生成任务依赖关系
        self.get_job_sqllist()
        
        # self.max_interval
        # self.sleep          #  每次循环休眠 todo
        
        self.logger.info('JobHandle Dict   : {dict}'.format(dict=self.__dict__))
        self.logger.info('JobHandle DBconn : {info}'.format(info=self.conn.get_host_info()))
        
        
    
    def run(self):
        """
            循环执行任务
        """
        # 执行前检查
        if self.status == 0:
            raise Exception("{job_name} is disabled".format(job_name=self.job_name))
        
        # 总体参数初始化
        __start_time = self.init_start_time
        __end_time = __start_time
        
        while True:
            
            # 本次循环参数初始化
            __max_rowcnt = 0
            
            # step（1）:执行sql list
            for i in range(len(self.sql_list)):
                # sql 参数赋值
                sql_name = self.sql_list[i]["sql_name"]  # todo: 去掉变量sql_name
                sql = self.sql_list[i]["sql"].format(start_time=__start_time, end_time=__end_time)
                self.logger.debug("SQL Execute at once :" + sql_name + ":" + sql)
                
                # 执行并记录执行时间
                execute_time_start = time.time()
                rowcnt = self.cursor.execute(sql)
                execute_time_end = time.time()
                
                self.logger.info("sql_name:%10s sql_seq:%3d sql_start(format):%s  sql_start:%d  sql_end:%d  row cnt:%4d seconds: %0.4f" %
                                 (sql_name,
                                  i + 1,
                                  datetime.datetime.fromtimestamp(__start_time / 1000).strftime("%Y-%m-%d %H:%M:%S"),
                                  __start_time,
                                  __end_time,
                                  rowcnt,
                                  execute_time_end - execute_time_start)
                                 )
                # 记录本批次最大更新数据量
                __max_rowcnt = rowcnt if rowcnt > __max_rowcnt else __max_rowcnt
            
            # step（2）:更新字典表
            self.refresh_job_info(start_time=__start_time, task_name=self.job, late_etl_time=__end_time)
            
            # step（3）sleep 避免空跑虚耗硬件资源
            # step（3.1）如果没有延迟，sleep
            if __end_time > int(time.time() * 1000) - 1000:  # 这里和本地时间比对目的是减少和数据库交互量
                time.sleep(self.sleep)
            # step（3.2）如果__start_time == __end_time，sleep，避免上游发生故障，dwd任务卡死在一个时间点死循环,这时候希望sleep
            if __start_time == __end_time:
                time.sleep(self.sleep)
            
            # step（4）:根据数据量/计算延迟决定time_interval动态调整
            if int(time.time() * 1000) > __end_time + 10 * 60 * 1000:
                self.adjust_interval(__end_time, __max_rowcnt)
            
            # step（5）:生成下一个周期的起止时间
            # step（5.1）确定下一个周期的开始时间
            __start_time = __end_time
            # 时间窗口在近10秒内的，每次回拨计算50ms
            # 这会在没有延迟的时候造成大量重复计算，应信任数据库的一致性操作，不需要回拨
            # todo:经过核心逻辑验证后删除回拨操作
            # 但是如果有一点延迟，就不会做重复计算，所以也还可以接受
            if int(time.time() * 1000) - __start_time < 10 * 1000:
                __start_time = __start_time - 50
            # step（5.2）确定下一个周期的结束时间
            # 下一周期结束时间受三个因素影响:1、小于当前自然时钟; 2、小于所依赖上游任务的结束时间; 3、小于下一个周期的start_time+time_interval
            __end_time = self.get_job_next_endtime(self.job)
            if __end_time - __start_time > self.time_interval * 1000:
                __end_time = __start_time + self.time_interval * 1000
            
            # todo 连接丢失如何重试
    
    def refresh_job_info(self, start_time, task_name, late_etl_time):
        pass
    
    def adjust_interval(self, __end_time, __max_rowcnt):
        """
            :param __max_rowcnt:
            :param __end_time:
            :return:
            # time_interval动态调整的目的是
            #   （1）保证不出现一个sql事务中有大量的数据更新，造成下游的流出现“波峰、波谷”
            #   （2）避免大sql导致程序异常
            #    todo 根据任务运行时长统计确定最优的窗口大小
            """
        # 进度延迟且 上一次执行数据量 低于100条/s，调大
        if __max_rowcnt < 100 and self.time_interval < self.max_interval:
            self.time_interval = self.time_interval + 10 if self.time_interval + 10 < self.max_interval else self.max_interval
            self.logger.debug("time_interval 动态调大，调整为 %s" % self.time_interval)
        
        # 进度延迟且 上一次执行数据量 高于2000条/s，调小
        if __max_rowcnt > 2000 and self.time_interval > self.min_interval:
            self.time_interval = self.time_interval - 10 if self.time_interval - 10 > self.min_interval else self.min_interval
            self.logger.debug("time_interval 动态调小，调整为 %s" % self.time_interval)
    
    def get_job_next_endtime(self, job):
        """
        根据任务的依赖关系，获取任务下一个任务的结束时间
        :param job:
        :return:
        """
        return 0
        pass
    
    def get_job_info(self):
        sql = ("SELECT start_time,time_interval,status,sleep,script "
               "FROM   easyetl.job_info                        "
               "WHERE  job_name = '{job_name}'                 ").format(job_name=self.job_name)
        
        self.logger.debug(sql)
        self.cursor.execute(sql)
        res = self.cursor.fetchall()
        
        if len(res) == 0:
            raise Exception("Can not find job {job_name}".format(job_name=self.job_name))
        
        elif len(res) > 1:
            raise Exception("Find multi job {job_name}".format(job_name=self.job_name))
        
        self.start_time = res[0][0]
        self.time_interval = res[0][1]
        self.status = res[0][2]
        self.sleep = res[0][3]
        self.script_path = self.prj_path / pathlib.Path(res[0][4])
    
    def get_job_sqllist(self):
        
        with open(self.script_path, 'r') as file:
            config = yaml.safe_load(file)
        
        print(config)
