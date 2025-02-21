# -*- coding: utf-8 -*-
# **************************************************************************
# author:ZHH
# function: Control entry for job creation/start/stop/deletion/maintenance
#           任务新增/启动/停止/删除/维护的 控制入口
# #*************************************************************************
import pathlib
import jobHandle
from common.getArg import get_args
from common.getLog import MyLogger as MyLogger
from common.getPath import get_project_path, get_logfile_path
from common.getConf import get_conf

# 获取参数
args = get_args()
prj_path = get_project_path()
log_file = get_logfile_path(args.job_name, log_type='log')
ctl_file = get_logfile_path(args.job_name, log_type='ctl')

# 设置日志参数
logger = MyLogger(name=pathlib.Path(__file__).name, log_file=log_file, log_level=args.log_level)


def __start(__args):
    # (1) 启动前检查:参数是否合理正确
    
    # (2) 启动前检查:检查同服务器上是否有相同的任务在执行
    
    # (3) 初始化:组织任务变量
    
    # (4) 初始化:组织表依赖关系
    
    # (5) 启动任务
    # (5.1) todo:输出启动时间到 ctllog , ctllog 只记录任务的启动、停止
    
    # (5.2) 启动任务
    
    job = jobHandle.JobHandle(job_name=args.job_name,
                              conf=conf,
                              prj_path=prj_path,
                              log_file=log_file,
                              log_level=args.log_level)
    job.start()
    
    # ----action start --job dwd_order_info --confile conf/conf.ini --confdb  db_dwh --log_level debug
    # start_job(
    #     script_path=_task_info[0],
    #     start_time=_task_info[1],
    #     target_schema=_task_info[2],
    #     target_table=_task_info[3],
    #     time_interval=_task_info[6],
    #     task_name=_task_info[12],
    #     proc_start=time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())),
    #     rootPath=rootPath,
    #     sleep=args.sleep,
    #     loglevel=args.loglevel
    # )
    #
    # cmd_startdwd = ('nohup echo "任务启动时间：{proc_start}" >> {target_table}.startlog 2>&1 & '
    #                 'nohup python {rootPath}/rt_etl/{script_path} --task_name {task_name} --start_time {start_time} --time_interval {time_interval} --sleep {sleep} --loglevel {loglevel} >> /data/etl/log/etl_py_realtime/{target_schema}/{target_schema}_{target_table}.log 2>&1 &').format(
    #     script_path=_task_info[0],
    #     start_time=_task_info[1],
    #     target_schema=_task_info[2],
    #     target_table=_task_info[3],
    #     time_interval=_task_info[6],
    #     task_name=_task_info[12],
    #     proc_start=time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())),
    #     rootPath=rootPath,
    #     sleep=args.sleep,
    #     loglevel=args.loglevel
    # )
    #
    # logger.info("cmd_startdwd:" + cmd_startdwd)
    # cmd_start_result = os.system(cmd_startdwd)
    # if cmd_start_result > 0:
    #     raise Exception("task is start failed!")
    # else:
    #     logger.info("task start success!!")
    #     logger.info("请检查任务进程和观察日志进一步确定任务执行情况")
    
    logger.debug(' do nothing ')


def stop(args):
    pass
    # todo py 如何接收到中断信号后 处理关闭问题再自杀


if __name__ == '__main__':
    
    # 读取config文件 ,并将config 文件中解析出来的配置信息加入到arg
    conf_file = prj_path / 'conf' / args.confile
    logger.info('Use Config File :{file}'.format(file=conf_file))
    conf = get_conf(file=conf_file)
    
    
    
    # 启动任务
    if args.action == 'start':
        __start(args)
    
    else:
        raise Exception("没有找到 合适的 Action 类型")
