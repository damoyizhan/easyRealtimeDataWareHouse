# 任务类型说明:

# 日志类型说明:

1、 log    文件:任务执行log输出
2、 ctlog  文件:只记录任务的启动、关闭、维护等干预操作的log输出

# startdwh
命令目的:启动一个计算类任务

python jobctl.py --action start --task aa.py  --log_level info  --confile  conf/conf.ini

  --confile   可选参数 默认conf/conf.ini

   --log_level 可选参数 
 