--系统参数调整
-- set global internal_tmp_disk_storage_engine=innodb;  MySQL 8.0.16 之后不再支持

set global tmp_table_size=128M;              -- 增大临时表空间，避免较大sql出现资源不足运行错误
set global innodb_support_instant_add_column=ON; -- 开启秒加字段（5.7.1.0.6 以上版本支持，且新增字段为最后一列）

CREATE SCHEMA easyetl.job_info; -- 存储字典表

DROP   TABLE IF EXISTS easyetl.job_info;
CREATE TABLE easyetl.job_info (
 job_name       varchar(100)                 comment '任务名'
,start_time     bigint                       comment '任务下一次执行的起始时间'
,time_interval  INT             DEFAULT 1000 comment '任务执行的时间窗口,单位毫秒'
,sleep          INT             DEFAULT 100  comment '任务执行后休眠时长,单位毫秒'
,status         TINYINT         DEFAULT 1    comment '任务状态, 0:停用 1:启用'
,script         varchar(200)                 comment '任务脚本在项目中的相对路径'
,create_time    TIMESTAMP DEFAULT CURRENT_TIMESTAMP                               comment '本条数据创建时间,业务逻辑无关'
,update_time    TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP   comment '本条数据更新时间,业务逻辑无关'
,primary key(job_name)
)engine=innodb DEFAULT charset=utf8mb4 COLLATE=utf8mb4_bin
;

-- ,late_etl_time  bigint                     -- 任务最新etl_time时间
-- ,target_table   varchar(100)               -- 目标表名,格式 schema.table ，删除target table 可以通过解析sql获得
-- ,script_path    varchar(100)               #任务脚本程序所在路径
-- ,phone_alarm    int                        #告警电话
-- ,contact_info   varchar(255)               #告警联系人
-- ,operator       varchar(100)               #操作人
-- ,alert_time     timestamp                  #任务上次告警时间
-- ,kafka          varchar(20)                #ods任务、推送kafka任务对应的kafak集群
-- ,topic          varchar(100)               #ods任务对应的topic
--,max_num        int                        #每次最大发送数量
--,decrpty_col    varchar(200)               #解密字段
-- ,datakey        varchar(512)               #json解析的key列表


CREATE USER 'easyetl'@'172.0.0.1' IDENTIFIED BY '123456';  -- 注意IP不要使用%，必须具体指定,多环境下防呆，防配置失误导致异常数据
GRANT SELECT,INSERT,UPDATE,DELETE  ON easyetl.*  to 'easyetl'@'172.0.0.1'; -- #注意IP 和ETL服务器对应，防止出现因为配置失误导致的相互干扰


INSERT INTO easyetl.job_info(job_name ,start_time ,script) VALUES ('dwd_order_info',NOW() ,'job/dwd_order_info.py')



--CREATE TABLE easyetl.job_info (
-- job_name       varchar(100)                 comment '任务名'
--,start_time     bigint                       comment '任务下一次执行的起始时间'
--,time_interval  INT             DEFAULT 1000 comment '任务执行的时间窗口,单位毫秒'
--,status         TINYINT         DEFAULT 1    comment '任务状态, 0:停用 1:启用'
--,script         varchar(200)                 comment '任务脚本在项目中的相对路径'
--,create_time    TIMESTAMP DEFAULT CURRENT_TIMESTAMP                               comment '本条数据创建时间,业务逻辑无关'
--,update_time    TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP   comment '本条数据更新时间,业务逻辑无关'
--,primary key(job_name)
--)engine=innodb DEFAULT charset=utf8mb4 COLLATE=utf8mb4_bin
--;