# -*- coding: utf-8 -*-
# sr is short for starrocks
import hashlib
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(funcName)s - %(message)s')
logger = logging.getLogger(__name__)


def get_data_blood_wholeline(cursor, obj, direction, depth):
    """
    返回指定数据对象的多层上游或多层下游
    参数说明：
    obj: 需要检索血缘的对象
    direction: 'up' = 溯源/上游  down=下游
    depth: int,向前或向后检索几层，不输入默认all
    """
    # todo depth 参数尚未起作用
    __list = get_data_blood_single(cursor, obj, direction)
    
    while any(__dict.get('matched') == 'NO' for __dict in __list):
        for __dict in __list:
            if __dict.get('matched') == 'NO':
                __list.extend(get_data_blood_single(cursor, (__dict['obj'] if direction == 'UP' else __dict['obj_down']), direction))
                __dict['matched'] = 'YES'
    
    for item in __list:
        print(item)
    return __list


def get_data_blood_single(cursor, obj, direction):
    """
    返回指定数据对象的直接上游或直接下游
    """
    __sql = ''
    if direction == 'UP':
        __sql = '''
            SELECT upstream_obj_name    as obj     ,
                   downstream_obj_name  as obj_down,
                   analysis_desc        as analysis_desc
            FROM   datagov.data_blood
            WHERE  downstream_obj_name = '{obj}'
            '''.format(obj=obj)
    if direction == 'DOWN':
        __sql = '''
            SELECT upstream_obj_name    as obj     ,
                   downstream_obj_name  as obj_down,
                   analysis_desc        as analysis_desc
            FROM   datagov.data_blood
            WHERE  upstream_obj_name = '{obj}'
            '''.format(obj=obj)
    
    if direction == 'BOTH':
        __sql = '''
             SELECT upstream_obj_name    as obj     ,
                    downstream_obj_name  as obj_down,
                    analysis_desc        as analysis_desc
             FROM   datagov.data_blood
             WHERE  downstream_obj_name = '{obj}'
             UNION ALL
             SELECT upstream_obj_name    as obj     ,
                    downstream_obj_name  as obj_down,
                    analysis_desc        as analysis_desc
             FROM   datagov.data_blood
             WHERE  upstream_obj_name = '{obj}'
            '''.format(obj=obj)
    
    cursor.execute(__sql)
    __result = list(cursor.fetchall())
    __return_list = []
    for __row in __result:
        __return_list.append({'obj': __row[0], 'obj_down': __row[1], 'desc': __row[2], 'matched': 'NO'})
    return __return_list


def data_blood_upsert(cursor, upsert_list):
    """
    insert or update 数据到 gov_pro.data_blood 表
    :param cursor:
    :param upsert_list:
    :return:
    """
    # 说明：
    # （1）反复执行INSERT 插入相同的数据，starrocks自动把INSERT改为了UPSERT，所以不需要自己写UPSERT 判断；
    # （2）autocommit 参数对starrocks无效，无法通过autocommit参数优化写入效率
    # （3）使用 INSERT VALUES 的方式提高写入速度
    sql_forebody = ("INSERT INTO datagov.data_blood ( "
                    "id                  ,relation_type       ,upstream_obj_type   ,upstream_obj_name   ,"
                    "downstream_obj_type ,downstream_obj_name ,analysis_desc       ,update_time         ,"
                    "is_delete )          "
                    "VALUES               ")
    sql = sql_forebody
    values_num = 50
    i = 1
    
    for item in upsert_list:
        # logger.info(item)
        # 可能会出现两个不同任务解析出来相同血缘的情况，这种两个血缘记录都会保留,所以用hash 方法生成row_key_id 的时候增加了analysis_desc
        row_key_id = hashlib.md5((item['relation_type'] +
                                  item['upstream_obj_type'] +
                                  item['upstream_obj_name'] +
                                  item['downstream_obj_type'] +
                                  item['downstream_obj_name'] +
                                  (item['analysis_desc'] if item['analysis_desc'] is not None else '')
                                  ).encode('utf-8')
                                 ).hexdigest()
        
        sql = sql + ("("
                     "  '{id}', '{relation_type}', '{upstream_obj_type}', '{upstream_obj_name}', "
                     "  '{downstream_obj_type}', '{downstream_obj_name}', '{analysis_desc}',{update_time},'{is_delete}'"
                     "),").format(id=row_key_id,
                                  relation_type=item['relation_type'],
                                  upstream_obj_type=item['upstream_obj_type'],
                                  upstream_obj_name=item['upstream_obj_name'],
                                  downstream_obj_type=item['downstream_obj_type'],
                                  downstream_obj_name=item['downstream_obj_name'],
                                  analysis_desc=item['analysis_desc'],
                                  update_time='NOW()',
                                  is_delete=item['is_delete'])
        
        if i % values_num == 0 or i == len(upsert_list):  # 执行sql，重新初始化sql
            logger.info('total rows-{t} insert rows-{i}'.format(t=len(upsert_list), i=i))
            sql = sql.rstrip(',')
            try:
                cursor.execute(sql)
            except Exception as e:
                logger.info(sql)
                logger.info(e)
                raise Exception('INSERT INTO datagov.data_blood Failed!!!')
            
            sql = sql_forebody
        
        i = i + 1
