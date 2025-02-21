# -*- coding: utf-8 -*-
# **************************************************************************
# 创建者 : ZHH
# 功能描述：通用的SQL 语句处理组件
# #**************************************************************************
import logging
import re

import sqlparse
from sqlparse.sql import IdentifierList, Identifier, Function
from sqlparse.tokens import Keyword
from sqlparse.tokens import Punctuation

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(funcName)s - %(message)s')
logger = logging.getLogger(__name__)


def format_sql(sql):
    """
    1、删除注释
    2、关键词大写
    3、特殊符号处理：回车、换行、制表 等符号用空格代替
    4、空格处理：左右多余空格删除、多个空格合并为一个空格
    5、对with t as (select ...)子句的临时表用反引号包裹，避免t是关键字引发token 解析异常
    :param :sql
    :return:sql
   """
    sql = sqlparse.format(sql, strip_comments=True, keyword_case='upper', identifier_case='lower')
    sql = sql.replace('\r', ' ').replace('\n', ' ').replace('\t', ' ').lstrip().rstrip()
    while '  ' in sql:
        sql = sql.replace('  ', ' ')
    sql = re.sub(r'(WITH|,)( *)(\w+)(\s+AS\s*\(\s*SELECT)', r'\1\2`\3`\4', sql, flags=re.IGNORECASE)
    return sql


def is_catalog_pattern(token):
    # 因python 的 token.normalized 对 catalog.schema.table 格式 解析时，会把 catalog.schema 当表名，错误截断token 所以需要做判断后针对性适配
    pattern_catalog = r'^(\w+)(\.){1}(\w+)(\.){1}(\w+).*$'  # 前半段符合catalog.schema.table 格式
    pattern_subsql = r'.*\(.*SELECT.*\).*'  # 后半段() 的 identifierList 可以指定字段清单，但是不能是子查询
    if bool(re.match(pattern_catalog, token.value)) is True and bool(re.match(pattern_subsql, token.value)) is False:
        return True
    else:
        return False


def parse_identifier(token, last_token, last_token_2, tables):
    # 处理 CREATE 子句
    if (  # CREATE TABLE t AS SELECT
            isinstance(token, Identifier) and
            last_token_2 is not None and
            last_token_2.normalized == 'CREATE' and
            last_token is not None and
            last_token.normalized == 'TABLE'
    ) or (  # CREATE TABLE [IF NOT EXISTS] t AS SELECT
            isinstance(token, Identifier) and
            last_token_2 is not None and
            last_token_2.normalized == 'NOT' and
            last_token is not None and
            last_token.normalized == 'EXISTS'
    ):
        table = {'table': token.normalized, 'type': 'CREATE'}
        tables.append(table)
    
    # 处理 DELETE 子句
    elif (
            last_token_2 is not None and
            last_token_2.normalized == 'DELETE' and
            last_token == 'FROM' and isinstance(token, Identifier)
    ):
        table = {'table': token.normalized, 'type': 'DELETE'}
        tables.append(table)
    
    # 处理 DROP 子句
    elif last_token is not None and last_token.normalized == 'DROP' and isinstance(token, Identifier):
        table = {'table': token.normalized, 'type': 'DROP'}
        tables.append(table)
    
    # 处理 USE 子句
    elif (  # USE DATABASE db 格式
            (last_token is not None and
             last_token.normalized == 'USE' and
             isinstance(token, Identifier)) or
            # USE db 格式
            (last_token_2 is not None and
             last_token_2.normalized == 'USE' and
             last_token is not None and
             last_token.normalized == 'DATABASE'
             and isinstance(token, Identifier))
    ):
        table = {'table': token.normalized, 'type': 'USE'}
        tables.append(table)
    
    # 处理 FROM/JOIN 子句
    elif last_token is not None and last_token.normalized in ['FROM', 'JOIN', 'INNER JOIN', 'CROSS JOIN',
                                                            'LEFT JOIN', 'LEFT OUTER JOIN', 'LEFT INNER JOIN',
                                                            'RIGHT JOIN', 'RIGHT OUTER JOIN', 'RIGHT INNER JOIN',
                                                            'FULL JOIN', 'FULL OUTER JOIN', 'FULL INNER JOIN']:
        # FROM table_a 的写法
        if isinstance(token, Identifier):
            table = {'table': token.normalized, 'type': 'FROM'}
            # 处理 catalog.schema.table 的特殊格式 todo: 更正确的做法是直接返回处理好的自定义normalized值，后续优化
            if is_catalog_pattern(token) is True:
                table = {'table': token.value.split(' ', 1)[0], 'type': 'FROM'}
            
            tables.append(table)
        
        # 处理 FROM  table_a,table_b 的写法
        elif isinstance(token, IdentifierList):
            for identifier in token.get_identifiers():
                # 处理LATERAL 语法，发现LATERAL 关键词 不执行后续操作
                if identifier.normalized in ['LATERAL']:
                    continue
                # 普通表格式
                if '(' not in identifier.value and ')' not in identifier.value:
                    table = {'table': identifier.normalized, 'type': 'FROM'}
                    tables.append(table)
                # 特殊function格式 如unnest
                if '(' in identifier.value and ')' in identifier.value and 'SELECT' in identifier.value:
                    table = {'table': identifier.normalized, 'type': 'FROM'}
                    tables.append(table)
                # catalog.schema.table 的特殊格式
                if is_catalog_pattern(token) is True:
                    table = {'table': token.value.split(' ', 1)[0], 'type': 'FROM'}
                    tables.append(table)
    
    # 处理 WITH t AS 子句
    elif last_token is not None and last_token.normalized == 'WITH':
        # 处理逻辑:
        # (1) 前面已经短路处理了ALTER TABLE t1 SWAP WITH t2，此处WITH 仅表示CET,不考虑SWAP
        #
        # (2) WITH t1 AS (SELECT ...), t2 AS (SELECT ...) 的CET子句中,
        #     sql-pars 会解析为 t1 AS (SELECT ...) 和 t2 AS (SELECT ...) 两个 Identifier
        #     此时只需要解析各个 Identifier 即可
        #
        # (3) 如果SQL 中 的t1、t2 为关键字(非保留字)，sql-pars 无法解析
        #     方案1:使用反引号`` 包裹关键字(目前采用的这个方法)
        #     方案2:整个解析方法重构，提取出WITH 子句来,用字符串模式匹配解析
        #
        # (4) 具体逻辑实现方式:
        #     STEP1: CET子句中, t1 AS (SELECT * FROM t) 作为一个表写入到tables清单, 用后面的子查询解析处理
        #     STEP2: CET子句中, t1 AS (SELECT * FROM t) 中(t,t1)对最后处理with 子句逻辑时清理t1
        
        # 单个temp table
        if isinstance(token, Identifier):
            table = {'table': token.value, 'type': 'WITH'}  # 这里不能用token.normalized 因为 normalized会截断
            tables.append(table)
        # 多个temp table
        elif isinstance(token, IdentifierList):
            for identifier in token.get_identifiers():
                table = {'table': identifier.value, 'type': 'WITH'}
                tables.append(table)
        
        # 避免WITH t AS SELECT 语句中 t 是关键词/保留字的情况，如WITH new AS SELECT ....
        # 同时要考虑 WITH t1 AS (), t2 AS () 中的t1、t2 任意一个是 关键词/保留字的情况
        
        else:
            raise ValueError('WITH subquery parse error: {token}'.format(token=token.value))
    
    # 处理 INSERT OVERWRITE/INTO 子句
    elif (
            last_token_2 is not None and
            last_token_2.normalized == 'INSERT' and
            last_token is not None and
            last_token.normalized in ['INTO', 'OVERWRITE']
    ):
        # INSERT INTO table SELECT 格式，不带字段清单)
        if isinstance(token, Identifier):
            table = {'table': token.normalized.split(' ', 1)[0].split('(', 1)[0], 'type': 'INSERT'} # INSERT INTO db.table(id,name)格式
            
            if is_catalog_pattern(token) is True:  # 处理 catalog.schema.table 的特殊格式
                table = {'table': token.value.split(' ', 1)[0], 'type': 'INSERT'}
            
            tables.append(table)
        
        # INSERT INTO table(column list ) SELECT 格式，带字段清单
        elif isinstance(token, Function):
            table = {'table': token.tokens[0].normalized, 'type': 'INSERT'}
            tables.append(table)
    
    # 处理 UPDATE 子句
    elif last_token is not None and last_token.normalized == 'UPDATE' and isinstance(token, Identifier):
        table = {'table': token.normalized, 'type': 'UPDATE'}
        tables.append(table)
    
    # 处理 TRUNCATE 子句
    elif last_token is not None and last_token.normalized == 'TRUNCATE' and isinstance(token, Identifier):
        table = {'table': token.normalized, 'type': 'TRUNCATE'}
        tables.append(table)
    
    # 处理 SELECT 子句
    elif last_token is not None and last_token.normalized == 'SELECT':
        # 处理 SELECT 子句中的嵌套子查询 (单个identifier)
        if isinstance(token, Identifier):
            if '(' in token.value and ')' in token.value and 'SELECT' in token.value:
                table = {'table': token.normalized, 'type': 'FROM'}
                tables.append(table)
        
        # 处理 SELECT 子句中的嵌套子查询 (多个identifier)
        elif isinstance(token, IdentifierList):
            for identifier in token.get_identifiers():
                if '(' in identifier.value and ')' in identifier.value and 'SELECT' in identifier.value:
                    table = {'table': identifier.normalized, 'type': 'FROM'}
                    tables.append(table)
    
    # 处理 WHERE 子句 ( WHERE 子句中的嵌套子查询)
    elif token.normalized.startswith('WHERE'):
        for sub_token in token.get_sublists():
            if '(' in sub_token.value and ')' in sub_token.value and 'SELECT' in sub_token.value:
                table = {'table': sub_token.normalized, 'type': 'FROM'}
                tables.append(table)
    
    # todo 处理 upsert 子句 （ starrocks 没有 ）
    
    return tables


def parse_subquery(tables):
    tables_with = []
    
    # 因为会在遍历tables的时候修改tables的内容，所以每次修改都要退出循环重新来过
    while any(bool(re.search(r'(.*SELECT.*)', any_item['table'])) for any_item in tables):
        
        for item in tables:
            
            # 如果()里面是嵌套查询，则（1）解析子查询（2）子查询解析结果写入tables（3）删除嵌套查询identifiers
            # if '(' in item['table'] and ')' in item['table'] and 'SELECT' in item['table']:
            
            if bool(re.search(r'(.*SELECT.*)', item['table'])):
                # 找到嵌套子查询中的sql
                subsql = item['table'][item['table'].find('(') + 1:item['table'].rfind(')')]
                # 解析嵌套子查询的对象
                subsql_statment = sqlparse.parse(format_sql(subsql))[0]
                subsql_tables = extract_tablename(subsql_statment)
                tables.extend(subsql_tables)
                # 记录with 语句的 表映射关系
                if bool(re.search(r'.* AS (.*SELECT.*)', item['table'])):
                    # replace_outer = {'replace_outer': item['table'].split(' ')[0]}
                    for subsql_table in subsql_tables:
                        # t AS (SELECT...) \ t(columns) AS (SELECT...) \ t (columns) AS (SELECT...) 带空格
                        item_with = {'outer': item['table'].split(' ')[0].split('(')[0],
                                     'inner': subsql_table['table']}
                        tables_with.append(item_with)
                
                tables.remove(item)
                # 循环体改变，需要跳出循环重新执行
                break
    
    # with 语句的表映射关系去重
    tables_with = [dict(t) for t in {frozenset(item.items()) for item in tables_with}]
    
    return tables_with, tables


def extract_tablename(statment):
    # 处理特殊写法
    # 首先识别并特殊处理 ALTER TABLE t1 SWAP WITH T2 的SWAP子句
    if (any(token.normalized == 'ALTER' for token in statment.tokens) and
            any(token.normalized == 'TABLE' for token in statment.tokens) and
            any(token.normalized == 'WITH' for token in statment.tokens) and
            any('SWAP' in token.value.upper() for token in statment.tokens)):
        tables = extract_tablename_alter_table_swap_with(statment)
        return tables
    
    tables = []
    
    # 遍历所有tokens，解析 Identifier 中的表名
    for token in statment.tokens:
        
        if token.is_whitespace or token.ttype in (Keyword, Keyword.DDL, Keyword.DML, Keyword.CTE):
            continue
        
        # STEP（1）:获取当前token的上一个token
        
        token_prev = statment.token_prev(statment.token_index(token), skip_ws=True, skip_cm=True)
        last_token = token_prev[1] if token_prev is not None else None
        token_prev = statment.token_prev(statment.token_index(last_token), skip_ws=True, skip_cm=True) if last_token is not None else None
        last_token_2 = token_prev[1] if token_prev is not None else None
        del token_prev
        
        # STEP（2）:解析Identifier 或 IdentifierList
        
        tables = parse_identifier(token=token,
                                  last_token=last_token,
                                  last_token_2=last_token_2,
                                  tables=tables)
    
    # STEP（3）:处理嵌套子查询，处理select/from/join/with/where 子句中格式为(SELECT...)的子查询
    tables_with, tables = parse_subquery(tables=tables)
    
    # STEP(4): 表名格式化，去除表名中包含的反单引号（back-quote）
    for item in tables:
        item['table'] = item['table'].replace('`', '')
    
    for item in tables_with:
        item['outer'] = item['outer'].replace('`', '')
        item['inner'] = item['inner'].replace('`', '')
    
    # STEP(5): 处理with 子句
    # 举例如下:
    # with  t1 as (select * from t),t2 as (select * from  t1),t3 as (select * from  t1,t2), select * from t3
    # 这个SQL有三个with 子句 需要做替换处理:
    # 第一个: 用t1,t2 替换 t3 (实际操作上是添加t1、t2 删除t3,下同 )
    # 第二个: 用t1    替换 t2
    # 第三个: 用t     替换 t1
    # 最终,有多个重复的t 在 tables列表里，执行去重得到最后结果
    # 这个例子的逻辑表述:
    # STEP1: 获取处理对象A，获取逻辑为，在 tables_with 所有元素中，获取一个 outer 不在其它元素 inner 中(即最外层调用)
    # STEP2: 在tables 清单中，加入 A.inner（多个）
    # STEP3: 在tables 清单中，删掉 A.outer
    #
    # 丢，想复杂了。之前解析都做完了,只需要在tables 删除掉 tables_with 的所有 outer 对象就行了
    
    while any(item_with['outer'] == item['table'] for item_with in tables_with for item in tables):
        # for循环中对循环对象做了编辑操作，外层包装一个循环判断，避免逻辑没有执行完退出循环
        for item_with in tables_with:
            for item in tables:
                if item['table'] == item_with['outer']:
                    tables.remove(item)
    # 去重
    tables = [dict(t) for t in {frozenset(item.items()) for item in tables}]
    
    return tables


def extract_tablename_alter_table_swap_with(statment):
    __tables = []
    for token in statment.tokens:
        
        if (token.is_whitespace or
                token.ttype is Keyword or
                token.ttype is Keyword.DDL or  # 判断是否是ALTER
                token.ttype is Keyword.DML or
                token.ttype is Keyword.CTE or  # 判断是否是WITH
                token.ttype is Punctuation or
                token.normalized == 'SWAP' or
                token.normalized == 'swap'):  # SWAP 不是KeyWord 需要判断
            continue
        else:
            __table = {'table': token.normalized, 'type': 'SWAP'}
            __tables.append(__table)
    
    return __tables
