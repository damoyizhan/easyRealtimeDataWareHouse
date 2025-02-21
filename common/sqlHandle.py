def get_primary_key(self, source_schema, source_table):
    sql = """SELECT column_name
             FROM   INFORMATION_SCHEMA.`KEY_COLUMN_USAGE`
             WHERE  table_name      ='{source_table}'
             AND    TABLE_SCHEMA    ='{source_schema}'
             AND    CONSTRAINT_NAME ='PRIMARY'
          """.format(source_table=source_table, source_schema=source_schema)
    self.cursor.execute(sql)
    r_list = self.cursor.fetchall()
    if len(r_list) == 0:
        raise Exception("error!!! NO  PRIMARY KEY")
    primary_key = r_list[0][0]
    return primary_key


    # 获取临时表主键最小值
def get_min_primary_key_value(self, source_table, primary_key):
        presto_conn = self.create_presto_conn()
        sql = """ select min({primary_key}) from {source_table}  """.format(source_table=source_table, primary_key=primary_key)
        cursor = presto_conn.cursor()
        cursor.execute(sql)
        cnt_list = list(cursor.fetchall())
        cursor.close()
        presto_conn.close()
        self.logger.info("获取临时表主键最小值:%s" % cnt_list[0][0])
        return cnt_list[0][0]

    # 获取临时表主键最大值
def get_max_primary_key_value(self, source_table, primary_key):
        presto_conn = self.create_presto_conn()
        sql = """ select max({primary_key}) from {source_table}  """.format(source_table=source_table, primary_key=primary_key)
        cursor = presto_conn.cursor()
        cursor.execute(sql)
        cnt_list = list(cursor.fetchall())
        cursor.close()
        presto_conn.close()
        self.logger.info("获取临时表主键最大值:%s" % cnt_list[0][0])
        return cnt_list[0][0]


def get_min_etl_time(self, source_table):
    sql = """select min(etl_time) from {source_table}""".format(source_table=source_table)
    self.cursor.execute(sql)
    r_list = list(self.cursor.fetchall())
    min_etl_time = r_list[0][0]
    return min_etl_time


def get_table_index(self, source_table):
    sql = """show index  from {source_table}""".format(source_table=source_table)
    self.cursor.execute(sql)
    index_list = self.cursor.fetchall()
    return index_list


def get_table_column_list(self, schema, table):
    """
    读取ods表结构
    转换为List的原因：List的遍历是有序的
    """
    sql = """SELECT lower(column_name) AS column_name,data_type ,column_type
                 FROM   information_schema.columns
                 WHERE  table_name  ='{table}'
                 AND    table_schema='{schema}'
              """.format(table=table.upper(), schema=schema.upper())
    self.cursor.execute(sql)
    return_list = list(self.cursor.fetchall())
    column_list = []
    for row in return_list:
        column_dict = {'column_name': row[0], 'data_type': row[1], 'column_type': row[2]}
        column_list.append(column_dict)
    return column_list


def get_database_time(self):
    sql = """ select unix_timestamp(current_timestamp(3)) * 1000 """
    # 校验连接是否存在
    if self.is_connected():
        for i in range(2):
            try:
                self.cursor.execute(sql)
                database_time_list = list(self.cursor.fetchall())
                break
            except Exception as e:
                time.sleep(2)
                self.logger.error(e)
                if i >= 1:
                    raise Exception("重试2次后失败")
    database_time = database_time_list[0][0]
    return database_time
