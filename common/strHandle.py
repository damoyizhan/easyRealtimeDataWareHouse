import re
from typing import Literal


def is_valid_table_name(__str, __format: Literal['table', 'schema.table', 'catalog.schema.table', None]):
    """
    判断一个字符串是否满足表命名格式
    1 格式  [catalog.][schema.]table 的
    2 catalog，table，schema 都可以用小写字母、数字、下划线的组合，但是必须以小写字母开始或数字开始
    """
    __pattern = r'^([a-z0-9][a-z0-9_]*\.)?([a-z0-9][a-z0-9_]*\.)?[a-z0-9][a-z0-9_]*$'  # 默认
    if __format == 'catalog.schema.table':
        __pattern = r'^([a-z0-9][a-z0-9_]*\.)?([a-z0-9][a-z0-9_]*\.)?[a-z0-9][a-z0-9_]*$'  # catalog.schema.table的正则表达式
    elif __format == 'schema.table':
        __pattern = r'^([a-z0-9][a-z0-9_]*\.)?[a-z0-9][a-z0-9_]*$'  # schema.table的正则表达式
    elif __format == 'table':
        __pattern = r'^[a-z0-9][a-z0-9_]*$'  # catalog.schema.table的正则表达式
    
    return bool(re.match(__pattern, __str))


def get_string_after_target(input_string, target_string):
    """
    找出符合条件的下一个字符串 example:
        input_string = "Hello, this is a test string."
        target_string = "test"
        return        = string.
    """
    # 找到目标字符串在输入字符串中的位置
    target_index = input_string.find(target_string)
    
    if target_index == -1:
        # 如果目标字符串不在输入字符串中，则返回None
        return None
    
    # 返回目标字符串后面的部分
    return input_string[target_index + len(target_string):]
