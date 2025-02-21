# -*- coding: utf-8 -*-
# **************************************************************************
# author:ZHH
# function: logger format
# #*************************************************************************
import logging
import os


# todo 日志文件过期加后缀名 如logfile.log.2025010123
class MyLogger:
    def __init__(self, name=None, log_file=None, log_level='info'):
        """
        初始化Logger类
        :param name: 日志记录器的名称
        :param log_file: 日志文件的路径
        :param log_level: 日志级别
        """
        self.logger = logging.getLogger(name)
        
        self.set_log_level(log_level)
        
        # 设置日志格式
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(lineno)s - %(funcName)s - %(levelname)s : %(message)s')
        
        # 创建控制台处理器并设置格式
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        self.logger.addHandler(console_handler)
        
        # 创建文件处理器并设置格式
        if log_file:
            if not os.path.exists(os.path.dirname(log_file)):
                os.makedirs(os.path.dirname(log_file))
            file_handler = logging.FileHandler(log_file, encoding='utf-8')
            file_handler.setFormatter(formatter)
            self.logger.addHandler(file_handler)
    
    def debug(self, msg):
        """记录DEBUG级别日志"""
        self.logger.debug(msg, stacklevel=2)
    
    def info(self, msg):
        """记录INFO级别日志"""
        self.logger.info(msg, stacklevel=2)
    
    def warning(self, msg):
        """记录WARNING级别日志"""
        self.logger.warning(msg, stacklevel=2)
    
    def error(self, msg):
        """记录ERROR级别日志"""
        self.logger.error(msg, stacklevel=2)
    
    def critical(self, msg):
        """记录CRITICAL级别日志"""
        self.logger.critical(msg, stacklevel=2)
    
    def set_log_level(self, level):
        """设置日志级别"""
        level_dict = {
            "debug": logging.DEBUG,
            "info": logging.INFO,
            "warning": logging.WARNING,
            "error": logging.ERROR,
            "critical": logging.CRITICAL
        }
        if level.lower() in level_dict:
            self.logger.setLevel(level_dict[level.lower()])
        else:
            print(f"Invalid log level: {level}. Defaulting to DEBUG.")
            self.logger.setLevel(logging.DEBUG)
