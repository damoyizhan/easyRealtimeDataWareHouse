# -*- coding: utf-8 -*-
# **************************************************************************
# author:ZHH
# function: parse yaml style  job config
# #*************************************************************************
import yaml


def parse_job(file_path=None):
    with open(file_path, 'r') as file:
        config = yaml.safe_load(file)
        
    print(config)
    return config
