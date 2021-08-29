# -*- coding: utf-8 -*-
"""
Created on Fri May  7 16:13:23 2021

@author: conde
"""

import os
import psutil
# import explore
import pandas as pd
import numpy as np
# import general_functions
# import os

# TEST = "TEST"

# print(TEST)

# print(psutil.virtual_memory().availble * 100 / psutil.virutal_memory().total)

# print(psutil.virtual_memory().percent)

# df_dict = explore.read_in_data(explore.LEAGUE_DATA_DIRECTORY)
# proj_df = df_dict['ProjPointsData']

# def check(df):
    
#     print(psutil.virtual_memory().percent)
    
#     new = df.copy()
    
#     print(psutil.virtual_memory().percent)
    
# check(proj_df)

# print(psutil.virtual_memory().percent)
# print(psutil.virtual_memory().available / psutil.virtual_memory().total)





class test():
    
    second_param = "Second"
    
    def __init__(self, main_param, second_param):
        
        self.main_param = main_param
        
        
    def check_main_param(self):
        return self.main_param
    
    def check_second_param(self):
        return test.second_param
    
test_this = test("Check", "Second")
test_second = test("Check", "Second")

test.second_param = "New"


print(test_second.check_second_param())
print(test_second.second_param)