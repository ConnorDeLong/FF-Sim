# -*- coding: utf-8 -*-
"""
Created on Sun Apr 25 12:26:39 2021

@author: conde
"""

import pandas as pd
from general_functions import get_list_of_files
import os

LEAGUE_DATA_DIRECTORY = "D:\Simulation-Data"

def read_in_data(load_data_dir):
    """ Returns a dictionary containing every dataframe created from each csv found in the load_data_dir """
    
    file_list = get_list_of_files(load_data_dir, extensions_list=['.csv'])
    
    raw_df_dict = {}
    for file in file_list:
        df_name_index = file.find('_')
        
        if df_name_index > -1:
            df_name = file[:df_name_index]
        else: 
            df_name = file

        full_file_path = os.path.join(LEAGUE_DATA_DIRECTORY, file)

        raw_df_dict[df_name] = pd.read_csv(full_file_path)
        
    return raw_df_dict


def merge_proj_and_matchup_data(proj_data, matchup_data):
    """ Returns a dataframe that mergeds the proj_data and matchup_data"""

    # Aggregate the proj_data to be on the same level as the matchup_data
    by_group = ['league_id', 'Week', 'Team']
    agg_proj_points_data = proj_data.groupby(by_group, as_index=False)['Actual'].sum()

    keep_vars = ['season_id', 'league_id', 'week_number', 'teamId', 'score']
    matchup_data = matchup_data[keep_vars]

    left_on_list=['league_id', 'Week', 'Team']
    right_on_list=['league_id', 'week_number', 'teamId']
    df = pd.merge(agg_proj_points_data, matchup_data,
                                        left_on=left_on_list, right_on=right_on_list, how='outer')

    rename_dict = {'Actual': 'ProjPointsData_Score', 'score': 'MatchupData_Score'}
    df.rename(columns=rename_dict, inplace=True)
    
    # Update the week number and team ids for records with missing matchup data
    df['week_number'].loc[df['week_number'].isnull()] = df['Week']
    df['teamId'].loc[df['teamId'].isnull()] = df['Team']

    drop_vars = ['Week', 'Team', 'season_id']
    df.drop(columns=drop_vars, inplace=True)
    
    return df


def add_vars(df):
    """ Adds variables to the merged ProjectPointsData and MatcupData """
    
    df = df.copy()
    
    # Add flags that indicate there is data for each df that was merged
    df['ProjPointsData_Missing'] = 0
    df['ProjPointsData_Missing'].loc[
        df['ProjPointsData_Score'].isnull()] = 1
    
    df['MatchupData_Missing'] = 0
    df['MatchupData_Missing'].loc[
    df['MatchupData_Score'].isnull()] = 1
    
    return df


def check_missing_data(df, missing_data_flag):
    
    df = df[df[missing_data_flag] == 1]
    
    return df


if __name__ == '__main__':
    
    file_list = get_list_of_files(LEAGUE_DATA_DIRECTORY, extensions_list=['.csv'])
    
    print(file_list)
    
    check = pd.read_csv(os.path.join(LEAGUE_DATA_DIRECTORY, 'ProjPointsData_Season_League_Team_Player_Week.csv'))
    
    # check = read_in_data(LEAGUE_DATA_DIRECTORY)
    
    # print (type(check))
    
    pass
