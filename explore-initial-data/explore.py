

import pandas as pd
import numpy as np
import general_functions
import os

LEAGUE_DATA_DIRECTORY = "D:\Simulation-Data"

def read_in_data(load_data_dir):
    """ Returns a dictionary containing every dataframe created from each csv found in the load_data_dir """
    
    file_list = general_functions.get_list_of_files(load_data_dir, 
                                                    extensions_list=['.csv'])
    
    raw_df_dict = {}
    for file in file_list:
        df_name_index = file.find('_')
        
        if df_name_index > -1:
            df_name = file[:df_name_index]
        else: 
            df_name = file

        full_file_path = os.path.join(load_data_dir, file)

        raw_df_dict[df_name] = pd.read_csv(full_file_path)
        
    return raw_df_dict


def update_proj_data(df):
    """ Add/Update any variables to the Projected data that are needed """
    
    df = df.copy()
    
    if 'Season' in list(df.columns):
        pass
    else:
        df['Season'] = 2020
        
        df = general_functions.rearrange_df_columns(df, ['Season'])
        
    df['Starter Indicator'] = np.where(df['Pos'] == 'Bench', 0, 1)
    
    return df


def compare_proj_and_matchup_points(proj_data, matchup_data):
    """ Returns a dataframe that merges the proj_data and matchup_data"""

    proj_data = proj_data[proj_data['Starter Indicator'] == 1].copy()
    matchup_data = matchup_data.copy()

    # Aggregate the proj_data to be on the same level as the matchup_data
    by_group = ['Season', 'league_id', 'Week', 'Team']
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
    
    df['ProjPoints_Matchup_Score_Diff'] = df['ProjPointsData_Score'] - df['MatchupData_Score']
    df['ProjPoints_Matchup_Score_Diff'] = df['ProjPoints_Matchup_Score_Diff'].round(2)
    df = add_vars(df)
    
    return df


def add_vars(df):
    """ Adds variables to the merged ProjectPointsData and MatcupData """
    
    df = df.copy()
    
    # Add flags that indicate there is data for each df that was merged
    df['ProjPointsData_Missing'] = 0
    df['ProjPointsData_Missing'].loc[
        df['ProjPointsData_Score'].isnull()] = 1
    
    df['MatchupData_Missing'] = 0
    df['MatchupData_Missing'].loc[df['MatchupData_Score'].isnull()] = 1
    
    return df


def check_missing_data(df, missing_data_flag):
    
    df = df[df[missing_data_flag] == 1]
    
    return df


if __name__ == '__main__':
    pass

    pd.options.display.max_columns = 100

    main_data_dict = read_in_data(LEAGUE_DATA_DIRECTORY)
    # print(main_data_dict.keys())
    
    
    # df_proj_point = main_data_dict['ProjPointsData']
    # df_proj_point = update_proj_data(df_proj_point)
    # print(list(df_proj_point.columns))
    # print(df_proj_point.head)    
    
    # df_matchup = main_data_dict['MatchupData']
    # print(list(df_matchup.columns))
    
    # check_merge = compare_proj_and_matchup_points(df_proj_point, df_matchup)
    # print(check_merge.head())
    
    # check_non_missing_data = check_merge[(check_merge['ProjPointsData_Missing'] == 0) &
    #                                       (check_merge['MatchupData_Missing'] == 0)
    #                                       ]
    
    # check_diff = check_non_missing_data.groupby(
    #     ['ProjPoints_Matchup_Score_Diff'], 
    #     as_index=False)['ProjPoints_Matchup_Score_Diff'].count()
    
    # check_diff = check_non_missing_data['ProjPoints_Matchup_Score_Diff'].value_counts()
    
    # print(check_diff)
    # check_non_missing_data.to_csv('CHECK.csv', index=False)

    
    
    
    # check = df_proj_point.groupby(['Pos', 'Starter Indicator'], as_index=False)['Actual'].sum()    
    # print(check)
    

    
    
    pass
