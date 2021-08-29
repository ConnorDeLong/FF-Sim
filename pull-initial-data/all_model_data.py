
'''
Provides functions that compiles all the data into a dictionary BESIDES the Projected Point data
which takes significantly longer to pull. Also provides functionality to write 
each dataframe as a csv to a specified location
'''

import pandas as pd
from settings_data import settingsData, pull_settings_data
from team_data import teamData, pull_team_data, pull_divisions_data
from point_data import pull_matchup_data


def pull_all_leagues_remaining_data(season_id, league_ids, index_start, index_end, league_id_example=48347143):
    """ Pulls the Team, Settings, Division, and Matchup data for every league found for one season """
    
    num_leagues = len(league_ids)
    
    if index_end > num_leagues:
        print("All Leagues have been processed")
        index_end = num_leagues   
    
    # Create the initial datasets using a league that is known to be populated
    teamObj = teamData(season_id, league_id_example)
    settingsObj= settingsData(season_id, league_id_example)

    df_team_stack = pull_team_data(teamObj)
    df_settings_stack = pull_settings_data(settingsObj)
    df_divisions_stack = pull_divisions_data(settingsObj)
    df_matchup_data_stack = pull_matchup_data(season_id, league_id_example, settingsObj)
    
    df_stack_dict = {'df_team_stack': df_team_stack, 'df_settings_stack': df_settings_stack, 
               'df_divisions_stack': df_divisions_stack, 'df_matchup_data_stack': df_matchup_data_stack}
    
    for df_name, df in df_stack_dict.items():
        # This creates an empty dataframe that will be used for stacking
        columns = list(df.columns)
        df_stack_dict[df_name] = pd.DataFrame(columns=columns)
    
    for i in range(index_start, index_end):
        
        league_id = league_ids[i]
        
        print(league_id, ": ", i)
        
        teamObj = teamData(season_id, league_id)
        settingsObj= settingsData(season_id, league_id)

        df_team = pull_team_data(teamObj)
        df_settings = pull_settings_data(settingsObj)
        df_divisions = pull_divisions_data(settingsObj)
        df_matchup_data = pull_matchup_data(season_id, league_id, settingsObj)

        df_dict = {'df_team': df_team, 'df_settings': df_settings, 
                   'df_divisions': df_divisions, 'df_matchup_data': df_matchup_data}

        for df_name, df in df_dict.items():
            if df is None:
                pass
            else:
                df_stack_name = df_name + "_stack"
                df_stack_dict[df_stack_name] = df_stack_dict[df_stack_name].append(df)
                

    return df_stack_dict


def write_df_to_csv(df_dict, file_names, directory):
    """ Writes a set of dataframes to csv files """
    
    if directory[-1] != "/":
        directory = directory + "/"        
    
    list_index = 0
    for df in df_dict.values():
        file_name = file_names[list_index]
        
        if file_name[-4:].lower() != ".csv":
            file_name = file_name + ".csv"
            
        full_path = directory + file_name
        
        df.to_csv(full_path, index=False
                 , mode='a', header=False
                 )
        
        list_index += 1


if __name__ == '__main__':
    # NEED TO UPDATE THIS
    # import find_leagues
    # FILE_PATH = find_leagues.FILE_PATH
    
    # df_leagues_found = pd.read_csv(file)
    # df_leagues_found = df_leagues_found.loc[df_leagues_found['seasonId'] == 2020]
    
    # leagues_found_list = list(df_leagues_found['leagueId'])
    
    # df_dict = pull_all_leagues_remaining_data(2020, leagues_found_list, 0, 1)
    
    # df_dict['df_matchup_data_stack']
    
    # file_names = ['TeamData_Season_League_Team', 'SettingsData_Season_League', 
    #               'DivisionData_Season_League_Division', 'MatchupData_Season_League_Team_Week']
            
    # write_df_to_csv(df_dict, file_names, "/home/cdelong/Python-Projects/FF-Web-App/Simulation-Data/")
    
    pass