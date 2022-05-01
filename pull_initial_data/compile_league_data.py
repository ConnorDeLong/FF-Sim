
'''
Provides functions that compiles all the data into a dictionary BESIDES the Projected Point data
which takes significantly longer to pull. Also provides functionality to write 
each dataframe as a csv to a specified location
'''

import pandas as pd
from functools import reduce
from os import listdir
from os.path import join
import time

from api_data import ApiData


def extracted_leagues(folder: str) -> pd.DataFrame:
    """ 
    Pulls the leagues/seasons that have been fully extracted 
    (i.e. the league/season is represented in all subfolders).
    """
    
    subfolders = [f for f in listdir(folder)]
    
    all_dfs = []
    for subfolder in subfolders:
        path = join(folder, subfolder)
        files = [f for f in listdir(path)]
        
        leagues = []
        for file in files:
            season_index = file.find('-') + 1
            league_index = season_index + 5
            
            season = int(file[season_index:season_index + 4])
            league = int(file[league_index:file.find('.')])
            
            leagues.append([season, league])
            
        df_leagues = pd.DataFrame(leagues, columns=['season_id', 'league_id'])
        all_dfs.append(df_leagues)
    
    df_merged = reduce(lambda  left,right: pd.merge(left,right,on=['season_id', 'league_id'],
                                                how='inner'), all_dfs)

    return df_merged


def remaining_leagues(leagues_found_path: str, extracted_leagues_folder: str) -> pd.DataFrame:
    """ Pulls remaining leagues found that have not been extracted. """
    
    df_leagues_found = pd.read_csv(leagues_found_path)
    df_leagues_found.rename(columns={'seasonId': 'season_id', 'leagueId': 'league_id'}, 
                            inplace=True)
    df_extracted_leagues = extracted_leagues(extracted_leagues_folder)
    
    df = pd.merge(df_leagues_found, df_extracted_leagues,
                  on=['season_id', 'league_id'],
                  how='left', indicator=True)

    df = df.loc[df['_merge'] == 'left_only']
    
    return df


def save_league_data(season_leagues: list, folder: str) -> None:
    """ Creates an individual csv for each data element for every league passed. """
    
    for season_league in season_leagues:
        time.sleep(5)
        
        season_id = season_league[0]
        league_id = season_league[1]
        suffix = str(season_id) + '_' + str(league_id)
        
        print('Processing: season_id =', season_id, 'league_id =', league_id)
        
        espn_data = ApiData(season_id, league_id)
        espn_data.pull_all_data()
        

        
#        filenames = ['team_player_scores' + '-' + suffix + '.csv',
#                     'team_scores' + '-' + suffix + '.csv',
#                     'settings' + '-' + suffix + '.csv',
#                     'divisions' + '-' + suffix + '.csv',
#                     'teams' + '-' + suffix + '.csv',
#                     'weeks' + '-' + suffix + '.csv',
#                     ]
        
        files = [[espn_data.team_player_scores, 'team_player_scores'],
                 [espn_data.team_scores, 'team_scores'],
                 [espn_data.settings, 'settings'],
                 [espn_data.divisions, 'divisions'],
                 [espn_data.divisions, 'divisions'],
                 [espn_data.weeks, 'weeks']
                 ]
        
        for file in files:
            df = file[0]
            filename = file[1] + '-' + suffix + '.csv'
            folder_path = join(folder, file[1], filename)
                               
            print(folder_path)
            
            df.to_csv(folder_path, index=False)
            
#        espn_data.team_player_scores.to_csv(join(folder + '/team_player_scores/', filenames[0]))
#        espn_data.team_scores.to_csv(join(folder + '/team_scores/', filenames[1]))
#        espn_data.settings.to_csv(join(folder + '/settings/', filenames[2]))
#        espn_data.divisions.to_csv(join(folder + '/divisions/', filenames[3]))
#        espn_data.teams.to_csv(join(folder + '/teams/', filenames[4]))
#        espn_data.weeks.to_csv(join(folder + '/weeks/', filenames[5]))
        
    return None


def compile_league_data(folder: str) -> pd.DataFrame:
    """ 
    Reads in every extracted file and stacks each into one df
    for each subfolder.
    """
    
    subfolders = [f for f in listdir(folder)]

    df_dict = {}
    for subfolder in subfolders:

        subfolder_path = join(folder, subfolder)
        files = [f for f in listdir(subfolder_path)]

        dfs = []
        for file in files:
            file_path = join(subfolder_path, file)
            df = pd.read_csv(file_path)
            
            dfs.append(df)
            
        df_dict[subfolder] = pd.concat(dfs)
        
    return df_dict



if __name__ == '__main__':
    
    output_folder = '/home/cdelong/Python-Projects/FF-Sim/Simulation-Data/league_data'

    ###########################################################################
    ###################### Scrape and Save League Data ########################
    ###########################################################################
    
    leagues_found_path = 'leagues/Leagues_Found_Cleaned.csv'
    
    df = remaining_leagues(leagues_found_path, output_folder)
    season_leagues = df[['season_id', 'league_id']].values.tolist()

    save_league_data(season_leagues[0:1], output_folder)
    
#    a = 'base/path'
#    b = 'subfolder'
#    c = 'filename.csv'
#    
#    print(join(a, b, c))
    
    
    ###########################################################################
    ######################## Compile Saved League Data ########################
    ###########################################################################
#    
#    df_dict = compile_league_data(output_folder)
#    print(df_dict['team_player_scores'])