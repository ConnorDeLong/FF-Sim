'''
Finds valid, public leagues that can be used to develop the models

TODO:
    - Need to update this so that it excludes leagues that don't have any data
    despite returning a 200 response
    - Rather than one functioning searching for a valid leagues and adding writing those
    that are valid to a csv, this gets split up into two
'''
import pandas as pd
from pull_api_data import pull_data

FILE_PATH = 'Leagues_Found_Cleaned.csv'

def find_leagues(startId, endId, seasonId, file_path):
        
    active_leagues = []

    for leagueId in range(startId, endId):   
        general_league_data = pull_data(seasonId, league_id=leagueId)
        
        columns = ['leagueId', 'number_of_teams', 'startId', 'endId', 'seasonId', 'status_code']            

        if general_league_data == None:
            pass
        else:
            num_teams = len(general_league_data['members'])

            current_league = [leagueId, num_teams, startId, endId, seasonId, 200]
            
            df_current_league = pd.DataFrame(data=[current_league], columns=columns)
            df_current_league.to_csv(file_path, mode='a', header=False)
            
            active_leagues.append(current_league)
            
    df_current_league = pd.DataFrame(data=active_leagues, columns=columns)
            
    return df_current_league


if __name__ == '__main__':
    # for_start = 61547491
    
    # get_leagues = find_leagues(for_start, for_start + 100000, 2020, FILE_PATH)
    # get_leagues
    pass