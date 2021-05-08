'''
Provides classes and functions to pull Team related data for a
Season/League/Team
'''

import pandas as pd
import numpy as np
from pull_api_data import pull_data


class teamData():
    """ 
    Pulls relevant team data
    
    Notes:
        - The raw_mTeam_data['members'] data does NOT always include every team, but raw_mTeam_data['teams']
          does
        
    TODO:
        - Some repetitive code between the "_pull_team_members_nicknames" and "_pull_team_division" methods
    """
    
    def __init__(self, season_id, league_id):
        
        raw_mTeam_data = pull_data(season_id, league_id, params=(('view', 'mTeam')))
        
        self.season_id = season_id
        self.league_id = league_id
             
        self.df_members_names = self._pull_team_members_names(raw_mTeam_data)
        self.df_nicknames = self._pull_team_members_nicknames(raw_mTeam_data)
        self.df_divisions = self._pull_team_division(raw_mTeam_data)
        
        self.df_team_data = self._merge_all_data(raw_mTeam_data)
        
    def _pull_team_members_names(self, raw_mTeam_data):
        """ Creates a dataframe containing each team members full name """
        
        members_data = raw_mTeam_data['members']
        
        team_data = []
        for team in members_data:
            
            try:
                team_firstName = team['firstName']
                team_lastName = team['lastName']
                team_fullName = team_firstName + " " + team_lastName
            except:
                team_firstName = np.nan
                team_lastName = np.nan
                team_fullName = np.nan
            
            # id" in this portion of the API data returns the "primaryOwnerKey" from the "teams" section
            team_primaryOwnerKey = team['id']
            
            team_data.append([team_fullName, team_primaryOwnerKey])
            
        columns = ['team_fullName', 'team_primaryOwnerKey']
        df = pd.DataFrame(team_data, columns=columns)
        
        return df

    def _pull_team_members_nicknames(self, raw_mTeam_data):
        """ Creates a dataframe containing each team members nickname """
        
        members_data = raw_mTeam_data['teams']
        
        team_data = []
        for team in members_data:
            
            try:
                team_firstName = team['location']
                team_lastName = team['nickname']
                team_nickName = team_firstName + " " + team_lastName
            except:
                team_firstName = np.nan
                team_lastName = np.nan
                team_nickName = np.nan
                
            # Not sure why, but this isn't always included for a team
            try:
                team_primaryOwnerKey = team['primaryOwner']
            except:
                team_primaryOwnerKey = np.nan
            
            # id" in this portion of the API data returns the "primaryOwnerKey" from the "teams" section
            team_id = team['id']
            
            team_data.append([team_nickName, team_id, team_primaryOwnerKey])
            
        columns = ['team_nickName', 'team_id', 'team_primaryOwnerKey']
        df = pd.DataFrame(team_data, columns=columns)
        
        return df
    
    def _pull_team_division(self, raw_mTeam_data):
        """ Returns dataframe contaiing each teams division Id"""
        teams_data = raw_mTeam_data['teams']
        
        team_data = []
        for team in teams_data:
            
            team_divisionId = team['divisionId']
            team_id = team['id']
            
            # Not sure why, but this isn't always included for a team
            try:
                team_primaryOwnerKey = team['primaryOwner']
            except:
                team_primaryOwnerKey = np.nan
            
            team_data.append([team_divisionId, team_id, team_primaryOwnerKey])
            
        columns = ['team_divisionId', 'team_id', 'team_primaryOwnerKey']
        df = pd.DataFrame(team_data, columns=columns)
        
        return df
    
    def _merge_all_data(self, raw_mTeam_data):
        """ Returns dataframe with team member names and division Id merged together """
        
        df_team_members_names = self._pull_team_members_names(raw_mTeam_data)
        df_team_nicknames = self._pull_team_members_nicknames(raw_mTeam_data)
        df_team_divisions = self._pull_team_division(raw_mTeam_data)
        
        # 'team_primaryOwnerKey' is a redundant merge variable, but including to prevent two versions of it
        df = pd.merge(df_team_nicknames, df_team_divisions, 
                      on=['team_id', 'team_primaryOwnerKey'], how='outer')
        df = pd.merge(df, df_team_members_names, on='team_primaryOwnerKey', how='outer')
        
        return df
    
    
def pull_team_data(teamObj):
    """ Returns dataframe containing all relevant data for each team in a league/year """

    df = teamObj.df_team_data
    
    df['league_id'] = teamObj.league_id
    df['season_id'] = teamObj.season_id
    
    df = df[['season_id', 'league_id', 'team_id', 'team_primaryOwnerKey', 
             'team_fullName', 'team_nickName', 'team_divisionId']]
        
    return df


if __name__ == '__main__':
    # check_team_data = teamData(2020, 48347143)
    # check_team_data.df_team_data
    
    teamObj = teamData(2020, 24693394)
    
    # print(teamObj.df_nicknames)
    # print(teamObj.df_divisions)
    print(teamObj.df_members_names)
    # teamObj.df_team_data
    
    # league = 24693394
    # teamObj = teamData(2020, league)
    # df_team = pull_team_data(teamObj)

    
    pass
    