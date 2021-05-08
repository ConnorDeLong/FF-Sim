'''
Provides classes and functions to pull Settings related data for a
Season/League/Team
'''

import pandas as pd
import numpy as np
from pull_api_data import pull_data
from general_functions import rearrange_df_columns


class settingsData():
    """ 
    Pulls the relevant league settings 
    
    Note: example of what to use for 'raw_mTeam_data': 
    pull_data(2019, 28056918, params=(("view", "mSettings")))
    
    TODO: update this so that this is part of the class and all that needs passed is the league id and year
    """
    
    def __init__(self, season_id, league_id):
        
        raw_settings_data = pull_data(season_id, league_id, params=(("view", "mSettings")))
        
        self.season_id = season_id
        self.league_id = league_id
        
        schedule_settings = raw_settings_data['settings']['scheduleSettings']
        scoring_settings = raw_settings_data['settings']['scoringSettings']
        status_settings = raw_settings_data['status']
        
        self.league_name = raw_settings_data['settings']['name']
        
        self.num_divisions = len(schedule_settings['divisions'])
        self.df_divisions = self._pull_divisions(raw_settings_data)
        
        self.num_playoff_teams = schedule_settings['playoffTeamCount']
        self.playoff_seeding_tiebreaker = schedule_settings['playoffSeedingRule']
        
        # Not sure what this is at the moment. Usually is just 0
        self.playoffSeedingRuleBy = schedule_settings['playoffSeedingRuleBy']
        self.playoffSeedingRule = schedule_settings['playoffSeedingRule']
        self.num_reg_season_matchups = schedule_settings['matchupPeriodCount']
        
        self.reg_season_matchup_tiebreaker = scoring_settings['matchupTieRule']
        self.playoff_matchup_tiebreaker = scoring_settings['playoffMatchupTieRule']
        
        self.home_team_bonus = scoring_settings['homeTeamBonus']
        
        # Not sure what this is yet - returns "H2H_POINTS"
        self.scoring_type = scoring_settings['scoringType']
        
        
        self.firstScoringPeriod = status_settings['firstScoringPeriod']
        self.finalScoringPeriod = status_settings['finalScoringPeriod']
        
        # NEED THIS
        self.df_scoring_periods = self._pull_scoring_period_lookup(raw_settings_data, 
                                                                   self.num_reg_season_matchups)
    
    def _pull_divisions(self, raw_settings_data):
        """ Return dataframe containing the name and size of each division """
        
        divisions_list = raw_settings_data['settings']['scheduleSettings']['divisions']

        divisions = []
        for division in divisions_list:
            division_name = division['name']
            division_size = division['size']
            divisionId = division['id']

            divisions.append([division_name, division_size, divisionId])
            
        columns = ['division_name', 'division_size', 'divisionId']
        divisions = pd.DataFrame(divisions, columns=columns)

        return divisions
        
    def _pull_scoring_period_lookup(self, raw_settings_data, num_reg_season_matchups):
        """ Returns dataframe containing the scoring period mapped to matchup period """
        
        reg_season_matchups_pds = raw_settings_data['settings']['scheduleSettings']['matchupPeriods']
        
        scoring_pd_list = []
        for matchup_pd, scoring_pds in reg_season_matchups_pds.items():
            
            matchup_pd = int(matchup_pd)   
            for scoring_pd in scoring_pds:
                
                scoring_pd = int(scoring_pd)

                scoring_pd_list.append([scoring_pd, matchup_pd])
                
        columns = ['scoringPeriodId', 'macthupPeriodId']
        df = pd.DataFrame(scoring_pd_list, columns=columns)
        
        week_type_conditions = [
            (df['macthupPeriodId'] <= num_reg_season_matchups),
            (df['macthupPeriodId'] > num_reg_season_matchups)
        ]

        df['regular_season_ind'] = np.select(week_type_conditions, [1, 0])
                
        return df
    
    
def pull_settings_data(settingsObj):
    """ Returns dataframe containing all relevant data for the settings of a league/year """

    st = settingsObj
    
    settings_list = [[st.playoffSeedingRule, st.playoffSeedingRuleBy, st.num_playoff_teams,
                    st.firstScoringPeriod, st.finalScoringPeriod, st.scoring_type, 
                    st.reg_season_matchup_tiebreaker, st.playoff_matchup_tiebreaker, st.home_team_bonus]]
    
    columns = ['settings_playoffSeedingRule', 'settings_playoffSeedigRuleBy', 'settings_num_playoff_teams',
               'settings_firstScoringPeriod', 'settings_finalScoringPeriod', 'settings_scoring_type', 
               'settings_reg_season_matchup_tiebreaker', 'settings_playoff_matchup_tiebreaker', 
               'settings_home_team_bonus']
    
    df = pd.DataFrame(settings_list, columns=columns)
    df['league_id'] = st.league_id
    df['season_id'] = st.season_id
    
    df = rearrange_df_columns(df, ['season_id', 'league_id'])
        
    return df


def pull_divisions_data(settingsObj):
    """ Returns dataframe containing all relvant division data for each league/year """
    
    df = settingsObj.df_divisions
    
    df['league_id'] = settingsObj.league_id
    df['season_id'] = settingsObj.season_id
    
    df = rearrange_df_columns(df, ['season_id', 'league_id'])
    
    return df
    

if __name__ == 'main':
    check_settings = settingsData(2019, 28056918)
    # print(check_settings.df_divisions)
    # check_settings.df_scoring_periods
    
    # league = 24693394
    # settingsObj= settingsData(2020, league)
    # df_settings = pull_settings_data(settingsObj)

    pass
