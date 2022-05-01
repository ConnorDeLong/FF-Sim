'''
Provides classes and functions to process the initial data
'''

import pandas as pd
import general_functions
import os
import numpy as np

LEAGUE_DATA_DIRECTORY = "D:\Simulation-Data"

        
# class DataDictionary():
    
#     def __init__


class BaseData():
    """ 
    Provides baseline functionality and interface requirements for any
    Data object that will be used
    """
    
    def __init__(self, data_directory: (str, list), raw_file_names: list):
        """ Object Initalization:
        
        data_directory...... (str, list, required): Folder location of the data.
                             If there are multiple files spread across multiple folders,
                             a list of those folders needs passed such that each folder
                             aligns with its file in the raw_file_names list. Otherwise,
                             simply pass a string.
        raw_file_names..... (list, required): List containing the file names                            
        """
        
        self.data_directory = data_directory
        self.raw_file_names = raw_file_names
        
        # This will result in data being read in every time a new Data object
        # instance is created. Going to just use the read_in_data method when needed instead.
        # self.raw_df_dict = self._read_in_data(data_directory, raw_file_names)
        
        self.df_dict_names = []
        for raw_file_name in raw_file_names:
            self.df_dict_names.append(self._create_df_name(raw_file_name))
              
    def read_in_data(self, data_directory: (str, list)=None, raw_file_names: list=None) -> dict:
        """ Returns the df from the raw CSV file """
        
        if data_directory is None:
            data_directory = self.data_directory
            
        if raw_file_names is None:
            raw_file_names = self.raw_file_names
            
        self.df_dict_names = []
        for raw_file_name in raw_file_names:
            self.df_dict_names.append(self._create_df_name(raw_file_name))
            
        # return self.raw_df_dict
        return self._read_in_data(data_directory, raw_file_names)
        
    def _read_in_data(self, data_directory: (str, list), 
                      raw_file_names: list) -> dict:
        """ 
        Provides the code needed to read and create a dictionary of DataFrames
        for the files passed. Leaving this in a private method gives flexibility
        over whether this should be created as an instance attribute, or kept purely
        as a method via the public version.
        """
        
        raw_df_dict = {}
        for i, raw_file_name in enumerate(raw_file_names):
            df_name = self._create_df_name(raw_file_name)
            
            if type(data_directory) is str:
                full_file_path = os.path.join(data_directory, raw_file_name)
                raw_df_dict[df_name] = pd.read_csv(full_file_path)
            else:
                full_file_path = os.path.join(data_directory[i], raw_file_name)
                raw_df_dict[df_name] = pd.read_csv(full_file_path)   
        
        return raw_df_dict
    
    def _create_df_name(self, file_name):
        """ 
        Returns only the word preceding the first underscore in the file_name.
        This is used to dynamically create a df name for each csv file.
        """
        
        df_name_index = file_name.find('_')
        
        if df_name_index > -1:
            df_name = file_name[:df_name_index]
        else: 
            df_name = file_name
            
        return df_name

class AllSettingsData(BaseData):
    
    def __init__(self, data_directory: str = None, raw_file_names: str=None):
        
        if data_directory is None:
            data_directory=LEAGUE_DATA_DIRECTORY
            
        if raw_file_names is None:
            raw_file_names = ['SettingsData_Season_League.csv', 
                              'DivisionData_Season_League_Division.csv']
        
        BaseData.__init__(self, data_directory, raw_file_names)
    
    def read_in_data(self, data_directory: (str, list)=None, 
                      raw_file_names: list=None) -> dict:
        """ Returns the df from the raw CSV file """
        return super().read_in_data(data_directory, raw_file_names)
    
    
class AllTeamData(BaseData):
    
    def __init__(self, data_directory: str = None, raw_file_names: str=None):
        
        if data_directory is None:
            data_directory=LEAGUE_DATA_DIRECTORY
            
        if raw_file_names is None:
            raw_file_names = ['TeamData_Season_League_Team.csv']
        
        BaseData.__init__(self, data_directory, raw_file_names)
    
    def read_in_data(self, data_directory: (str, list)=None, 
                      raw_file_names: list=None) -> dict:
        """ Returns the df from the raw CSV file """
        return super().read_in_data(data_directory, raw_file_names)
    
    
class AllScoreData(BaseData):
    
    def __init__(self, data_directory: str = None, raw_file_names: str=None):
        
        if data_directory is None:
            data_directory=LEAGUE_DATA_DIRECTORY
            
        if raw_file_names is None:
            raw_file_names = ['MatchupData_Season_League_Team_Week.csv', 
                              'ProjPointsData_Season_League_Team_Player_Week.csv']
        
        BaseData.__init__(self, data_directory, raw_file_names)
    
    def read_in_data(self, data_directory: (str, list)=None, raw_file_names: list=None) -> dict:
        """ Returns the df from the raw CSV file """
        
        df_dict = super().read_in_data(data_directory, raw_file_names)
        
        # Need to add a season_id to the ProjPoints data for consistency
        proj_data = df_dict['ProjPointsData']
        proj_data['season_id'] = 2020
        
        # return super().read_in_data(data_directory, raw_file_names)
        return df_dict
    
    def create_df_dict(self) -> pd.DataFrame:
        pass

    def find_leagues_w_non_missing_team_weeks(self, df_score: pd.DataFrame, agg_vars: list=None) -> pd.DataFrame:
        """ 
        Returns a df of Season/Leagues that has data for every Season/Team/Week 
        
        agg_vars needs to be ordered in the following manner:
        season id, league id, team id, week number
        """
        
        if agg_vars is None:
            agg_vars = ['season_id', 'league_id', 'teamId', 'week_number']
            
        season_lg_vars = agg_vars[0:2]
        season_lg_team_vars = agg_vars[0:3]
        
        df_score = df_score.copy()
        
        # Agg to the Season/League/Team/Week level to ensure counts are done properly
        initial_agg_df = df_score.groupby(agg_vars, as_index=False).size()
        
        # Get the number of weeks with data by Season/League/Team
        num_lg_team_weeks_df = initial_agg_df.groupby(season_lg_team_vars, as_index=False).size()
        
        # Get the max number of weeks with data by Season/League
        max_weeks_df = num_lg_team_weeks_df.groupby(season_lg_vars, as_index=False)['size'].max()
        max_weeks_df = max_weeks_df.rename(columns={'size': 'max_size'})
        
        # Merge on max weeks with data to primary df and add missing week id
        num_lg_team_weeks_df = pd.merge(num_lg_team_weeks_df, max_weeks_df, on=season_lg_vars)
        num_lg_team_weeks_df['missing_week_ind'] = num_lg_team_weeks_df['size'] - num_lg_team_weeks_df['max_size']
        
        # Aggregate the missing week indicator by Season/League
        id_missing_weeks_df = num_lg_team_weeks_df.groupby(season_lg_vars, as_index=False)['missing_week_ind'].max()
        id_missing_weeks_df.rename(columns={'missing_week_ind': 'missing_week_ind_final'}, inplace=True)
        
        final_df = id_missing_weeks_df[id_missing_weeks_df['missing_week_ind_final'] == 0]
        final_df = final_df[season_lg_vars]
        
        return final_df
        
    def find_leagues_w_complete_weeks(self, df_score :pd.DataFrame, season_lg_wk_vars: list=None) -> pd.DataFrame:
        """ 
        Returns a df of Season/Leagues whose weeks are all consecutive
        (e.g. if a league has data through week 15, there are no missing weeks).
        """
        
        if season_lg_wk_vars is None:
            season_lg_wk_vars = ['season_id', 'league_id', 'week_number']
            
        season_lg_vars = season_lg_wk_vars[0:2]
            
        df_score = df_score.copy()
        
        # Aggregate to the Season/League/Week level
        initial_agg_df = df_score.groupby(season_lg_wk_vars, as_index=False).size()
        initial_agg_df.sort_values(season_lg_wk_vars, inplace=True)
        
        # Create a cumulative metric by Season/League to validate consecutive week numbers
        initial_agg_df['check_week_number'] = 1
        initial_agg_df['check_week_number'] = initial_agg_df.groupby(season_lg_vars, 
                                            as_index=False)['check_week_number'].cumsum()
        
        # Create final metric that flags a consecutive week when = 0
        initial_agg_df['check_cons_weeks'] = (initial_agg_df['week_number'] - 
                                              initial_agg_df['check_week_number'])
        
        # Final df needs to be at the Season/League level 
        final_df = initial_agg_df.groupby(season_lg_vars, as_index=False)['check_cons_weeks'].max()
        
        final_df = final_df[final_df['check_cons_weeks'] == 0]
        final_df = final_df[season_lg_vars]
        
        return final_df
        
    def find_leagues_w_playoff_weeks(self, df_score: pd.DataFrame, season_lg_vars: list=None) -> pd.DataFrame:
        """ Returns a df of Season/Leagues with data through the entire regular season """
        
        if season_lg_vars is None:
            season_lg_vars = ['season_id', 'league_id']
        
        df_score = df_score.copy()
        
        # League should have one week with a 0 for this if it includes the playoffs
        final_df = df_score.groupby(season_lg_vars, as_index=False)['regular_season_ind'].min()
        final_df= final_df[final_df['regular_season_ind'] == 0]
        
        return final_df
    
    def merge_proj_and_matchup_points(self, proj_data, matchup_data):
        """ Returns a dataframe that merges the proj_data and matchup_data"""
    
        agg_proj_points_data = self._aggregate_proj_points_data(proj_data)
        matchup_data = matchup_data.copy()
    
        # keep_vars = ['season_id', 'league_id', 'week_number', 'teamId', 'score']
        # matchup_data = matchup_data[keep_vars]
        
        # Merge on each team's projected data
        left_on_list=['season_id', 'league_id', 'Week', 'Team']
        right_on_list=['season_id', 'league_id', 'week_number', 'teamId']
        df = pd.merge(agg_proj_points_data, matchup_data,
                                            left_on=left_on_list, right_on=right_on_list, how='outer')
        
        df = df.drop(['Week', 'Team'], axis=1)
          
        # Merge on each team opponent's projected data
        rename_dict = {'Actual_Bench': 'Actual_Bench_Opp', 'Actual_Starter': 'Actual_Starter_Opp', 
                       'Proj_Bench': 'Proj_Bench_Opp', 'Proj_Starter': 'Proj_Starter_Opp'}
        agg_proj_points_data.rename(columns=rename_dict, inplace=True)
        
        left_on_list=['season_id', 'league_id', 'Week', 'Team']
        right_on_list=['season_id', 'league_id', 'week_number', 'teamId_opp']
        df = pd.merge(agg_proj_points_data, df, left_on=left_on_list, right_on=right_on_list, how='right')
    
        rename_dict = {'score': 'MatchupData_Score', 'score_opp': 'MatchupData_Score_Opp'}
        df.rename(columns=rename_dict, inplace=True)

        # Update the week number and team ids for records with missing matchup data
        df['week_number'].loc[df['week_number'].isnull()] = df['Week']
        df['teamId'].loc[df['teamId'].isnull()] = df['Team']
    
        drop_vars = ['Week', 'Team', 'season_id']
        df.drop(columns=drop_vars, inplace=True)
        
        df['ProjPoints_Matchup_Score_Diff'] = abs(df['Actual_Starter'] - df['MatchupData_Score'])
        df['ProjPoints_Matchup_Score_Diff'] = df['ProjPoints_Matchup_Score_Diff'].round(2)
        
        return df
    
    def _aggregate_proj_points_data(self, df_score: pd.DataFrame) -> pd.DataFrame:
        """ Returns the Projected Points data aggregated to the Season/League/Week level """
        
        df_score = df_score.copy()
        
        df_score['starter_ind'] = np.where(df_score['Pos'] == 'Bench', 0, 1)
        
        # Including the starter indiciator in order to include starter and bench points
        groupby_vars = ['season_id', 'league_id', 'Week', 'Team', 'starter_ind']
        sum_vars = ['Proj', 'Actual']
        df_score = df_score.groupby(groupby_vars, as_index=False)[sum_vars].sum()
        
        id_vars = ['season_id', 'league_id', 'Week', 'Team']
        df_score = df_score.pivot_table(index=id_vars, columns='starter_ind', aggfunc=sum, fill_value=0)
        
        # Pivot table causes multi-dimensional column names and id_vars become the index
        df_score.columns = ['{}_{}'.format(x[0], 'Starter') if x[1] == 1
                            else '{}_{}'.format(x[0], 'Bench') for x in df_score.columns]
        df_score = df_score.reset_index().rename_axis(None, axis=1)

        
        final_df = df_score
        
        return final_df

if __name__ == '__main__':
    pd.set_option('display.max_columns', None)
    output_dir = r'C:\Users\conde\OneDrive\Documents\Python-Projects\FF-Sim\Output Data'
    ################################################
    ############ Create Settings Data ##############
    ################################################
    # settings_data_obj = AllSettingsData()
    # settings_df = settings_data_obj.read_in_data()['SettingsData']
    # division_df = settings_data_obj.read_in_data()['DivisionData']
    
    ################################################
    ############ Create Team Data ##################
    ################################################
    # team_data_obj = AllTeamData()
    # team_df = team_data_obj.read_in_data()['TeamData']
    
    ################################################
    ############ Create Points Data ################
    ################################################
    points_data_obj = AllScoreData()
    # matchup_df = points_data_obj.read_in_data()['MatchupData']
    # proj_df = points_data_obj.read_in_data()['ProjPointsData']

  
    ################################################
    ################ Check Data ####################
    ################################################
    
    ### Pull Season/Leagues have without any missing weeks ###
    # proj_agg_vars = ['season_id', 'league_id', 'Team', 'Week']
    # season_lgs_no_miss_weeks = points_data_obj.find_leagues_w_non_missing_team_weeks(matchup_df)
    # season_lgs_no_miss_weeks = points_data_obj.find_leagues_w_non_missing_team_weeks(proj_df, proj_agg_vars)
    
    ### Pull Season/Leagues that have data for the entire regular season ###
    # season_lgs_full_reg_season = points_data_obj.find_leagues_w_playoff_weeks(matchup_df)
    # print(season_lgs_full_reg_season)
    
    ### Pull Season/Leagues whose weeks are all consecutive ###
    # season_lgs_all_cons_wks = points_data_obj.find_leagues_w_complete_weeks(matchup_df)
    # print(season_lgs_all_cons_wks)
    
    ### Merge Mathchup data with projected data ###
    # csv_name = 'Compare Projected and Matchup Data.csv'
    # output_path = os.path.join(output_dir, csv_name)
    
    matchup_w_proj = points_data_obj.merge_proj_and_matchup_points(proj_df, matchup_df)
    print(matchup_w_proj)
    # matchup_w_proj.to_csv(output_path, index=False)
    
    
    # check_this = proj_df[proj_df['league_id'] == 24693394]
    
    # csv_name = 'check this.csv'
    # output_path = os.path.join(output_dir, csv_name)
    # check_this.to_csv(output_path, index=False)
    
    ################################################
    ########### Check Transformations ##############
    ################################################
    # print(matchup_df.columns)
    # print(points_data_obj._aggregate_proj_points_data(proj_df).columns)
    # print(proj_df.columns)
    
    
    
    

    
    


