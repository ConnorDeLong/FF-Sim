'''
Provides functions to pull both Actual Point data (Season/League/Team/Week level)
AND Projected Point data (Season/League/Team/Week/Player level).
IMPORTANT NOTE: The Projected Point data code ultimately needs to be run
separately from the rest due to how long it takes.
'''

import pandas as pd
import numpy as np
import general_functions
from pull_api_data import pull_data
from settings_data import settingsData
from team_data import teamData


#####################################################################################
################### Functions used to create the matchup data #######################
#####################################################################################

def create_matchup_df(matchup_data, playoff_week_start=14):
    """
    Returns a week/matchup level dataframe containing the total scores of each team
    Note that this requires the mMatchup filtered data
    """

    data = []

    for i, matchup_dict in enumerate(matchup_data['schedule']):
        matchup_period = matchup_dict['matchupPeriodId']
        week_number = matchup_dict['matchupPeriodId']
        playoffTierType = matchup_dict['playoffTierType']
        # NOTE: Need a way to properly assign week number. Below doesn't work because
        # 'pointsByScoringPeriod' is only available for weeks that have been played
        # print(week_number, ": ", matchup_dict['home']['pointsByScoringPeriod'].keys())

        # bye weeks cause the "away" key to be missing for the team with a bye
        try:
            teamId_away = int(matchup_dict['away']['teamId'])
            score_away = matchup_dict['away']['totalPoints']
        except KeyError:
            teamId_away = None
            score_away = None

        # applying this to home teams just in case
        try:
            teamId_home = int(matchup_dict['home']['teamId'])
            score_home = matchup_dict['home']['totalPoints']
        except KeyError:
            teamId_home = None
            score_home = None

        data.append([week_number, matchup_period, teamId_away, 
                     score_away, teamId_home, score_home, playoffTierType])

    df_matchup = pd.DataFrame(data, columns=['week_number', 'matchup_period', 'teamId_away', 'score_away',
                                             'teamId_home', 'score_home', 'playoffTierType']
                              )

    df_matchup['week_type'] = ['Regular' if w <= playoff_week_start else 'Playoffs' \
                               for w in df_matchup['week_number']]

    return df_matchup


def expand_matchup_data(matchup_data):
    """ Expands data to the week/team level """

    df_away_data = matchup_data.copy()
    df_home_data = matchup_data.copy()

    rename_a = {'teamId_away': 'teamId', 'score_away': 'score', 'teamId_home': 'teamId_opp', 'score_home': 'score_opp'}
    rename_h = {'teamId_home': 'teamId', 'score_home': 'score', 'teamId_away': 'teamId_opp', 'score_away': 'score_opp'}

    df_away_data.rename(columns=rename_a, inplace=True)
    df_away_data['home_or_away'] = 'away'

    df_home_data.rename(columns=rename_h, inplace=True)
    df_home_data['home_or_away'] = 'home'

    df_expanded = pd.concat([df_home_data, df_away_data])

    # update null values returned due to bye weeks
    df_expanded = df_expanded[df_expanded['teamId'].notnull()]
    df_expanded['teamId_opp'].fillna(-1, inplace=True)
    df_expanded['score_opp'].fillna(0, inplace=True)

    df_expanded['teamId'] = df_expanded['teamId'].astype('int')
    df_expanded['teamId_opp'] = df_expanded['teamId_opp'].astype('int')
    
    df_expanded.sort_values(['week_number', 'teamId'], inplace=True)

    return df_expanded


def pull_matchup_data(season_id, league_id, settingsObj):
    """ 
    Pulls the Week/Team level data for a league/year 
    Note: This data is being pulled primarily to be used to validate the Week/Team/Player projection data
    """
    
    params = [["view", "mMatchup"], ["view", "mMatchupScore"]]
    
    raw_matchup_data = pull_data(season_id, league_id, params=params)

    df_matchup_data = create_matchup_df(raw_matchup_data)
    df_expanded_matchup = expand_matchup_data(df_matchup_data)
    
    df_scoring_periods = settingsObj.df_scoring_periods

    # Merge the matchup period and regular season indicator onto the main dataframe
    df_expanded_matchup = pd.merge(df_expanded_matchup, df_scoring_periods,
                                  left_on='week_number', right_on='scoringPeriodId', how='left')
    
    df_expanded_matchup['league_id'] = league_id
    df_expanded_matchup['season_id'] = season_id
    
    keep_columns = ['season_id', 'league_id', 'week_number', 'macthupPeriodId', 'teamId', 'teamId_opp',
                   'score', 'score_opp', 'home_or_away', 'regular_season_ind', 'playoffTierType']
    
    df_expanded_matchup = df_expanded_matchup[keep_columns]
    
    return df_expanded_matchup


##############################################################################
################### CREATE THE PROJECTED DATA ################################
##############################################################################
def create_week_team_player_df(season_id, league_id, week_start=1, week_end=17):
    slotcodes = {
        0 : 'QB', 2 : 'RB', 4 : 'WR',
        6 : 'TE', 16: 'Def', 17: 'K',
        20: 'Bench', 21: 'IR', 23: 'Flex'
    }
    
    data = []
    for week in range(week_start, week_end + 1):

        request_params = (("view", "mMatchup"), ("view", "mMatchupScore"), ("scoringPeriodId", week))

        d = pull_data(season_id, league_id, params=request_params)

        try:            
            for tm in d['teams']:
                tmid = tm['id']
                for p in tm['roster']['entries']:
                    name = p['playerPoolEntry']['player']['fullName']

                    slot = p['lineupSlotId']
                    pos  = slotcodes[slot]

                    # injured status (need try/exc bc of D/ST)
                    inj = 'NA'
                    try:
                        inj = p['playerPoolEntry']['player']['injuryStatus']
                    except:
                        pass

                    # projected/actual points
                    proj, act = None, None
                    for stat in p['playerPoolEntry']['player']['stats']:
                        if stat['scoringPeriodId'] != week:
                            continue
                        if stat['statSourceId'] == 0:
                            act = stat['appliedTotal']
                        elif stat['statSourceId'] == 1:
                            proj = stat['appliedTotal']

                    data.append([week, tmid, name, slot, pos, inj, proj, act])
                    
        except:
            pass
                
    columns = ['week_number', 'team_id', 'player', 'slot', 'position', 
               'status', 'Proj', 'Actual']
                
    data = pd.DataFrame(data, columns=columns)
    
    data['Season'] = season_id
    
    data['Starter Indicator'] = np.where(data['position'] == 'Bench', 0, 1)
                
    return data


def create_league_df(season_id, league_id, week_start=1, week_end=17):
    """ 
    Returns a dataframe containing the initial data necessary for the simulation
    for one league/year.
    
    Note: the settings_data and team_data objects are a bit overkill for this, but I'm using them
    in case there's any additional data that needs to be pulled in the future.
    
    Note: I'm creating files exclusively for Team and Settings data, so these variables will ultimatley be
    removed from this.
    """
    
    df_week_team_player = create_week_team_player_df(season_id, league_id, 
                                                     week_start=week_start, week_end=week_end)
    
    if len(df_week_team_player) == 0:
        return None
    
    else:
        settings_data = settingsData(season_id, league_id)
        team_data = teamData(season_id, league_id)

        df_divisions = settings_data.df_divisions[['divisionId', 'division_name']]
        
        df_team_data = team_data.df_team_data
        df_team_data = pd.merge(df_team_data, df_divisions,
                            left_on='team_divisionId', right_on='divisionId', how='left')

        df_scoring_periods = settings_data.df_scoring_periods
        df_week_team_player = pd.merge(df_week_team_player, df_scoring_periods,
                                      left_on='Week', right_on='scoringPeriodId', how='left')

        df_league_data = pd.merge(df_week_team_player, df_team_data,
                                left_on='Team', right_on='team_id', how='left')

        df_league_data.drop(['team_id', 'scoringPeriodId', 
                             'team_primaryOwnerKey', 'divisionId'], 1, inplace=True)
        df_league_data.rename(columns={'division_name': 'team_division_name'}, inplace=True)

    return df_league_data


def pull_all_leagues_projection_data(season_id, league_ids, index_start, index_end):
    """ Returns Season/Team/Player/Week level dataframe with each players actual and projected points """
    
    num_leagues = len(league_ids)
    
    if index_end > num_leagues:
        print("All Leagues have been processed")
        index_end = num_leagues    
    
    # This creates an empty dataframe that will be used for stacking
    get_cols_for_empty_df = create_league_df(2020, 48347143, week_end=1)
    
    columns = list(get_cols_for_empty_df.columns)
    columns.append('league_found_index')
    columns.append('league_id')
    
    df_league_data = pd.DataFrame(columns=columns)
    
    for i in range(index_start, index_end):
        league_id = league_ids[i]
        
        print(league_id, ": ", i)

        df_append_league_data = create_league_df(2020, league_id, week_end=17)
        
        if df_append_league_data is None:
            pass
        else:
            df_append_league_data['league_found_index'] = i

            df_append_league_data['league_id'] = league_id

            df_league_data = df_league_data.append(df_append_league_data)
    
    return df_league_data


if __name__ == '__main__':
    pd.options.display.max_columns = 100
    # matchup_data = pull_data(2020, 48347143, params=[["view", "mMatchup"], ["view", "mMatchupScore"]])
    # # matchup_data = pull_data(2020, 48347143, params=[["view", "mMatchupScore"]])
    
    # df_matchup_data = create_matchup_df(matchup_data, playoff_week_start=14)
    # df_expanded_matchup = expand_matchup_data(df_matchup_data)
    
    # df_expanded_matchup
    
    # from settings_data import settingsData
    # league = 24693394
    # settingsObj= settingsData(2020, league)
    
    # df_settings = pull_settings_data(settingsObj)
    # df_matchup = pull_matchup_data(2020, league, settingsObj)
    
    # check = create_league_df(2020, 48347143, week_end=1)
    
    matchup_data = create_week_team_player_df(2020, 48347143, week_end=1)

    print(matchup_data)    
    pass
