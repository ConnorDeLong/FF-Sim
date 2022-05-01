
# https://opensource.com/article/18/1/step-step-guide-git
import pandas as pd
import numpy as np
# from api_data import pull_data


def survivor_challenge(df_matchup_data, week_number):
    """
    Returns a DataFrame containing the remaining teams left in the survivor challenge through
    week specified by the "week_number" param and the loser of that week

    NOTE: CURRENTLY RETURNS AN ERROR IF THE WEEK_NUMBER IS AFTER THE CONTEST HAS ENDED
    """
    df_matchup_data = df_matchup_data.copy()

    df_remaining_teams = df_matchup_data.groupby(['full_name'], as_index=False).size()
    df_remaining_teams = df_remaining_teams['full_name']

    survivor_losers = []

    for week in range(1, week_number + 1):
        df_matchup_data_week = df_matchup_data.loc[df_matchup_data['week_number'] == week].copy()

        df_matchup_data_week = pd.merge(df_remaining_teams, df_matchup_data_week, on=['full_name'], how='left')

        df_matchup_data_week.sort_values(by=['score', 'cum_score'], inplace=True, ascending=True)

        survivor_loser = df_matchup_data_week.iloc[0]
        survivor_loser = survivor_loser['full_name']
        
        survivor_losers.append(survivor_loser)

        df_remaining_teams = df_remaining_teams.loc[df_remaining_teams != survivor_loser]

    return df_remaining_teams, survivor_losers


def _create_matchup_df(matchup_data, playoff_week_start=14):
    """
    Returns a week/matchup level dataframe containing the total scores of each team
    Note that this requires the mMatchup filtered data
    """

    data = []

    for i, matchup_dict in enumerate(matchup_data['schedule']):
        matchup_period = matchup_dict['matchupPeriodId']
        week_number = matchup_dict['matchupPeriodId']
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

        data.append([week_number, matchup_period, teamId_away, score_away, teamId_home, score_home])

    df_matchup = pd.DataFrame(data, columns=['week_number', 'matchup_period', 'teamId_away', 'score_away',
                                             'teamId_home', 'score_home']
                              )

    df_matchup['week_type'] = ['Regular' if w < playoff_week_start else 'Playoffs' \
                               for w in df_matchup['week_number']]

    return df_matchup


def _parse_matchups_dict(matchup_dict: dict, key: str, index: int) -> list:
    ''' 
    Returns 2-D list containing score related data found in the 
    dictionary found in each element of matchup_data['schedule'] for
    either the home or away team. Note that this needs to be 2-D to handle
    matchup periods which contain multiple weeks
    '''
    
    scores = []
    try:
        team_id = int(matchup_dict[key]['teamId'])
        
        for key, val in matchup_dict[key]['pointsByScoringPeriod'].items():
            week_number = int(key)
            score = val
            
            scores.append([index, week_number, team_id, score])
            
        return scores
            
    except:
        return None


def create_matchup_df(matchup_data, playoff_week_start=14):
    """
    Returns a week/matchup level dataframe containing the total scores of each team
    Note that this requires the mMatchup filtered data
    """

    all_home = []
    all_away = []

    for i, matchup_dict in enumerate(matchup_data['schedule']):
        matchup_period = matchup_dict['matchupPeriodId']
        # NOTE: Need a way to properly assign week number. Below doesn't work because
        # 'pointsByScoringPeriod' is only available for weeks that have been played
        # print(week_number, ": ", matchup_dict['home']['pointsByScoringPeriod'].keys())

        # Pull out the scores for the home and away teams for every week
        home = _parse_matchups_dict(matchup_dict, 'home', i)
        if home is not None:
            for week in home:
                week.append(matchup_period)
                all_home.append(week)
        
        away = _parse_matchups_dict(matchup_dict, 'away', i)
        if away is not None:                
            for week in away:
                week.append(matchup_period)
                all_away.append(week)
        
        cols = ['schedule_index', 'week_number', 'team_id', 'score', 'matchup_period']
        df_home = pd.DataFrame(all_home, columns=cols)
        df_away = pd.DataFrame(all_away, columns=cols)
        
        # Rename cols for the merge
        rename_cols = {'team_id': 'teamId_home', 'score': 'score_home'}
        df_home.rename(columns=rename_cols, inplace=True)
        
        rename_cols = {'team_id': 'teamId_away', 'score': 'score_away'}
        df_away.rename(columns=rename_cols, inplace=True)
        
        df = pd.merge(left=df_home, right=df_away,
                      on=['schedule_index', 'week_number'], how='outer')
        
        # This will choose the away team's matchup_period if home doesn't exist
        # Note: There should always be a home team, but this is a safeguard
        df['matchup_period'] = df['matchup_period_x'].combine_first(df['matchup_period_y'])
        df.drop(['matchup_period_x', 'matchup_period_y', 'schedule_index'], axis=1, inplace=True)
        
        df['week_type'] = ['Regular' if w < playoff_week_start else 'Playoffs' \
                                   for w in df['week_number']]

    return df


def expand_matchup_data(matchup_data):
    """ Expands data to the week/team level """

    df_away_data = matchup_data.copy()
    df_home_data = matchup_data.copy()

    rename_a = {'teamId_away': 'team_id', 'score_away': 'score', 'teamId_home': 'team_id_opp', 'score_home': 'score_opp'}
    rename_h = {'teamId_home': 'team_id', 'score_home': 'score', 'teamId_away': 'team_id_opp', 'score_away': 'score_opp'}

    df_away_data.rename(columns=rename_a, inplace=True)
    df_away_data['home_or_away'] = 'away'

    df_home_data.rename(columns=rename_h, inplace=True)
    df_home_data['home_or_away'] = 'home'

    df_expanded = pd.concat([df_home_data, df_away_data])

    # update null values returned due to bye weeks
    df_expanded = df_expanded[df_expanded['team_id'].notnull()]
    df_expanded['team_id_opp'].fillna(-1, inplace=True)
    df_expanded['score_opp'].fillna(0, inplace=True)

    df_expanded['team_id'] = df_expanded['team_id'].astype('int')
    df_expanded['team_id_opp'] = df_expanded['team_id_opp'].astype('int')

    df_expanded.sort_values(['week_number', 'team_id'], inplace=True)

    return df_expanded


def add_win_loss_ind(matchup_data):
    """ Returns Week/Team level dataframe with win, loss, tie indicators added """

    matchup_data = matchup_data.copy()
    wl_conditions = [
        (matchup_data['score'] > matchup_data['score_opp']),
        (matchup_data['score'] < matchup_data['score_opp'])
    ]

    matchup_data['win_ind'] = np.select(wl_conditions, [1, 0])
    matchup_data['loss_ind'] = np.select(wl_conditions, [0, 1])

    tie_conditions = [
        (matchup_data['score'] == matchup_data['score_opp']),
        (matchup_data['score'] != matchup_data['score_opp'])
    ]

    matchup_data['tie_ind'] = np.select(tie_conditions, [1, 0])

    # need a metric that combines wins and ties (1 for win and .5 for tie) to properly sort rankings
    matchup_data['total_wins'] = matchup_data['win_ind'] + .5 * matchup_data['tie_ind']

    return matchup_data


def add_all_play(matchup_data):
    """ Returns Week/Team level dataframe with all play wins and losses added """

    matchup_data = matchup_data.copy()

    number_of_teams = len(matchup_data.groupby(['team_id'], as_index=False).size().index)

    # sorting by UNIQUE week_number/scores to handle multi team ties more efficiently
    unique_week_scores = matchup_data.groupby(['week_number', 'score'], as_index=False).size()
    unique_week_scores.sort_values(by=['week_number', 'score'], inplace=True, ascending=True)

    # represents the number of scores <= each team in the given week
    unique_week_scores['cum_sum'] = unique_week_scores.groupby(['week_number'])['size'].cumsum()

    # creates All Play metric that incorporates ties to sort standings properly
    unique_week_scores['add_tie_amount'] = 0
    unique_week_scores.loc[unique_week_scores['size'] > 1, 'add_tie_amount'] = 1 / unique_week_scores['size']
    unique_week_scores['all_play_wins'] = unique_week_scores['cum_sum'] - unique_week_scores['size'] + \
                                          unique_week_scores['add_tie_amount']

    unique_week_scores['all_play_wins_int'] = unique_week_scores['cum_sum'] - unique_week_scores['size']
    unique_week_scores['all_play_ties_int'] = unique_week_scores['size'] - 1

    keep_vars_for_merge = ['week_number', 'score', 'all_play_wins', 'all_play_wins_int', 'all_play_ties_int']
    matchup_data = pd.merge(matchup_data, unique_week_scores[keep_vars_for_merge],
                            on=['week_number', 'score'], how='outer')

    matchup_data['all_play_losses'] = number_of_teams - matchup_data['all_play_wins'] - 1
    matchup_data['all_play_losses_int'] = number_of_teams - \
                                          matchup_data['all_play_wins_int'] - \
                                          matchup_data['all_play_ties_int'] - 1

    matchup_data.sort_values(by=['week_number', 'team_id'], inplace=True, ascending=True)

    return matchup_data


def add_cum_metrics(matchup_data, by_group=None, cum_group=None):
    """ Returns Week/Team level dataframe with cumulative metrics added to it """

    if by_group is None:
        by_group = ['team_id']

    if cum_group is None:
        cum_group = ['week_number']
    
    cum_metrics_dict = {'score': 'cum_score', 'win_ind': 'cum_wins', 'all_play_wins': 'cum_all_play_wins',
                        'all_play_losses': 'cum_all_play_losses', 'tie_ind': 'cum_ties', 'loss_ind': 'cum_losses',
                        'total_wins': 'cum_total_wins', 'all_play_wins_int': 'cum_all_play_wins_int',
                        'all_play_losses_int': 'cum_all_play_losses_int',
                        'all_play_ties_int': 'cum_all_play_ties_int', 'score_opp': 'cum_score_opp'}

    matchup_data = matchup_data.copy()

    by_cum_group = by_group + cum_group

    matchup_data.sort_values(by=by_cum_group, inplace=True)

    for cum_metric, new_col_name in cum_metrics_dict.items():
        matchup_data[new_col_name] = matchup_data.groupby(by_group)[cum_metric].cumsum()

    return matchup_data


def merge_on_team_data(matchup_data, team_df):
    """ Returns Week/Team level dataframe with team information merged on """

    matchup_data = matchup_data.copy()
    team_df = team_df.copy()

    matchup_data = pd.merge(matchup_data, team_df, on=['team_id'])
    matchup_data.sort_values(by=['week_number', 'team_id'], inplace=True, ascending=True)

    return matchup_data


def add_standings(matchup_data, week_number, rank_metrics_by_week_range=None):
    """ Returns one week of team level data that includes the standings for that week """

    if rank_metrics_by_week_range is None:
        rank_metrics_by_week_range = {'1-12': [['cum_total_wins', 'cum_score'], [False, False]]}

    matchup_data = matchup_data.copy()
    matchup_data = matchup_data.loc[matchup_data['week_number'] == week_number]

    number_of_teams = len(matchup_data.groupby(['team_id'], as_index=False).size().index)

    columns = list(matchup_data.columns)
    columns.append('standings')

    # creating a shell to stack each sorted ranking range
    df_matchup_data_stacked = pd.DataFrame(columns=columns)

    for rank_range, rank_sort_list in rank_metrics_by_week_range.items():
        rank_range_lower = int(rank_range[0:rank_range.find('-')].strip())
        rank_range_upper = int(rank_range[rank_range.find('-') + 1:].strip())

        # create list of all the ranks that will be calculated in the loop
        rank_list = []
        for i in range(rank_range_lower, rank_range_upper + 1):
            rank_list.append(i)

        # create an index number to filter out the remaining matchup data
        index_upper = rank_range_upper - rank_range_lower + 1

        rank_metric_list = []
        ascending_indicator_list = []
        for k in range(len(rank_sort_list[0])):
            rank_metric_list.append(rank_sort_list[0][k])
            ascending_indicator_list.append(rank_sort_list[1][k])

        ascending_indicator_tuple = tuple(ascending_indicator_list)

        # sort the remaining matchup data according to current tiers' metrics
        matchup_data.sort_values(rank_metric_list, ascending=ascending_indicator_tuple, inplace=True)

        df_append = matchup_data.iloc[:index_upper].copy()

        df_append.reset_index(inplace=True)

        for i in range(0, index_upper):
            df_append.loc[i, 'standings'] = rank_list[i]

        # filter out the matchup data used for df_append so it doesn't get used in the next iteration
        if rank_range_upper < number_of_teams:
            matchup_data = matchup_data.iloc[index_upper:]

        df_matchup_data_stacked = df_matchup_data_stacked.append(df_append)

    df_matchup_data_stacked['standings'] = df_matchup_data_stacked['standings'].astype('int')
    df_matchup_data_stacked.reset_index(inplace=True)

    return df_matchup_data_stacked


def add_update_additional_metrics(df_matchup_data):
    """ Returns Week/Team level dataframe with additional metrics specified below added to it """

    df_matchup_data = df_matchup_data.copy()
    df_matchup_data['cum_wlt'] = df_matchup_data['cum_wins'].astype(str) \
                                 + "-" + df_matchup_data['cum_losses'].astype(str) \
                                 + "-" + df_matchup_data['cum_ties'].astype(str)

    df_matchup_data['cum_all_play_wlt_int'] = df_matchup_data['cum_all_play_wins_int'].astype(str) \
                                              + "-" + df_matchup_data['cum_all_play_losses_int'].astype(str) \
                                              + "-" + df_matchup_data['cum_all_play_ties_int'].astype(str)

    df_matchup_data['cum_score'] = df_matchup_data['cum_score'].round(decimals=2)
    df_matchup_data['cum_score_opp'] = df_matchup_data['cum_score_opp'].round(decimals=2)

    df_matchup_data['cum_score_per_week'] = df_matchup_data['cum_score'] / df_matchup_data['week_number']
    df_matchup_data['cum_score_per_week'] = df_matchup_data['cum_score_per_week'].round(decimals=2)

    df_matchup_data['cum_score_opp_per_week'] = df_matchup_data['cum_score_opp'] / df_matchup_data['week_number']
    df_matchup_data['cum_score_opp_per_week'] = df_matchup_data['cum_score_opp_per_week'].round(decimals=2)

    df_matchup_data['cum_all_play_wins_per_week'] = df_matchup_data['cum_all_play_wins'] / \
                                                    df_matchup_data['week_number']
    df_matchup_data['cum_all_play_wins_per_week'] = df_matchup_data['cum_all_play_wins_per_week'].round(decimals=1)

    return df_matchup_data


def add_all_standings(df_matchup_data, rank_metrics_by_week_range=None):
    """ Returns Week/Team level dataframe that includes week level standings """

    if rank_metrics_by_week_range is None:
        rank_metrics_by_week_range = {'1-12': [['cum_total_wins', 'cum_score'], [False, False]]}

    df_matchup_data = df_matchup_data.copy()
    
    reg_season = df_matchup_data.loc[df_matchup_data['week_type'] == 'Regular']
    weeks = reg_season[['week_number']].sort_values(by='week_number', inplace=False, ascending=False)
    num_weeks_reg_season = weeks['week_number'].tolist()[0]

    columns = list(df_matchup_data.columns)
    columns.append('standings')

    # Shell for stacking
    df_matchup_data_stacked = pd.DataFrame(columns=columns)

    for week_number in range(1, num_weeks_reg_season + 1):
        df_matchup_data_w_standings = add_standings(df_matchup_data, week_number, rank_metrics_by_week_range)

        df_matchup_data_stacked = df_matchup_data_stacked.append(df_matchup_data_w_standings)

    df_matchup_data_stacked.reset_index(inplace=True, drop=True)

    return df_matchup_data_stacked


def pull_standings(matchup_data, playoff_week_start, rank_metrics_by_week_range=None):
    """ return standings through all current weeks available and current standings"""

    if rank_metrics_by_week_range is None:
        rank_metrics_by_week_range = {'1-12': [['cum_total_wins', 'cum_score'], [False, False]]}

    df_matchup_data = create_matchup_df(matchup_data, playoff_week_start=playoff_week_start)
    df_expanded_matchup = expand_matchup_data(df_matchup_data)
    df_matchup_data_w_wl = add_win_loss_ind(df_expanded_matchup)
    df_matchup_data_w_all_play = add_all_play(df_matchup_data_w_wl)
    df_matchup_data_w_cum = add_cum_metrics(df_matchup_data_w_all_play)
    df_updated_matchup_data = add_update_additional_metrics(df_matchup_data_w_cum)
    df_final = add_all_standings(df_updated_matchup_data,
                                 rank_metrics_by_week_range=rank_metrics_by_week_range)
    
    # This needs done first in order to retain these vars 
    rename_vars = {'all_play_wins': 'all_play_wlt_points'
                   , 'cum_all_play_wins': 'cum_all_play_wlt_points'}
    df_final.rename(columns=rename_vars, inplace=True)
    
    # These conflict with other vars that need renamed for the DB
    df_final.drop(['all_play_losses' ,'cum_all_play_losses'], axis=1, inplace=True)
    
    rename_vars = {
    'total_wins': 'wlt_points',  
    'all_play_wins_int': 'all_play_wins', 
    'all_play_losses_int': 'all_play_losses', 
    'all_play_ties_int': 'all_play_ties', 
    'cum_total_wins': 'cum_wlt_points', 
    'cum_all_play_wins_int': 'cum_all_play_wins', 
    'cum_all_play_losses_int': 'cum_all_play_losses', 
    'cum_all_play_ties_int': 'cum_all_play_ties', 
    'cum_all_play_wins_per_week': 'cum_all_play_wlt_points_per_week', 
    'cum_wlt': 'record', 
    'cum_all_play_wlt_int': 'all_play_record'
    }
    df_final.rename(columns=rename_vars, inplace=True)

    return df_final


if __name__ == '__main__':
    
    pd.options.display.max_columns = None
    pd.options.display.max_rows = 100
    
    from api_data import ApiData
    espn_data = ApiData(2019, 24693394)
    matchup_data = espn_data._pull_raw_matchup()

#    check_dict = matchup_data['schedule'][65]
    
#    check = _parse_matchups_dict(check_dict, 'away', 1)
#    check[0].append(1)
#    print(check)
    
    df_matchup_data = create_matchup_df(matchup_data, playoff_week_start=14)
    df_expanded = expand_matchup_data(df_matchup_data)
    print(df_expanded)

    
