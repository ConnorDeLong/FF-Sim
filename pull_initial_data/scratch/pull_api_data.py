'''
Provides functionality to pull data fromt the ESPN API
'''

import requests
import pandas as pd
from general_functions import convert_tuple_to_list, convert_dict_to_list

from ratelimit import limits, sleep_and_retry

pd.options.display.max_columns = None
pd.options.display.max_rows = 100


@sleep_and_retry
@limits(calls=6000, period=600)
def pull_data(season_id, league_id, params=None):
    """ Returns a JSON object containing the data pulled APIs url """

    if params == None:
        params = []
        
    if season_id < 2020:
        url = "https://fantasy.espn.com/apis/v3/games/ffl/leagueHistory/" + \
              str(league_id) + "?seasonId=" + str(season_id)
    else:
        url = "https://fantasy.espn.com/apis/v3/games/ffl/seasons/" + \
              str(season_id) + "/segments/0/leagues/" + str(league_id)
    
    # Passing the dict_params directly to the request_params of the requests.get method was
    # resulting in certain pulls retrieving unspecified data.
    # So, I'm directly applying those parameters to the URL string to prevent this
    # Note: This was likely happening due to duplicate keys being used (e.g. "view") in the dict
    
    if type(params) is tuple:
        params = convert_tuple_to_list(params)
        
    if type(params) is dict:
        params = convert_dict_to_list(params)
    
    for full_param in params:
        param = str(full_param[0])
        param_value = str(full_param[1])
        
        if url.find("?") == -1:
            url = url + "?" + param + "=" + param_value
        else:
            url = url + "&" + param + "=" + param_value
    
    r = requests.get(url)
    
    if r.status_code == 200:
        pass
    else:
        if r.status_code == 429:
            print("429 error")

        return None   
    
    # 2020 url returns JSON object while prior seasonIds return it in a list 
    if season_id < 2020:
        d = r.json()[0]
    else:
        d = r.json()
        
    r.close()
        
    return d

