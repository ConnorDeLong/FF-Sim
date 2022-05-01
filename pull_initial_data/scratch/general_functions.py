'''
Provides functions that can be used for various files for various reasons
'''

import pandas as pd
import os

def get_league_ids(leagues_found_csv='Leagues_Found.csv'):
    """ Returns dataframe containing all of the league IDs found """

    df_leagues_found = pd.read_csv(leagues_found_csv)

    df_leagues_found = df_leagues_found.loc[df_leagues_found['seasonId'] == 2020]

    league_ids = list(df_leagues_found['leagueId'])
    
    return league_ids



def get_list_of_files(load_data_dir, extensions_list=None, include_or_exclude_ext=None):
    """ Returns a list containing all of the excel files found in the load_data_dir """
    # Pulling the original working directory in order to reset it later
    
    if extensions_list is None:
        extensions_list = []
 
    if include_or_exclude_ext is None:
        include_or_exclude_ext = "include"
        
    # This factor determines if the file being checked needs to be included or excluded from the list
    if include_or_exclude_ext == "include":
        include_exclude_factor = 1
    else:
        include_exclude_factor = -1
        
    original_working_directory = os.getcwd()
    
    os.chdir(load_data_dir)
    
    files_list = os.listdir()
    
    os.chdir(original_working_directory)
    
    remove_files = []
    for file in files_list:
        remove_file_bool = 1 * include_exclude_factor
        
        # Checks if the file has any of the extensions in the list
        for ext in extensions_list: 
            file_index = len(ext) * -1
            if file[file_index:] == ext:
                # Override the flag if the file is found
                remove_file_bool = -1 * include_exclude_factor
                break
            
        if remove_file_bool == 1:
            remove_files.append(file)
            
    for remove_file in remove_files:
        files_list.remove(remove_file)
            
    return files_list


def convert_tuple_to_list(tuple_var):
    """ 
    Converts tuple to a list
    Note: This isn't really necessary, but accounts for 1D tuple cases, so i'm using it
    """
    
    list_var = []
    
    # Need to process 1D and 2D tuples differently
    if type(tuple_var[0]) is tuple:
        for value in tuple_var:

            dict_key = value[0]
            dict_value = value[1]

            list_var.append([dict_key, dict_value])
    else:
        dict_key = tuple_var[0]
        dict_value = tuple_var[1]

        list_var.append([dict_key, dict_value])
            
    return list_var


def convert_dict_to_list(dict_var):
    """ Converts dictionary to a 2D list """
    
    list_var = []
    for param, param_value in dict_var.items():
        list_var.append([param, param_value])
        
    return list_var


def rearrange_list_by_val(input_list, move_vals_list):
    """ 
    Moves the 'vals' in the 'list_var' to the front
    
    Note: This is primarily used to rearrange the columns in a dataframe. Due to this, 
    the lists this will be used for will be relatively small, so I'm not worrying about efficiency
    """
    
    move_vals_list_len = len(move_vals_list)
  
    for i in range(1, move_vals_list_len + 1):
        if move_vals_list_len > 0:
            # Start with the back of the list in order to properly arrange the values
            list_index = -1 * i
        else:
            list_index = 0
            
        move_val = move_vals_list[list_index]
        
        for j, match_val in enumerate(input_list):
            if match_val == move_val:
                move_val = input_list.pop(j)
                input_list.insert(0, move_val)
            else:
                pass
                
    return input_list


def rearrange_df_columns(df, move_columns_list):
    """ Returns a dataframe with its columns rearranged according to the passed 'move_columns_list' """
    
    df = df.copy()
    
    df_cols = list(df.columns)
    
    df_cols_new = rearrange_list_by_val(df_cols, move_columns_list)
    
    df = df[df_cols_new]
    
    return df
