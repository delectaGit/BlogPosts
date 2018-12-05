import numpy as np
import pandas as pd
import os.path
import subprocess

Epsilon = 0.0000001 # a small number to avoid division by zero

def read_datafile(filename,date_time_name = "Gmt time",\
    date_format = "%d.%m.%Y %H:%M:%S.%f",remove_zero_activity = False):
    '''
    Description:
        Reads a csv candlestick csv file in a specific format.
        Always use this function to read the files if you want to use
        the utility_funcs or indicators in the package.
    Inputs:
        filename: the address to the csv file
        date_time_name: date and time column name in the csv file
        date_format: the string format that the date and time are stored
        remove_zero_activity: if True, removes rows that have no activity in them
    Output:
        a pandas dataframe
    '''
    assert os.path.isfile(filename), "filename provided does not exist"

    data = pd.read_csv(filename)

    # make all-lower-case, the convention to follow for the rest of the pipeline:
    # remove spaces from column names:
    translate_col = {}
    for col in data.columns:
        if len(col.split()) > 1:
            translate_col[col] = '_'.join(col.lower().split())
    # make all columns lower case
    translate_col = {col:col.lower() for col in data.columns}
    # universally use "Date" for the datetime column name
    translate_col[date_time_name] = "Date"

    data.rename(columns=translate_col,inplace=True)

    try:
        data['Date'] = pd.to_datetime(data['Date'],format=date_format)
    except:
        date_format = "%Y.%m.%d %H:%M:%S.%f"
        data['Date'] = pd.to_datetime(data['Date'],format=date_format)
    if remove_zero_activity:
        cond = data.high != data.low
        data = data[cond]
        data.reset_index(inplace=True)

    return data

def read_many_files(file_list, date_time_name = "Gmt time",\
                    date_format="%d.%m.%Y %H:%M:%S.%f",\
                    add_to_each_df_func = None, func_args=None):
    '''
    Description:
        Reads multiple date files, listed in files_list and appends them into
        a single data frame
    Inputs:
        files_list: a python list containing the full address to all of the files
            that should be read
        date_time_name: the name of the date and time column in the csv file
        date_format: the string format that the date and time are stored
        add_to_each_df_func: a python function that adds some columns to each date DataFrame
            while reading the files. *Function should return the dataframe*, after it does
            some operations or adds columns to it
        func_args: a python dictionary. The keys are files in the file_list, and
            values are the required arguments for the add_to_each_df_func function

    Output:
        a pandas dataframe containing all the rows in the files provided in the
        file_list, with additional columns created by add_to_each_df_func
    '''
    assert len(file_list)> 0, "file list is empty!"

    df_list = []
    for f in file_list:
        df = read_datafile(filename=f,date_format=date_format,date_time_name=date_time_name)

        if add_to_each_df_func != None:
            if func_args != None:
                df = add_to_each_df_func(df,func_args[f])
            else:
                df = add_to_each_df_func(df)
        df_list.append(df)

    result = pd.concat(df_list)

    return result


def rm_tmp_folder(dir_name):
    '''
    Description:
        removes the folder created by TF to store a summary of the network. It
        makes looping over NNs more manageable in terms of memnory!
    Inputs:
        dir_name: the path to the directory that stores the network graph
    '''

    dir_path = dir_name.split("/")
    for xx in dir_path:
        if xx != '':
            assert xx == 'tmp', "cannot remove from folders other than tmp!"
            break
    subprocess.call(["rm", "-rf",dir_name])
