#!/usr/bin/env python

import sys
import datetime
import pandas as pd

from PipeBox import query

def create_dataframe(query_object):
    df = pd.DataFrame(query_object,columns=['nite','time_obs','expnum','band','exptime','obstype',
                                            'program','propid','object'])
    return df

def remove_junk(dataframe):
    df = dataframe[(dataframe.object.str.contains('PTC') == False) | 
                   (dataframe.object.str.contains('junk') == False) |
                   (dataframe.object.str.contains('focus') == False)]
    return df

def remove_gap_expnums(dataframe,tdelta=60):
    for key,row in dataframe.iterrows():
        h,mins,secs = row['time_obs'].split(':')
        hours,minutes,seconds = int(h),int(mins),int(float(secs))
        time = datetime.time(hours,minutes,seconds)
        timediff = time - 
        if timediff > tdelta:
            print "remove from dataframe" 

def remove_first(dataframe):
    dataframe = dataframe.fillna('NA')
    grouped = dataframe.groupby(by=['obstype','nite','band','object'])
    first_index_list = []
    for name,group in grouped:
        first_index_list.append(group.index[0])
    new_df = dataframe.drop(dataframe.index[first_index_list])
    return new_df

def remove_satrband(dataframe):
    nor_df = dataframe[(dataframe.band !='r')]
    r_df = dataframe[(dataframe.band == 'r') & (dataframe.exptime >= 15)]
    df = pd.concat([nor_df,r_df]) 
    return df

def create_lists(dataframe):
    bias_list = list(dataframe[(dataframe.obstype=='zero')].expnum.values)
    dflat_list = list(dataframe[(dataframe.obstype=='dome flat')].expnum.values)
    return bias_list,dflat_list 

if __name__ == "__main__":
    cur = query.NitelyCal('db-desoper')
    query = cur.get_cals(['20151007','20151008'])
    df = create_dataframe(query)
    remove_gap_expnums(df)
    #print df
    # Munge the dataframe
    junkless_df = remove_junk(df)
    trimmed_df = remove_first(junkless_df)   
    final_df = remove_satrband(trimmed_df)
    bias_list,dflat_list = create_lists(final_df)
    #print final_df
    #print bias_list,dflat_list
