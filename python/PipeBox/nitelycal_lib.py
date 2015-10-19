#!/usr/bin/env python

import sys
from datetime import datetime
import pandas as pd

from PipeBox import query

def create_dataframe(query_object):
    df = pd.DataFrame(query_object,columns=['nite','date_obs','expnum','band',
                                            'exptime','obstype','program','propid','object'])
    return df

def remove_junk(dataframe):
    df = dataframe[(dataframe.object.str.contains('PTC') == False) | 
                   (dataframe.object.str.contains('junk') == False) |
                   (dataframe.object.str.contains('focus') == False)]
    return df

def remove_gap_expnums(dataframe,tdelta=60):
    index_list = []
    for i,row in dataframe[1:].iterrows():
        j = i - 1
        # Current exposure's obs time
        obs1 = datetime.strptime(row['date_obs'],'%Y-%m-%dT%H:%M:%S.%f')
        date1 = obs1.date()
        # Prior exposure's obs time
        obs2 = datetime.strptime(dataframe.loc[j,'date_obs'],'%Y-%m-%dT%H:%M:%S.%f')
        date2 = obs2.date()
        # Create datetime objects to compute difference in time
        if date1 == date2:
            timediff = date1 - date2
            diffstr = str(timediff).split(':')[2]
            print obs1,obs2,timediff,diffstr
            if int(str(timediff).split(':')[2]) > tdelta:
                index_list.append(i)
    new_df = dataframe.drop(dataframe.index([index_list]))
    return new_df

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
    print df
    # Munge the dataframe
    new_df = remove_gap_expnums(df)
    junkless_df = remove_junk(new_df)
    trimmed_df = remove_first(junkless_df)   
    final_df = remove_satrband(trimmed_df)
    bias_list,dflat_list = create_lists(final_df)
    print final_df
    print bias_list,dflat_list
