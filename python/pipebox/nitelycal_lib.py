#!/usr/bin/env python

import sys
from datetime import datetime
import pandas as pd

from pipebox import query

def create_dataframe(query_object):
    df = pd.DataFrame(query_object,columns=['nite','date_obs','expnum','band',
                                            'exptime','obstype','program','propid','object'])
    return df

def fillna(dataframe):
    df = dataframe.fillna('NA')
    return df
    
def replace_bias_band(dataframe):
    dataframe.ix[(dataframe.obstype =='zero') & (dataframe.band != 'NA'),'band'] = 'NA'
    return dataframe

def remove_junk(dataframe):
    """ If PTC, junk, test, or focus show up in program, remove """
    df = dataframe[(dataframe.object.str.contains('PTC') == False) &
                   (dataframe.object.str.contains('junk') == False) &
                   (dataframe.object.str.contains('focus') == False) &
                   (dataframe.object.str.contains('test') == False)]
    return df

def remove_gap_expnums(dataframe,tdelta=60):
    """ Remove exposures that occur greater than tdelta from previous exposures. Assumes
        testing or otherwise - precautionary"""
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
            timediff = obs1 - obs2
            diffstr = str(timediff).split(':')[2]
            if float(diffstr) > tdelta:
                index_list.append(i)
    if index_list:
        new_df = dataframe.drop(dataframe.index([index_list]))
        return new_df
    else:
        return dataframe

def remove_first_in_sequence(dataframe):
    """ Remove the first exposure in any given sequence """
    grouped = dataframe.groupby(by=['obstype','nite','band','object'])
    first_index_list = []
    for name,group in grouped:
        first_index_list.append(group.index[0])
    new_df = dataframe.drop(dataframe.index[first_index_list])
    return new_df

def remove_sat_rband(dataframe):
    """ Remove r-band exposures with exptime greater than 15 seconds. Avoids
        saturated exposures """
    nor_df = dataframe[(dataframe.band !='r')]
    r_df = dataframe[(dataframe.band == 'r') & (dataframe.exptime <= 15)]
    df = pd.concat([nor_df,r_df]) 
    return df

def create_lists(dataframe):
    """ Returns comma-separated lists of bias exposures and flat exposures 
        for use in WCL """
    bias_list = list(dataframe[(dataframe.obstype=='zero')].expnum.values)
    dflat_list = list(dataframe[(dataframe.obstype=='dome flat')].expnum.values)
    return bias_list,dflat_list 

def final_count_by_band(dataframe):
    """ Returns count per band of exposures to be included in processing """
    grouped = df.groupby(by=['obstype','band']).agg(['count'])['expnum']
    print grouped

def create_clean_df(query_object):
    """ Combines all functions in nitelycal_lib """
    df = create_dataframe(query_object)
    nafilled_df = fillna(df)
    biasband_df = replace_bias_band(nafilled_df)
    junkless_df = remove_junk(biasband_df)
    gapless_df = remove_gap_expnums(junkless_df)
    satr_df = remove_sat_rband(gapless_df)
    reindexed_df = satr_df.reset_index()
    final_df = remove_first_in_sequence(reindexed_df)
        
    return final_df

if __name__ == "__main__":
    cur = query.NitelyCal('db-desoper')
    query = cur.get_cals(['20130919','20131007'])
    count = cur.count_by_band(['20130919','20131007'])
    df = create_dataframe(query)
    df = fillna(df)
    df = replace_bias_band(df)
    df = remove_junk(df)
    df = remove_gap_expnums(df)
    df = remove_sat_rband(df)
    df = df.reset_index()
    df = remove_first_in_sequence(df)
    #print df
    #print bias_list,dflat_list
    final_count_by_band(df)
