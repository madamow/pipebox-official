#!/usr/bin/env python

import pandas as pd
from PipeBox import query

def remove_junk(dataframe):
    df = dataframe['PTC' not in df.object | 'junk' not in df.object]
    return dataframe

def remove_first(dataframe):
    grouped = dataframe.groupby(by=['obstype','nite','band'])
    for name,group in grouped.iteritems():
        print name,group     

def remove_rband(dataframe):
    df = dataframe[df.band == 'r' and df.exptime <= 15.0]
    return dataframe

def create_dataframe(query_object):
    df = pd.DataFrame(query_object,columns=['nite','expnum','band','exptime','obstype',
                                            'program','propid','object'])
    junkless_df = remove_junk(df)
    trimmed_df = remove_first(df)
 
cur = query.NitelyCal('db-desoper')
query = cur.get_cals(20151017)
   
