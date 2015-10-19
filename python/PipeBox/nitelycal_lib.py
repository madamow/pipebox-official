#!/usr/bin/env python

import pandas as pd
from PipeBox import query

def remove_junk(dataframe):
    df = dataframe[(dataframe.object.str.contains('PTC') ==False) | (dataframe.object.str.contains('junk')==False)]
    return df

def remove_first(dataframe):
    grouped = dataframe.groupby(by=['obstype','nite','band'])
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

def create_dataframe(query_object):
    df = pd.DataFrame(query_object,columns=['nite','expnum','band','exptime','obstype',
                                            'program','propid','object'])
    return df
 
cur = query.NitelyCal('db-desoper')
query = cur.get_cals(20151007)
df = create_dataframe(query)
print df
new_df = remove_first(df)
print new_df
# Munge the dataframe
#junkless_df = remove_junk(df)
#trimmed_df = remove_first(junkless_df)   
#final_df = remove_satrband(trimmed_df)
#print final_df
