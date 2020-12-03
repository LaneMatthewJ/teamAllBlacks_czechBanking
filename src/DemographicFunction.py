#!/usr/bin/env python
# coding: utf-8

# In[1]:


def accChangeToDemo(filepath):
    """
        Creates column to be added to demographic table. Column is taken from the 
        summed difference of the median account balances by district (1994-1998)
        relative to the median account value in all data in 1994.
    Args:
        filepath (str): path to financial.db.
    """
    conn = sqlite3.connect(filepath)
    accdist = pd.read_sql_query("SELECT account_id, district_id from account", conn)
    df = pd.read_sql_query("SELECT date as 'y-m-d', balance, account_id from trans ", conn)
    from datetime import date
    df['y-m-d'] = pd.to_datetime(df['y-m-d'])
    df['year'] = df['y-m-d'].map(lambda x: x.strftime('%Y'))
    df = pd.merge(accdist, df, on='account_id')
    avg = df.groupby(['year', 'district_id', 'account_id'])[['balance']].mean()
    average = avg.groupby(['year', 'district_id'])[['balance']].median()
    districtchange = average.groupby(['district_id'])[['balance']].diff().fillna(0)
    asum = districtchange.groupby(['district_id']).sum([['balance']])
    newdemo = asum.div(302.70801242)
    
    return newdemo
    

