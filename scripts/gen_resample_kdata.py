#! /user/bin/env python
#encoding:utf-8

from __future__ import division
import os

from gm_data_tool import load_gm_csv, resampleKData, gen_trade_date

rootdir = u'D:/Data/data'
nrootdir = u'D:/Data/ndata'
periods = ['5T','15T','30T','1H','90T','2H','3H','4H']
names = ['5m','15m','30m','1h','90m','2h','3h','4h']

for parent,dirnames,filenames in os.walk(rootdir):
    for filename in filenames:
        print('begin '+ filename +'.......')
        df = load_gm_csv(os.path.join(rootdir,filename))
        if len(df) > 0:
            df = gen_trade_date(df)
            for i,period in enumerate(periods):
                print('begin period '+period +'.......')
                r_df = resampleKData(df,period)
                r_df.to_csv(os.path.join(nrootdir,filename.replace('1m',names[i])), index=False)
        else:
            print('DataFrame length is 0,end')

