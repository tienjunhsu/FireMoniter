#! /user/bin/env python
#encoding:utf-8
from __future__ import  division

import os

import pymongo
import numpy as np
import pandas as pd
from WindPy import w

output_mongo_client = pymongo.MongoClient('192.168.2.181', 27017)
output_db = output_mongo_client['fire_trade']

monitor_mongo_client = pymongo.MongoClient('192.168.2.112', 27017)
moniter_db = monitor_mongo_client['fire_moniter']


def fetch_startegy_stock(strategy):
    cursor = output_db['strategy_list_output01'].find({'date': '2017-05-01', 'strategy': strategy},
                                                          {'_id': 0, 'code': 1, 'weight': 1, 'wind_code': 1})
    df = pd.DataFrame(list(cursor))
    return df


def parse_ims_excel_position(filename):
    df = pd.read_excel(filename, converters={u'证券代码': str})
    df = df[[u'证券代码', u'证券名称', u'持仓数量', u'持仓市值']]
    df.columns = ['code', 'name', 'position_num', 'position_value']
    df = df[df.code.str.startswith('30') | df.code.str.startswith('60') | df.code.str.startswith('00')]
    df = df[df.position_num > 0]
    return df

def parse_hs_excel_position(filename):
    df = pd.read_excel(filename, converters={u'证券代码': str})
    df = df[[u'证券代码', u'证券名称', u'持仓数量', u'持仓市值(全价)']]
    df.columns = ['code', 'name', 'position_num', 'position_value']
    df = df[df.code.str.startswith('30') | df.code.str.startswith('60') | df.code.str.startswith('00')]
    df = df[df.position_num > 0]
    return df

def main():
    f1 = u'F:/firecapital/code/FireMoniter/兴鑫.xls'
    f2 = u'F:/firecapital/code/FireMoniter/20170503 1号当日现货持仓.xlsx'
    df1 = parse_hs_excel_position(f1)
    df1 = df1[['code','position_num']]
    df1= df1.set_index('code')
    df2 = parse_ims_excel_position(f2)
    df2 = df2[['code','position_num','position_value']]
    df2.columns = ['code','position_num_2','position_value']
    df2 = df2.set_index('code')
    df = df1.join(df2,how = 'outer')
    df = df.fillna(0)
    df['dif'] = df.position_num - df.position_num_2
    df = df.reset_index()
    df01 = df[~df.code.str.startswith('60')]
    df = df[df.code.str.startswith('60')]
    df01['wind_code'] = df01.code +'.SZ'
    df['wind_code'] = df.code +'.SH'
    df = df.append(df01)
    w.start()
    d1 = '2017-05-02'
    d2 = '2017-05-03'
    df['last_close'] = w.wsd(','.join(df.wind_code.tolist()),"close", d1, d1, "Fill=Previous").Data[0]
    df['close'] = w.wsd(','.join(df.wind_code.tolist()),"close", d2, d2, "Fill=Previous").Data[0]
    df['dv'] = df.dif*((df.close - df.last_close))
    print(df.dv.sum())
    df.to_excel('diff.xlsx',index=False)

def test():
    rootdir = u'C:/Users/asus/Desktop/xx3.6/兴鑫5.2单笔成交'
    #filename = u'F:/firecapital/code/FireMoniter/'
    for parent,dirnames,filenames in os.walk(rootdir):
        names = filenames
    df = None
    for filename in names:
        print(filename)
        #t_df = pd.read_excel(filename,converters = {u'证券代码':str})
        t_df = pd.read_excel(os.path.join(rootdir,filename),converters = {u'证券代码':str})
        t_df = t_df.reset_index(drop=True)
        t_df = t_df[[u'证券代码',u'证券名称',u'成交数量',u'成交均价',u'成交金额',u'佣金']]
        t_df.columns = ['code','name','volume','vwap','amount','yj']
        t_df=t_df[t_df.code.str.startswith('30')|t_df.code.str.startswith('60')|t_df.code.str.startswith('00')]
        if df is None:
            df = t_df
        else:
            df = df.append(t_df,ignore_index = True)
    df['real_yj'] = 0.00016*df.amount
    error = (df.real_yj - df.yj).sum()
    print(error)


if __name__ == '__main__':
    main()
    #test()