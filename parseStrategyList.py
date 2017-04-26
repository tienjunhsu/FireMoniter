#! /user/bin/env python
#encoding:utf-8
#将子策略列表存放到数据库里面去

from __future__ import division

import os

import datetime
import pandas as pd
import pymongo
import requests

#rootdir = u'F:/firecapital/数据/result_sublist'
#n_rootdir = u'F:/firecapital/数据/nresult_sublist'
rootdir = u'D:/Data/result_sublist'
mongo_client = pymongo.MongoClient('192.168.2.181', 27017)
db = mongo_client['fire_trade']


def parseStrategyListData2mongo():
    #将子策略的数据存放到mongodb里面去
    names = None
    today = datetime.date.today().strftime('%Y-%m-%d')
    strategy_list =[]
    for parent,dirnames,filenames in os.walk(rootdir):
        names = filenames
    total_weight = 0
    for filename in names:
        #df = pd.read_excel(os.path.join(rootdir,filename), header=None,names=['code','weight'])
        df = pd.read_excel(os.path.join(rootdir,filename),converters={'code':str})
        df = df[['code','weight']]
        print('strategy:'+os.path.splitext(filename)[0])
        if len(df.code.iat[0]) > 6:
            df.loc[:,'code'] = df.code.str.slice(2,8)
        df['strategy'] = (os.path.splitext(filename)[0]).split('.')[0]
        strategy_list.append((os.path.splitext(filename)[0]).split('.')[0])
        df01 = df[~df.code.str.startswith('60')]
        df = df[df.code.str.startswith('60')]
        df01['wind_code'] = df01.code +'.SZ'
        df['wind_code'] = df.code +'.SH'
        df = df.append(df01)
        df['date'] = today
        db['strategy_list_output01'].insert_many(df.to_dict('records'))
        print('weight:'+str(df.weight.sum()))
        total_weight += df.weight.sum()
    print('total weight is:'+str(total_weight))
    db['strategy_list'].insert_one({'date':today,'strategy_list':','.join(strategy_list)})


def get_all_one():
    risk_url = 'http://180.168.45.126:60618/risk/highriskticks/2017-04-23/'
    high_risk_list = requests.get(risk_url).text.split(',')
    all_df  = pd.read_excel(u'F:/firecapital/数据/20170423_outputlist_selectedlist_4_23_20_53_32.xlsx',converters = {'code':str})
    all_df = all_df[['code','weight']]
    if len(all_df.code[0]) > 6:
        all_df.code = all_df.code.str.slice(2,8)
    all_df = all_df[~all_df.code.isin(high_risk_list)]
    total_weight = all_df.weight.sum()

    for parent,dirnames,filenames in os.walk(rootdir):
        names = filenames
    for filename in names:
        df = pd.read_excel(os.path.join(rootdir,filename), header=None,names=['code','weight'])
        #df = pd.read_excel(os.path.join(rootdir,filename))
        df = df[['code','weight']]
        print('strategy:'+os.path.splitext(filename)[0])
        if len(df.code.iat[0]) > 6:
            df.loc[:,'code'] = df.code.str.slice(2,8)
        df['strategy'] = os.path.splitext(filename)[0]
        df01 = df[~df.code.str.startswith('60')]
        df = df[df.code.str.startswith('60')]
        df01['wind_code'] = df01.code +'.SZ'
        df['wind_code'] = df.code +'.SH'
        df = df.append(df01)
        df = df[df.code.isin(all_df.code.tolist())]
        #df.loc[:,'weight'] = df.weight/total_weight
        df.to_excel(filename,index=False)


def main():
    filename = u'list_5_0.0499.xlsx'
    df = pd.read_excel(os.path.join(rootdir,filename))
    if len(df.code[0]) > 6:
        df.loc[:,'code'] = df.code.str.slice(2,8)
    print(df.head())

if __name__ == '__main__':
    #main()
    parseStrategyListData2mongo()
    #get_all_one()
