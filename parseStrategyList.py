#! /user/bin/env python
#encoding:utf-8
#将子策略列表存放到数据库里面去

from __future__ import division

import os

import datetime
import pandas as pd
import pymongo

rootdir = u'F:/firecapital/数据/子算法'
mongo_client = pymongo.MongoClient('192.168.2.181', 27017)
db = mongo_client['fire_trade']


def parseStrategyListData2mongo():
    #将子策略的数据存放到mongodb里面去
    names = None
    today = datetime.date.today().strftime('%Y-%m-%d')
    today = '2017-04-09'
    strategy_list =[]
    for parent,dirnames,filenames in os.walk(rootdir):
        names = filenames
    a_names =[u'list_1_0.0506.xlsx',u'list_2_0.0498.xlsx',u'list_3_0.0471.xlsx',u'list_4_0.098.xlsx',u'list_16_0.0411.xlsx']
    total_weight = 0
    for filename in names:
        #df = pd.read_excel(os.path.join(rootdir,filename), header=None,names=['code','weight'])
        df = pd.read_excel(os.path.join(rootdir,filename))
        df = df[['code','weight']]
        print('strategy:'+os.path.splitext(filename)[0])
        if len(df.code.iat[0]) > 6:
            df.loc[:,'code'] = df.code.str.slice(2,8)
        df['strategy'] = os.path.splitext(filename)[0]
        strategy_list.append(os.path.splitext(filename)[0])
        df01 = df[~df.code.str.startswith('60')]
        df = df[df.code.str.startswith('60')]
        df01['wind_code'] = df01.code +'.SZ'
        df['wind_code'] = df.code +'.SH'
        df = df.append(df01)
        df['date'] = today
        db['strategy_list_output01'].insert_many(df.to_dict('records'))
        print('weight:'+str(df.weight.sum()))
        if filename in a_names:
            total_weight += df.weight.sum()
    print('total weight is:'+str(total_weight))
    db['strategy_list'].insert_one({'date':today,'strategy_list':','.join(strategy_list)})



def main():
    filename = u'list_5_0.0499.xlsx'
    df = pd.read_excel(os.path.join(rootdir,filename))
    if len(df.code[0]) > 6:
        df.loc[:,'code'] = df.code.str.slice(2,8)
    print(df.head())

if __name__ == '__main__':
    #main()
    parseStrategyListData2mongo()
