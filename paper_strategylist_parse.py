#! /user/bin/env python
#encoding:utf-8

#解析跟踪策略列表
import datetime
import os

import pymongo
import requests
import pandas as pd

mongo_client = pymongo.MongoClient('192.168.2.181', 27017)
db = mongo_client['paper_trade']

rootdir = u'F:/firecapital/数据/paper_realoutput_20170507/sublist'


def parse_all():
    risk_url = 'http://192.168.2.112:8000/risk/highriskticks/2017-05-07/'
    high_risk_list = requests.get(risk_url).text.split(',')
    all_df  = pd.read_excel(u'F:/firecapital/数据/paper_realoutput_20170507/lists_outputlist_selectedlist_5_8_1_44_33.xlsx',converters = {'code':str})
    all_df = all_df[['code','weight']]
    if len(all_df.code[0]) > 6:
        all_df.code = all_df.code.str.slice(2,8)
    all_df = all_df[~all_df.code.isin(high_risk_list)]
    total_weight = all_df.weight.sum()
    all_df.loc[:,'weight'] = all_df.weight/total_weight
    all_df['date'] = '2017-05-07'
    db['strategy_output01'].insert_many(all_df.to_dict('records'))

def parseStrategyListData2mongo():
    #将子策略的数据存放到mongodb里面去
    names = None
    today = datetime.date.today().strftime('%Y-%m-%d')
    today = '2017-05-07'
    strategy_list =[]
    #risk_url = 'http://180.168.45.126:60618/risk/highriskticks/2017-04-23/'
    risk_url = 'http://192.168.2.112:8000/risk/highriskticks/2017-05-07/'
    high_risk_list = requests.get(risk_url).text.split(',')
    #all_df  = pd.read_excel(u'F:/firecapital/数据/20170423_outputlist_selectedlist_4_23_20_53_32.xlsx',converters = {'code':str})
    cursor = db['strategy_output01'].find({'date': today},projection={'_id': False})
    all_df = pd.DataFrame(list(cursor))
    all_df = all_df[['code','weight']]
    if len(all_df.code.iat[0]) > 6:
        all_df.code = all_df.code.str.slice(2,8)
    all_df = all_df[~all_df.code.isin(high_risk_list)]
    for parent,dirnames,filenames in os.walk(rootdir):
        names = filenames
    total_weight = 0
    for filename in names:
        df = pd.read_excel(os.path.join(rootdir,filename), header=None,names=['code','weight'])
        #df = pd.read_excel(os.path.join(rootdir,filename),converters={'code':str})
        df = df[['code','weight']]
        print('strategy:'+os.path.splitext(filename)[0])
        if len(df.code.iat[0]) > 6:
            df.loc[:,'code'] = df.code.str.slice(2,8)
        df = df[df.code.isin(all_df.code.tolist())]
        print(len(df))
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


def parseShareStock():
    #成分股,策略名以share开始，如share50,share300,share500
    filename = u'F:/firecapital/数据/paper_realoutput_20170507/share.xlsx'
    sheet_names = ['share500','share300','share50']
    date = '2017-05-07'
    strategy_list = []
    for sheet_name in sheet_names:
        df = pd.read_excel(filename,sheet_name)
        if len(df.code.iat[0]) > 0:
            df.loc[:,'code'] = df.code.str.slice(2,8)
        df01 = df[~df.code.str.startswith('60')]
        df = df[df.code.str.startswith('60')]
        df01['wind_code'] = df01.code +'.SZ'
        df['wind_code'] = df.code +'.SH'
        df = df.append(df01)
        df['date'] = date
        df['strategy'] = sheet_name
        db['other_strategy_output01'].insert_many(df.to_dict('records'))
        strategy_list.append(sheet_name)
    db['other_strategy_list'].insert_one({'date':date,'strategy_list':','.join(strategy_list)})



if __name__ == '__main__':
    #parse_all()
    #parseStrategyListData2mongo()
    parseShareStock()

