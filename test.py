#! /user/bin/env python
#encoding:utf-8
import os
import pandas as pd
import pymongo
import requests
from WindPy import w
import tushare as ts

output_mongo_client = pymongo.MongoClient('192.168.2.181', 27017)
output_db = output_mongo_client['fire_trade']

monitor_mongo_client = pymongo.MongoClient('192.168.2.112', 27017)
moniter_db = monitor_mongo_client['fire_moniter']


def main():
    cursor = output_db['strategy_output01'].find({'date': '2017-04-16'},
                                                          {'_id': 0, 'code': 1, 'weight': 1})
    df = pd.DataFrame(list(cursor))
    df.loc[:,'weight'] = df.weight/df.weight.sum()
    close_cursor = monitor_mongo_client['fire_data']['close_data'].find({'date':'2017-04-17','code':{'$in':df.code.tolist()}},{'_id':0,'close':1,'code':1,'name':1})
    close_df = pd.DataFrame(list(close_cursor))
    close_df = close_df.set_index('code')
    df = df.set_index('code')
    df = df.join(close_df)
    df['position_num'] = 0
    df = df.reset_index()
    df01 = df[df.code.str.startswith('60')]
    df = df[~df.code.str.startswith('60')]
    df01.loc[:,'code'] = 'SH'+df01.code
    df.loc[:,'code'] = 'SZ'+df.code
    df = df.append(df01,ignore_index=True)
    totalAmount = 500000.0
    df = approach_totalAmount(totalAmount,df)
    df = df[df.position_num > 0]
    df['position_num01'] = (100*((df.position_num//2)//100)).astype(int)
    df.loc[:,'position_num'] = (df.position_num - df.position_num01).astype(int)
    df = df[['code','weight','close','position_num','position_num01']]
    df.to_csv('open_test_pool.csv',index=False)


def approach_totalAmount(totalAmount,df):
    #获取计划持仓股数，逼近总金额
    #totalAmount是计划持仓的总金额，df是带有收盘价的策略生成的票池DataFrame
    delta = 1000.0 #最小的迭代步长
    df.position_num = 100 * ((totalAmount * df.weight) // (100 * df.close))
    real_total = (df.position_num * df.close).sum()
    new_totalAmount = totalAmount
    while real_total < totalAmount:
        new_totalAmount +=  min(totalAmount - real_total,delta)
        df.position_num = 100 * ((new_totalAmount * df.weight) // (100 * df.close))
        real_total = (df.position_num * df.close).sum()
    return df

def get_buy():
    w.start()
    cursor = output_db['strategy_output01'].find({'date': '2017-04-16'},
                                                          {'_id': 0, 'code': 1, 'weight': 1})

    df = pd.DataFrame(list(cursor))
    risk_url = 'http://192.168.2.112：8000/risk/highriskticks/2017-04-16/'
    high_risk_list = requests.get(risk_url).text.split(',')
    df = df[~df.code.isin(high_risk_list)]
    date = '2017-04-17'
    df01 = df[~df.code.str.startswith('60')]
    df = df[df.code.str.startswith('60')]
    df01['wind_code'] = df01.code +'.SZ'
    df['wind_code'] = df.code +'.SH'
    df = df.append(df01)
    df['mkt'] = w.wsd(','.join(df.wind_code.tolist()),"mkt_cap_float", date, date, "unit=1;currencyType=;Fill=Previous").Data[0]
    df = df.sort_values(by='mkt')
    df = df[:20]
    df.loc[:,'weight'] = df.weight/df.weight.sum()
    close_cursor = monitor_mongo_client['fire_data']['close_data'].find({'date':date,'code':{'$in':df.code.tolist()}},{'_id':0,'close':1,'code':1,'name':1})
    close_df = pd.DataFrame(list(close_cursor))
    close_df = close_df.set_index('code')
    df = df.set_index('code')
    df = df.join(close_df)
    df['position_num'] = 0
    df = df.reset_index()
    df01 = df[df.code.str.startswith('60')]
    df = df[~df.code.str.startswith('60')]
    df01.loc[:,'code'] = 'SH'+df01.code
    df.loc[:,'code'] = 'SZ'+df.code
    df = df.append(df01,ignore_index=True)
    totalAmount = 500000.0
    df = approach_totalAmount(totalAmount,df)
    df = df[df.position_num > 0]
    df['position_num01'] = (100*((df.position_num//3)//100)).astype(int)
    df['position_num02'] = (100*((df.position_num//3)//100)).astype(int)
    df.loc[:,'position_num'] = (df.position_num - df.position_num01 - df.position_num02).astype(int)
    df = df[['code','weight','close','position_num','position_num01','position_num02']]
    df.to_csv('open_test_pool.csv',index=False)


def get_sell():
    filename = u'0511一号加1-12.xlsx'
    df = pd.read_excel(filename,converters={'code': str})
    df['code_six'] = df.code
    df01 = df[df.code.str.startswith('60')]
    df = df[~df.code.str.startswith('60')]
    df01.loc[:,'code'] = 'SH'+df01.code
    df.loc[:,'code'] = 'SZ'+df.code
    df = df.append(df01,ignore_index=True)
    df['position_num01'] = (100*((df.position_num//3)//100)).astype(int)
    df['position_num02'] = (100*((df.position_num//3)//100)).astype(int)
    df.loc[:,'position_num'] = (df.position_num - df.position_num01 - df.position_num02).astype(int)
    df = df[['code','position_num','position_num01','position_num02']]
    # df['position_num02'] = (100*((df.position_num//2)//100)).astype(int)
    # df.loc[:,'position_num'] = (df.position_num  - df.position_num02).astype(int)
    # df = df[['code','position_num','position_num02']]
    df.to_csv('open_test_pool.csv',index=False)

if __name__ == '__main__':
    #main()
    get_sell()
    #get_buy()