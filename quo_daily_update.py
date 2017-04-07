#! /user/bin/env python
# encoding:utf-8
# 本地行情和股票资料日更新
import datetime
import pymongo
import pandas as pd
import tushare as ts
from WindPy import w

mongo_client = pymongo.MongoClient('192.168.2.112', 27017)


def update_stock_basis():
    # 更新股票基本资料
    df = ts.get_stock_basics()
    if len(df) > 0:
        df = df.reset_index()
        collection = mongo_client['fire_data']['stock_basics']
        collection.remove()
        collection.insert_many(df.to_dict('records'))


def update_stock_quo_data(today = None):
    # 更新股票的收盘行情数据
    if today is None:
        today = datetime.date.today().strftime('%Y-%d-%m')
    all_a = w.wset("sectorconstituent", "date={0};sectorid=a001010100000000".format(today))
    df = pd.DataFrame({'wind_code': all_a.Data[1], 'name': all_a.Data[2]})
    df['date'] = today
    df['code'] = df['wind_code'].str.slice(0,6)
    #print(df.head())
    indication_list = ['pre_close', 'open', 'high', 'low', 'close', 'volume', 'amt', 'chg', 'pct_chg', 'swing', 'vwap',
                       'adjfactor', 'turn', 'lastradeday_s','rel_ipo_chg','rel_ipo_pct_chg','trade_status','maxupordown']
    #indication_list = ['pre_close', 'open']
    secs =','.join(df['wind_code'].tolist())
    for para in indication_list:
        result = w.wsd(secs,para,today,today,'Fill=Previous')
        df[para] = result.Data[0]
    collection = mongo_client['fire_data']['close_data']
    collection.insert_many(df.to_dict('records'))


def update_index_quo_data(today = None):
    #更新指数数据
    if today is None:
        today = datetime.date.today().strftime('%Y-%d-%m')
    df = pd.DataFrame({'code':['000016','000300','000905'],'wind_code': ['000016.SH','000300.SH','000905.SH'], 'name': [u'中证50',u'沪深300',u'中证500']})
    df['date'] = today
    indication_list = ['pre_close', 'open', 'high', 'low', 'close', 'volume', 'amt', 'chg', 'pct_chg', 'swing', 'vwap']
    secs =','.join(df['wind_code'].tolist())
    for para in indication_list:
        result = w.wsd(secs,para,today,today,'Fill=Previous')
        df[para] = result.Data[0]
    collection = mongo_client['fire_data']['index_data']
    collection.insert_many(df.to_dict('records'))


if __name__ == '__main__':
    update_stock_basis()
    w.start()
    for today in ['2017-04-05','2017-04-06','2017-04-07']:
        update_stock_quo_data(today)
        update_index_quo_data(today)
