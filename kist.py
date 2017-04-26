#! /user/bin/env python
#encoding:utf-8
from __future__ import  division
from WindPy import w
import pymongo
import numpy as np
import pandas as pd

indexes = ['000016.SH', '000300.SH', '000905.SH']  # 指数万得代码，分别是上证50、沪深300和中证500
index_ratio = [0.75, 1.0, 0.9]  # IH,IF和IC对应的系数分别是0.75,1.0和0.9

output_mongo_client = pymongo.MongoClient('192.168.2.181', 27017)
output_db = output_mongo_client['fire_trade']
last_trade_day = '2017-04-24'
today = '2017-04-25'

def fetch_index_vwap_chg():
    #根据指数的均价计算指数的涨跌幅
    indexes_chg = []
    for sec in indexes:
        last_prices = w.wsi(sec, "close", last_trade_day + " 09:00:00",last_trade_day+ " 15:00:00", "").Data[0]
        last_prices = np.mean(last_prices)
        today_prices = w.wsi(sec, "close", today + " 09:00:00",today+ " 15:00:00", "").Data[0]
        today_prices = np.mean(today_prices)
        chg = (today_prices - last_prices)/last_prices
        print(sec)
        print(last_prices)
        print(today_prices)
        print(chg)
        #indexes_chg.append(chg)

def fetch_chg_from_wind_wsd(secs):
    # 从万得接口获取涨跌幅数据,通过日期函数获取，这个得到每天15：30后才能获取当前的数据
    last_vwap = np.array(fetch_last_vwap(secs))
    #r = w.wsd(secs, "vwap", today, today, "")
    r = w.wsd(secs, "close", today, today, "")
    data = np.array(r.Data[0])
    data =(data-last_vwap)/last_vwap
    return data.tolist()

def fetch_chg_from_wind_wsq(secs):
    # 从万得接口获取涨跌幅数据,通过实时行情获取数据快照数据，带15:30之前获取,后面数据就会不准确
    last_vwap = np.array(fetch_last_vwap(secs))
    r = w.wsq(secs, 'rt_vwap')
    data = np.array(r.Data[0])
    data =(data-last_vwap)/last_vwap
    return data.tolist()

def fetch_last_vwap(secs):
    #获取上一交易日的均价
    #r = w.wsd(secs, "vwap", last_trade_day, last_trade_day, "Fill=Previous")
    r = w.wsd(secs, "close", last_trade_day, last_trade_day, "Fill=Previous")
    data = r.Data[0]
    return data

def fetch_startegy_stock(strategy):
    cursor = output_db['strategy_list_output01'].find({'date': '2017-04-23', 'strategy': strategy},
                                                          {'_id': 0, 'code': 1, 'weight': 1, 'wind_code': 1})
    df = pd.DataFrame(list(cursor))
    return df

def main():
    w.start()
    fetch_index_vwap_chg()
    df = fetch_startegy_stock('list_2_0')
    secs = ','.join(df.wind_code.tolist())
    df['pct_chg'] =  fetch_chg_from_wind_wsd(secs)
    chg = (df['pct_chg'] * (df['weight'] / df['weight'].sum())).sum()
    print(chg)

if __name__ == '__main__':
    main()