#! /user/bin/env python
# encoding:utf-8
import threading
from copy import deepcopy
import datetime
import pandas as pd
import time
import os

import pymongo
import redis
import requests
from WindPy import w

mongo_client = pymongo.MongoClient('192.168.2.112', 27017)
collection = mongo_client['fire_moniter']['daily_moniter']
strategies = ['sz50', 'hs300', 'zz500', 'strategy']
# strategies = ['sz50', 'hs300', 'zz500']
strategy_names = [u'上证50', u'沪深300', u'中证500', u'策略']
index_secs = ['000016.SH', '000300.SH', '000905.SH']

r = redis.Redis(host='192.168.2.112', port=6379, db=0)

live = True  # 脚本是否需要持续运行

delivery_dates = ['2017-01-20', '2017-02-17', '2017-03-17', '2017-04-21', '2017-05-19', '2017-06-16', '2017-07-21',
                  '2017-08-18', '2017-09-15', '2017-10-20', '2017-11-17', '2017-12-15']  # 股指期货合约交割日
prefix = ['IH', 'IF', 'IC']
spread_contracts = None  # 基差合约
spread_dict = None  # 基差合约字典


def gen_contract():
    # 获取IH、IF和IC的当月合约
    today = datetime.date.today()
    c_month = today.month
    c_year = today.year
    if today.strftime('%Y-%m-%d') > delivery_dates[c_month - 1]:
        # 大于交割日了，期货已经换仓
        c_month += 1
        if c_month > 12:
            c_month %= 12
            c_year += 1
    c_month = '%02d' % c_month
    c_year = str(c_year)[-2:]
    contract = c_year + c_month + '.CFE'
    global spread_contracts
    spread_contracts = [pre + contract for pre in prefix]
    global spread_dict
    spread_dict = dict(zip(spread_contracts, ['ih_spread', 'if_spread', 'ic_spread']))


class there_global(object):
    # 对某些不可变对象重新赋值会出现全局变量不可用的状态，用这个类构建一个可变对象
    def __init__(self):
        self.date = datetime.date.today().strftime('%Y-%m-%d')
        self.time_stamp = None
        self.last_minute = None
        self.str_time = None
        self.sz50 = None  # 上证50
        self.hs300 = None  # 沪深300
        self.zz500 = None  # 中证500
        self.strategy = None  # 策略
        self.ex_strategy = None  # 派生策略
        self.ih_spread = None  # 上证基差(和当月合约比较)
        self.if_spread = None
        self.ic_spread = None

    def get(self):
        return {'time_stamp': self.time_stamp, 'sz50': self.sz50, 'hs300': self.hs300, 'zz500': self.zz500,
                'strategy': self.strategy, 'ih_spread': self.ih_spread, 'if_spread': self.if_spread,
                'ic_spread': self.ic_spread}


class var(object):
    def __init__(self):
        self.pool_df = None  # 策略生成策略df
        self.secs = None


now_tick = there_global()
g_var = var()


def mWSQCallback(indata, *args, **kwargs):
    d_time = indata.Times[0]
    d_data = indata.Data[0]
    ss = pd.Series(d_data, index=indata.Codes, name='rt_pct_chg')
    if now_tick.last_minute is None:
        now_tick.last_minute = d_time.minute
    elif d_time.minute != now_tick.last_minute:
        # 将数据插入db
        threading.Thread(target=insert_to_db, args=(deepcopy(now_tick),)).start()
        now_tick.last_minute = d_time.minute
    g_var.pool_df['rt_pct_chg'].update(ss)
    now_tick.str_time = d_time.strftime('%Y-%m-%d %H:%M')
    now_tick.time_stamp = time.mktime(d_time.timetuple())
    now_tick.sz50 = g_var.pool_df['rt_pct_chg'].loc['000016.SH']
    now_tick.hs300 = g_var.pool_df['rt_pct_chg'].loc['000300.SH']
    now_tick.zz500 = g_var.pool_df['rt_pct_chg'].loc['000905.SH']
    t_df = g_var.pool_df[~g_var.pool_df.index.isin(index_secs)]
    now_tick.strategy = round((t_df['rt_pct_chg'] * t_df['weight']).sum(), 4)
    r.mset(now_tick.get())
    #print(d_time)


def mSpreadCallback(indata, *args, **kwargs):
    print(getattr(now_tick, 'if_spread'))
    s_codes = indata.Codes
    s_data = indata.Data[0]
    for i in range(len(s_data)):
        setattr(now_tick, spread_dict[s_codes[i]], -s_data[i])


def insert_to_db(data):
    # 将数据存入数据库
    d_dict = {'date': data.date, 'time_stamp': data.time_stamp, 'str_time': data.str_time, 'sz50': data.sz50,
              'hs300': data.hs300, 'zz500': data.zz500, 'strategy': data.strategy, 'ih_spread': data.ih_spread,
              'if_spread': data.if_spread, 'ic_spread': data.ic_spread}
    collection.insert_one(d_dict)


def fetch_stockpool():
    mongo = pymongo.MongoClient('192.168.2.181', 27017)
    db = mongo['fire_trade']
    date = db['strategy_output_date'].find().sort('date', -1).limit(1)[0]['date']
    cursor = db['strategy_output01'].find({'date': date}, {'_id': 0, 'code': 1, 'weight': 1})
    df = pd.DataFrame(list(cursor))
    if len(df.code[0]) > 6:
        df.code = df.code.str.slice(2, 8)
    high_risk_ticks = requests.get('http://192.168.2.112:8000/risk/highriskticks/' + date + '/').text
    high_risk_ticks = high_risk_ticks.split(',')
    df = df[~df['code'].isin(high_risk_ticks)]
    df.loc[:, 'weight'] = df['weight'] / df['weight'].sum()
    g_var.pool_df = df[df['code'].str.startswith('60')]
    g_var.pool_df.loc[:, 'code'] = g_var.pool_df['code'] + '.SH'
    df = df[df['code'].str.startswith('00') | df['code'].str.startswith('30')]
    df.loc[:, 'code'] = df['code'] + '.SZ'
    g_var.pool_df = g_var.pool_df.append(df)
    g_var.pool_df = g_var.pool_df.append(pd.DataFrame({'code': index_secs, 'weight': [0.0, 0.0, 0.0]}),
                                         ignore_index=True)
    g_var.secs = ','.join(g_var.pool_df['code'].tolist())
    g_var.pool_df['rt_pct_chg'] = 0.0
    g_var.pool_df = g_var.pool_df.set_index('code')


def tick():
    print('Tick! The time is: %s' % datetime.datetime.now())


def background_thread():
    """Example of how to send server generated events to clients."""
    count = 0
    while True:
        time.sleep(1)
        count += 1
        tick()


def kk():
    # threading.Thread(target=background_thread).start()
    threading.Thread(target=start_w).start()


def start_w():
    gen_contract()
    fetch_stockpool()
    w.start()
    print(len(g_var.pool_df))
    w.wsq(','.join(spread_contracts), "rt_spread", func=mSpreadCallback)
    w.wsq(g_var.secs, "rt_pct_chg", func=mWSQCallback)
    # w.wsq("RM709.CZC", "rt_last", func=mWSQCallback)


def cancle_w():
    w.cancelRequest()
    w.stop()


if __name__ == '__main__':
    start_w()
    print('Press Ctrl+{0} to exit'.format('Break' if os.name == 'nt' else 'C'))
    try:
        # This is here to simulate application activity (which keeps the main thread alive).
        while live:
            if datetime.datetime.now().strftime('%H:%M') > '15:01':
                live = False
            elif (datetime.datetime.now().strftime('%H:%M') > '11:31') and (
                        datetime.datetime.now().strftime('%H:%M') < '12:58'):
                live = False
            time.sleep(200)
    except (KeyboardInterrupt, SystemExit):
        # Not strictly necessary if daemonic mode is enabled but should be done if possible
        live = False
