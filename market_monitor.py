#! /user/bin/env python
#encoding:utf-8
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
#strategies = ['sz50', 'hs300', 'zz500']
strategy_names = [u'上证50', u'沪深300', u'中证500', u'策略']
index_secs = ['000016.SH', '000300.SH', '000905.SH']

r = redis.Redis(host='192.168.2.112',port=6379,db=0)

live = True #脚本是否需要持续运行

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

    def get(self):
        return {'time_stamp':self.time_stamp,'sz50':self.sz50,'hs300':self.hs300,'zz500':self.zz500,'strategy':self.strategy}


class var(object):
    def __init__(self):
        self.pool_df = None  # 策略生成策略df
        self.secs = None


now_tick = there_global()
g_var = var()

def mWSQCallback(indata, *args, **kwargs):
    d_time = indata.Times[0]
    d_data = indata.Data[0]
    ss = pd.Series(d_data,index=indata.Codes,name='rt_pct_chg')
    if now_tick.last_minute is None:
        now_tick.last_minute = d_time.minute
    elif d_time.minute != now_tick.last_minute:
        # 将数据插入db
        threading.Thread(target=insert_to_db,args=(deepcopy(now_tick),)).start()
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
    print(d_time)


def insert_to_db(data):
    # 将数据存入数据库
    d_dict = {'date': data.date, 'time_stamp': data.time_stamp, 'str_time': data.str_time, 'sz50': data.sz50,
              'hs300': data.hs300, 'zz500': data.zz500, 'strategy': data.strategy}
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
    df.loc[:,'weight'] = df['weight']/df['weight'].sum()
    g_var.pool_df = df[df['code'].str.startswith('60')]
    g_var.pool_df.loc[:, 'code'] = g_var.pool_df['code'] + '.SH'
    df = df[df['code'].str.startswith('00') | df['code'].str.startswith('30')]
    df.loc[:, 'code'] = df['code'] + '.SZ'
    g_var.pool_df = g_var.pool_df.append(df)
    g_var.pool_df = g_var.pool_df.append(pd.DataFrame({'code':index_secs,'weight':[0.0,0.0,0.0]}),ignore_index=True)
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
    #threading.Thread(target=background_thread).start()
    threading.Thread(target=start_w).start()

def start_w():
    fetch_stockpool()
    w.start()
    print(len(g_var.pool_df))
    w.wsq(g_var.secs, "rt_pct_chg", func=mWSQCallback)
    #w.wsq("RM709.CZC", "rt_last", func=mWSQCallback)


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
            elif (datetime.datetime.now().strftime('%H:%M') > '11:31') and (datetime.datetime.now().strftime('%H:%M') < '12:58'):
                live = False
            time.sleep(200)
    except (KeyboardInterrupt, SystemExit):
        # Not strictly necessary if daemonic mode is enabled but should be done if possible
        live = False