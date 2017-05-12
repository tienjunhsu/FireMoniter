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

output_mongo_client = pymongo.MongoClient('192.168.2.181', 27017)
output_db = output_mongo_client['paper_trade']

mongo_client = pymongo.MongoClient('192.168.2.112', 27017)
collection = mongo_client['paper_moniter']['daily_moniter']
#collection = mongo_client['fire_moniter']['daily_moniter_test'] #for test
strategies = ['sz50', 'hs300', 'zz500', 'strategy']
# strategies = ['sz50', 'hs300', 'zz500']
strategy_names = [u'上证50', u'沪深300', u'中证500', u'策略']
index_secs = ['000016.SH', '000300.SH', '000905.SH']

r = redis.Redis(host='192.168.2.112', port=6379, db=2)
#r = redis.Redis(host='192.168.2.112', port=6379, db=1) #db1 for test

live = True  # 脚本是否需要持续运行

delivery_dates = ['2017-01-20', '2017-02-17', '2017-03-17', '2017-04-21', '2017-05-19', '2017-06-16', '2017-07-21',
                  '2017-08-18', '2017-09-15', '2017-10-20', '2017-11-17', '2017-12-15']  # 股指期货合约交割日
prefix = ['IH', 'IF', 'IC']
spread_contracts = None  # 基差合约
spread_dict = None  # 基差合约字典

output_date = None #策略生成时间
strategy_list = None #子策略列表
last_trade_day = None #上一个交易日


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
        self.gte200 = None #市值200亿以上的股票
        self.signal = 0.0 #信号生成的复合策略

    def get(self):
        d_dict =  {'time_stamp': self.time_stamp, 'sz50': self.sz50, 'hs300': self.hs300, 'zz500': self.zz500,
                'strategy': self.strategy, 'ih_spread': self.ih_spread, 'if_spread': self.if_spread,
                'ic_spread': self.ic_spread,'gte200':self.gte200}
        d_dict.update({strategy_name:self.__dict__[strategy_name] for strategy_name in strategy_list})
        #print(self.gte200)
        return d_dict

    def getitem(self,key):
        return self.__dict__[key]

    def setitem(self,key,value):
        self.__dict__[key] = value


class var(object):
    def __init__(self):
        self.pool_df = None  # 策略生成策略df
        self.secs = None
        self.sub_df_dict = {} #用于存储生成子策略的df
        self.gte200_df = None #市值200亿以上的df
        self.signal_df = None #信号生成的df


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
    for strategy in strategy_list:
        g_var.sub_df_dict[strategy]['rt_pct_chg'].update(ss)
        v= round((g_var.sub_df_dict[strategy]['rt_pct_chg'] * g_var.sub_df_dict[strategy]['weight']).sum(), 4)
        setattr(now_tick,strategy,v)
    g_var.gte200_df.update(ss)
    now_tick.gte200 = round((g_var.gte200_df['rt_pct_chg'] * g_var.gte200_df['weight']).sum(), 4)
    #g_var.signal_df.update(ss)
    #now_tick.signal = round((g_var.signal_df['rt_pct_chg'] * g_var.signal_df['weight']).sum(), 4)
    r.mset(now_tick.get())
    #print(d_time)


def mSpreadCallback(indata, *args, **kwargs):
    #print(getattr(now_tick, 'if_spread'))
    s_codes = indata.Codes
    s_data = indata.Data[0]
    for i in range(len(s_data)):
        setattr(now_tick, spread_dict[s_codes[i]], s_data[i])


def insert_to_db(data):
    # 将数据存入数据库
    d_dict = {'date': data.date, 'time_stamp': data.time_stamp, 'str_time': data.str_time, 'sz50': data.sz50,
              'hs300': data.hs300, 'zz500': data.zz500, 'strategy': data.strategy, 'ih_spread': data.ih_spread,
              'if_spread': data.if_spread, 'ic_spread': data.ic_spread,'gte200':data.gte200}
    d_dict.update({strategy_name:data.getitem(strategy_name) for strategy_name in strategy_list})
    collection.insert_one(d_dict)


def fetch_stockpool():
    global output_date
    output_date = output_db['strategy_output_date'].find().sort('date', -1).limit(1)[0]['date']
    cursor = output_db['strategy_output01'].find({'date': output_date}, {'_id': 0, 'code': 1, 'weight': 1})
    df = pd.DataFrame(list(cursor))
    if len(df.code[0]) > 6:
        df.code = df.code.str.slice(2, 8)
    high_risk_ticks = requests.get('http://192.168.2.112:8000/risk/highriskticks/' + output_date + '/').text
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
    g_var.secs = g_var.pool_df['code'].tolist() #先设置为列表，然后用set来去重，再进行组合成字符串来进行订阅
    g_var.pool_df['rt_pct_chg'] = 0.0
    g_var.pool_df = g_var.pool_df.set_index('code')
    fetch_strategy_list()
    g_var.secs = set(g_var.secs) #利用set来去除重复的
    g_var.secs = ','.join(g_var.secs)


def fetch_strategy_list():
    #获取子策略列表
    cursor = output_db['strategy_list'].find({'date': output_date}, {'_id': 0, 'strategy_list': 1})
    global strategy_list
    strategy_list = list(cursor)[0]['strategy_list']
    strategy_list = strategy_list.split(',')
    print(strategy_list)
    for strategy in strategy_list:
        fetch_sub_strategy(strategy)
    fetch_other_strategy() #获取其他单独的策略


def fetch_sub_strategy(strategy_name):
    #获取子策略
    cursor = output_db['strategy_list_output01'].find({'date': output_date, 'strategy': strategy_name},
                                                          {'_id': 0, 'code': 1, 'weight': 1, 'wind_code': 1})
    df = pd.DataFrame(list(cursor))
    df.loc[:,'code'] = df.wind_code
    df.loc[:,'weight'] = df.weight/df.weight.sum() #监控单独的每个子策略，子策略的权重需要归一化
    df = df[['code','weight']]
    df['rt_pct_chg'] = 0.0
    g_var.secs += df.code.tolist() #把所有的股票代码放进订阅列表里面去
    df = df.set_index('code')
    g_var.sub_df_dict[strategy_name] = df #子策略的DataFrame
    setattr(now_tick,strategy_name,None) #初始化tick数据值，每个子策略的初始值都设置为None


def fetch_other_strategy():
    #其他的策略
    cursor = output_db['other_strategy_list'].find({'date': output_date}, {'_id': 0, 'strategy_list': 1})
    other_strategy_list = list(cursor)[0]['strategy_list']
    other_strategy_list = other_strategy_list.split(',')
    global strategy_list
    strategy_list += other_strategy_list
    for strategy_name in other_strategy_list:
        cursor = output_db['other_strategy_output01'].find({'date': output_date, 'strategy': strategy_name},
                                                          {'_id': 0, 'code': 1, 'weight': 1, 'wind_code': 1})
        df = pd.DataFrame(list(cursor))
        df.loc[:,'code'] = df.wind_code
        df.loc[:,'weight'] = df.weight/df.weight.sum() #监控单独的每个子策略，子策略的权重需要归一化
        df = df[['code','weight']]
        df['rt_pct_chg'] = 0.0
        g_var.secs += df.code.tolist() #把所有的股票代码放进订阅列表里面去
        df = df.set_index('code')
        g_var.sub_df_dict[strategy_name] = df #子策略的DataFrame
        setattr(now_tick,strategy_name,None) #初始化tick数据值，每个子策略的初始值都设置为None



def fetch_gte200_stock():
    #市值200亿以上的股票
    today = datetime.date.today().strftime('%Y-%m-%d')
    global last_trade_day
    last_trade_day = mongo_client['fire_data']['trade_day'].find({'date': {'$lt': today}}, {'_id': False}).sort('date', -1).limit(1)
    last_trade_day  = list(last_trade_day)[0]['date']
    t_df = g_var.pool_df[:-3] #去除指数
    r = w.wsd(','.join(t_df.index.tolist()),"float_a_shares", last_trade_day, last_trade_day, "unit=1;currencyType=;Fill=Previous")
    print(r)
    t_df['float_a_shares'] = w.wsd(','.join(t_df.index.tolist()),"float_a_shares", last_trade_day, last_trade_day, "unit=1;currencyType=;Fill=Previous").Data[0]
    t_df['close'] = w.wsd(','.join(t_df.index.tolist()),"close", last_trade_day, last_trade_day, "Fill=Previous").Data[0]
    t_df['mkt'] = t_df.close * t_df.float_a_shares
    t_df = t_df[t_df.mkt >=20000000000]
    t_df.loc[:,'weight'] = t_df.weight/t_df.weight.sum()
    g_var.gte200_df = t_df[['weight','rt_pct_chg']]


def fetch_sinagl_df():
    #信号生成的复合策略
    cc = mongo_client['fire_moniter']['daily_strategy_list']
    record = cc.find_one({'date':last_trade_day }, {'_id': 0})
    strategy_list = record['strategy_list']
    bull_reduce = record['bull_reduce']
    bull_list = list(set(strategy_list).difference(set(bull_reduce)))
    if len(bull_list) < 1:
        df = pd.DataFrame({'code':[],'weight':[],'rt_pct_chg':[]})
        df = df.set_index('code')
    else:
        cursor = output_db['strategy_list_output01'].find({'date': output_date, 'strategy': {'$in': bull_list}},
                                                          {'_id': 0, 'code': 1, 'weight': 1, 'wind_code': 1})
        df = pd.DataFrame(list(cursor))
        df = df.groupby(['code', 'wind_code']).agg({'weight': sum})
        df = df.reset_index()
        df.loc[:,'code'] = df.wind_code
        df.loc[:,'weight'] = df.weight/df.weight.sum() #监控单独的每个子策略，子策略的权重需要归一化
        df = df[['code','weight']]
        df = df.set_index('code')
        df['rt_pct_chg'] = 0.0
    g_var.signal_df = df

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
    fetch_gte200_stock()
    #fetch_sinagl_df()
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
