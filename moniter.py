#! /user/bin/env python
# encoding:utf-8
import datetime
import json
import random

import pymongo
import pandas as pd

import redis
import time
import tushare as ts
from flask import Flask, render_template, request

async_mode = None

app = Flask(__name__)

mongo_client = pymongo.MongoClient('192.168.2.112', 27017)
collection = mongo_client['fire_moniter']['daily_moniter']
data_db = mongo_client['fire_data']
#collection  = None

strategies = ['sz50', 'hs300', 'zz500', 'strategy']
index_names = ['ih_spread', 'if_spread', 'ic_spread']
index_code_dicts = {'ih_spread':'000016.SH', 'if_spread':'000300.SH', 'ic_spread':'000905.SH'}
basis_name = {'ih_spread':u'IH基差', 'if_spread':u'IF基差', 'ic_spread':u'IC基差'}
# strategies = ['sz50', 'hs300', 'zz500']
strategy_names = [u'上证50', u'沪深300', u'中证500', u'策略']
index_secs = ['000016.SH', '000300.SH', '000905.SH']
thread = None

r = redis.Redis(host='192.168.2.112', port=6379, db=0)
#r = None

@app.route('/')
def index():
    today = datetime.date.today().strftime('%Y-%m-%d')
    trade_days = data_db['trade_day'].find({'date':{'$lte':today}},{'_id':False}).sort('date', -1).limit(2)
    trade_days = list(trade_days)
    if trade_days[0]['date'] < today:
        last_trade_day = trade_days[0]['date']
        current_trade_day = last_trade_day
    else:
        last_trade_day = trade_days[1]['date']
        current_trade_day = trade_days[0]['date']
    cursor = collection.find({'date': current_trade_day}, {'_id': False}).sort('time_stamp', -1).limit(1)
    variable = {}
    if cursor.count() > 0:
        variable = list(cursor)[0]
    index_cursor = data_db['index_data'].find({'date':last_trade_day},{'_id':0,'close':1,'wind_code':1})
    df = pd.DataFrame(list(index_cursor))
    df = df.set_index('wind_code')
    # ih_last_close =  ts.get_k_data('000016',index=True,start=last_day,end=last_day).close.iloc[0]
    # if_last_close =  ts.get_k_data('000300',index=True,start=last_day,end=last_day).close.iloc[0]
    # ic_last_close =  ts.get_k_data('000905',index=True,start=last_day,end=last_day).close.iloc[0]
    # variable['ih_last_close'] = ih_last_close
    # variable['if_last_close'] = ih_last_close
    # variable['ic_last_close'] = ih_last_close
    variable['ih_last_close'] = df.loc['000016.SH'].close
    variable['if_last_close'] = df.loc['000300.SH'].close
    variable['ic_last_close'] = df.loc['000905.SH'].close
    return render_template('index.html',variable=variable)


@app.route('/rt_pct/')
def rt_pct():
    today = datetime.date.today().strftime('%Y-%m-%d')
    trade_days = data_db['trade_day'].find({'date':{'$lte':today}},{'_id':False}).sort('date', -1).limit(2)
    trade_days = list(trade_days)
    if trade_days[0]['date'] < today:
        last_trade_day = trade_days[0]['date']
        current_trade_day = last_trade_day
    else:
        last_trade_day = trade_days[1]['date']
        current_trade_day = trade_days[0]['date']

    index_cursor = data_db['index_data'].find({'date':last_trade_day},{'_id':0,'close':1,'wind_code':1})
    index_df = pd.DataFrame(list(index_cursor))
    index_df = index_df.set_index('wind_code')

    cursor = collection.find({'date': current_trade_day}, {'_id': False})
    response_data = {}
    response_data['retCode'] = 1
    response_data['retMsg'] = 'Success'
    response_data['data'] = []
    #df = pd.read_csv('data.csv')
    df = None
    if cursor.count() > 0:
        df = pd.DataFrame(list(cursor))
    if df is not None and len(df) > 0:
        df.loc[:, 'time_stamp'] = 1000 * df['time_stamp']
        for index_name in index_names:
            df.loc[:,index_name] = df[index_name]/index_df.loc[index_code_dicts[index_name]].close
    today = datetime.date.today().strftime('%Y-%m-%d')
    min_time = today + ' 09:14:00'
    max_time = today + ' 15:01:00'
    min_time = 1000*time.mktime(datetime.datetime.strptime(min_time,'%Y-%m-%d %H:%M:%S').timetuple())
    max_time = 1000*time.mktime(datetime.datetime.strptime(max_time,'%Y-%m-%d %H:%M:%S').timetuple())
    #first_data = r.mget( 'sz50', 'hs300', 'zz500', 'strategy')
    #first_data = ['0.634','0.543','0.313','0.654']
    i = 0
    for name in strategies:
        json_item = {}
        json_item['name'] = name
        json_item['id'] = name
        #json_item['data'] =[[min_time,float(first_data[i])]]
        json_item['data'] =[[min_time,0.0]]
        i += 1
        if df is not None and len(df) > 0:
            json_item['data'] += df[['time_stamp', name]].dropna().values.tolist()
        response_data['data'].append(json_item)
    time_range_item = {'name':'-','data':[[min_time,-0.0000],[max_time,0.0000]],'lineWidth':1} #用于调整时间轴
    response_data['data'].append(time_range_item)
    for name in index_names:
        json_item = {}
        json_item['name'] = basis_name[name]
        json_item['data'] =[]
        json_item['yAxis'] = 1
        if df is not None and len(df) > 0:
            json_item['data'] = df[['time_stamp', name]].dropna().values.tolist()
        response_data['data'].append(json_item)
    time_range_item = {'name':'-','data':[[min_time,-0.0000],[max_time,0.0000]],'yAxis':1,'lineWidth':1} #用于调整时间轴
    response_data['data'].append(time_range_item)
    return json.dumps(response_data)


@app.route('/tick/')
def tick():
    response_data = {}
    response_data['retCode'] = 1
    response_data['retMsg'] = 'Success'
    if isTrading():
        #response_data['data'] = [time.mktime(datetime.datetime.now().timetuple())]
        #response_data['data'] =r.mget('time_stamp', 'sz50', 'hs300', 'zz500', 'strategy')
        response_data['data'] = [datetime.datetime.now().strftime('%Y-%m-%d %H:%M'),random.random(),random.random(),random.random(), random.random()]
    else:
        response_data['data'] = []
    return json.dumps(response_data)


def isTrading():
    #当前是否是交易时段
    now = datetime.datetime.now().strftime('%H:%M')
    if now < '09:15':
        return False
    elif (now > '11:31') and (now < '12:59'):
        return False
    elif now > '15:01':
        return False
    else:
        return True


if __name__ == '__main__':
    app.run(host='192.168.2.136',port= 5000,debug=True)
