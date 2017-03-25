#! /user/bin/env python
# encoding:utf-8
import datetime
import json
import random
from copy import deepcopy

import pymongo
import pandas as pd
import threading

import redis
import time
from flask import Flask, render_template, request
from flask.ext.socketio import SocketIO, emit, send

async_mode = None

app = Flask(__name__)
app.config['SECRET_KEY'] = 'secret!'
socketio = SocketIO(app,async_mode=async_mode)

mongo_client = pymongo.MongoClient('192.168.2.112', 27017)
collection = mongo_client['fire_moniter']['daily_moniter']
#collection  = None

strategies = ['sz50', 'hs300', 'zz500', 'strategy']
# strategies = ['sz50', 'hs300', 'zz500']
strategy_names = [u'上证50', u'沪深300', u'中证500', u'策略']
index_secs = ['000016.SH', '000300.SH', '000905.SH']
thread = None

r = redis.Redis(host='192.168.2.112', port=6379, db=0)
#r = None


def background_thread():
    """Example of how to send server generated events to clients."""
    #count = 0
    while True:
        socketio.sleep(2)
        #count += 1
        # if not isTrading():
        #     continue
        data = r.mget('time_stamp', 'sz50', 'hs300', 'zz500', 'strategy')
        for i in range(1,len(data)):
            data[i] = float(data[i])
        socketio.emit('onTick',
                      {'time_stamp': data[0], 'sz50': data[1], 'hs300': data[2], 'zz500': data[3], 'strategy': data[4]})
        # socketio.emit('onTick',
        #               {'time_stamp': datetime.datetime.now().strftime('%Y-%m-%d %H:%M'), 'sz50': random.random(), 'hs300': random.random(), 'zz500': random.random(), 'strategy': random.random()})


@app.route('/')
def index():
    return render_template('index.html')


@app.route('/rt_pct/')
def rt_pct():
    date = datetime.date.today().strftime('%Y-%m-%d')
    cursor = collection.find({'date': date}, {'_id': False})
    response_data = {}
    response_data['retCode'] = 1
    response_data['retMsg'] = 'Success'
    response_data['data'] = []
    #df = pd.read_csv('data.csv')
    if cursor.count() > 0:
    #if len(df) > 0:
        df = pd.DataFrame(list(cursor))
        df.loc[:, 'time_stamp'] = 1000 * df['time_stamp']
        df.loc[:, 'sz50'] = 100 * df['sz50']
        df.loc[:, 'hs300'] = 100 * df['hs300']
        df.loc[:, 'zz500'] = 100 * df['zz500']
        df.loc[:, 'strategy'] = 100 * df['strategy']
        for name in strategies:
            # json_item = {}
            # json_item['name'] = name
            # json_item['data'] = df[['time_stamp', name]].dropna().values.tolist()
            # response_data['data'].append(json_item)
            response_data['data'].append(df[name].dropna().tolist())
        response_data['data'].append(df['str_time'].dropna().tolist())
    return json.dumps(response_data)


@app.route('/tick/')
def tick():
    response_data = {}
    response_data['retCode'] = 1
    response_data['retMsg'] = 'Success'
    if isTrading():
        #response_data['data'] = [time.mktime(datetime.datetime.now().timetuple())]
        response_data['data'] =r.mget('time_stamp', 'sz50', 'hs300', 'zz500', 'strategy')
        #response_data['data'] = [datetime.datetime.now().strftime('%Y-%m-%d %H:%M'),random.random(),random.random(),random.random(), random.random()]
    else:
        response_data['data'] = []
    return json.dumps(response_data)


@socketio.on('my event')
def test_message(message):
    emit('my response', {'data': 'got it!'})


@socketio.on('connect')
def connect():
    print('Client connected-----------------------------------------')
    global thread
    if thread is None:
        thread = socketio.start_background_task(target=background_thread)
    emit('my_response', {'data': 'Connected', 'count': 0})


@socketio.on('disconnect')
def disconnect():
    print('Client disconnected', request.sid)


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
    socketio.run(app, debug=True)
    #app.run(host='192.168.2.136',port= 5000,debug=True)
