#! /user/bin/env python
#encoding:utf-8
#子策略净值跟踪
import datetime
import pymongo
import pandas as pd
from WindPy import w

output_mongo_client = pymongo.MongoClient('192.168.2.181', 27017)
output_db = output_mongo_client['fire_trade']

monitor_mongo_client = pymongo.MongoClient('192.168.2.112', 27017)
moniter_db = monitor_mongo_client['fire_moniter']

holidays = ['2017-01-02', '2017-01-27', '2017-01-30', '2017-01-31', '2017-02-01', '2017-02-02', '2017-04-03',
            '2017-04-04', '2017-05-01', '2017-05-29', '2017-05-30', '2017-10-02',
            '2017-10-03', '2017-10-04', '2017-10-05', '2017-10-06']


class StrategyListDailyMonitor(object):
    def __init__(self):
        self.output_date = None  # 策略生成日期
        self.strategy_list = None  # 策略名称列表
        self.today = datetime.date.today().strftime('%Y-%m-%d')

    def fetch_strategy_list(self):
        #获取策略列表
        self.output_date = output_db['strategy_output_date'].find().sort('date', -1).limit(1)[0]['date']
        cursor = output_db['strategy_list'].find({'date': self.output_date}, {'_id': 0, 'strategy_list': 1})
        strategy_list = list(cursor)[0]['strategy_list']
        print(strategy_list)
        self.strategy_list = strategy_list.split(',')

    def fetch_signal(self):
        #获取加减仓信号
        cc = moniter_db['daily_strategy_list']
        record = cc.find_one({'date': self.today}, {'_id': 0})
        self.strategy_list = record['strategy_list']
        self.bull_reduce = record['bull_reduce']
        self.ic_reduce = record['ic_reduce']
        self.if_reduce = record['if_reduce']
        self.ih_reduce = record['ih_reduce']

    def get_last_tradeDay(self):
        #获取上一个交易日
        trade_days = monitor_mongo_client['fire_data']['trade_day'].find({'date': {'$lt': self.today}},
                                                                         {'_id': False}).sort('date', -1).limit(1)
        self.today = list(trade_days)[0]['date']


    def start(self):
        pass


if __name__ == '__main__':
    strategyListDailyMonitor = StrategyListDailyMonitor()
    strategyListDailyMonitor.start()
