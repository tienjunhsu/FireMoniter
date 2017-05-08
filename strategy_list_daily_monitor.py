#! /user/bin/env python
#encoding:utf-8
#子策略净值跟踪
import datetime
import pymongo
import pandas as pd
from WindPy import w

output_mongo_client = pymongo.MongoClient('192.168.2.181', 27017)
output_db = output_mongo_client['fire_trade']
stock_position_cc = output_db['daily_strategies_position01']  #策略每日的股票持仓
future_position_cc = output_db['strategies_future_position01'] #策略每日的期货持仓

monitor_mongo_client = pymongo.MongoClient('192.168.2.112', 27017)
moniter_db = monitor_mongo_client['fire_moniter']
netvalue_cc = moniter_db['daily_netvalue']

holidays = ['2017-01-02', '2017-01-27', '2017-01-30', '2017-01-31', '2017-02-01', '2017-02-02', '2017-04-03',
            '2017-04-04', '2017-05-01', '2017-05-29', '2017-05-30', '2017-10-02',
            '2017-10-03', '2017-10-04', '2017-10-05', '2017-10-06']


delivery_dates = ['2017-01-20', '2017-02-17', '2017-03-17', '2017-04-21', '2017-05-19', '2017-06-16', '2017-07-21',
                  '2017-08-18', '2017-09-15', '2017-10-20', '2017-11-17', '2017-12-15']  # 期货换仓日


class StrategyListDailyMonitor(object):
    def __init__(self):
        self.w = w
        self.output_date = None  # 策略生成日期
        self.strategy_list = None  # 策略名称列表
        self.today = datetime.date.today().strftime('%Y-%m-%d')
        self.lastTradeDay = None  #上一个交易日
        self.get_last_tradeDay()
        self.bull_reduce = None
        self.ic_reduce = None
        self.if_reduce = None
        self.ih_reduce = None
        self.bull_df = None  # 裸多部分票池
        self.ic_df = None  # ic部分票池
        self.if_df = None  # if部分票池
        self.ih_df = None  # ih部分票池
        self.out_strategy_list = []  # 不在策略内的股票，需要从票池里面去掉，并调整权重

        self.fetch_strategy_list()

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

    def fetch_bull_df(self):
        # 获取裸多部分票池
        bull_list = list(set(self.strategy_list).difference(set(self.bull_reduce)))
        print('bull_list lenght:' + str(len(bull_list)))
        if len(bull_list) < 1:
            return
        cursor = output_db['strategy_list_output01'].find({'date': self.output_date, 'strategy': {'$in': bull_list}},
                                                          {'_id': 0, 'code': 1, 'weight': 1, 'wind_code': 1})
        df = pd.DataFrame(list(cursor))
        df = df.groupby(['code', 'wind_code']).agg({'weight': sum})
        df = df.reset_index()
        df = df[~df.code.isin(self.out_strategy_list)]
        df.loc[:, 'weight'] = df['weight'] / self.total_weight
        self.bull_df = df

    def fetch_ic_df(self):
        # 获取IC部分票池
        ic_list = list(set(self.strategy_list).difference(set(self.ic_reduce)))
        print('ic_list lenght:' + str(len(ic_list)))
        if len(ic_list) < 1:
            return
        cursor = output_db['strategy_list_output01'].find({'date': self.output_date, 'strategy': {'$in': ic_list}},
                                                          {'_id': 0, 'code': 1, 'weight': 1, 'wind_code': 1})
        df = pd.DataFrame(list(cursor))
        df = df.groupby(['code', 'wind_code']).agg({'weight': sum})
        df = df.reset_index()
        df = df[~df.code.isin(self.out_strategy_list)]
        df.loc[:, 'weight'] = df['weight'] / self.total_weight
        self.ic_df = df

    def fetch_if_df(self):
        # 获取IF部分票池
        if_list = list(set(self.strategy_list).difference(set(self.if_reduce)))
        print('if_list lenght:' + str(len(if_list)))
        if len(if_list) < 1:
            return
        cursor = output_db['strategy_list_output01'].find({'date': self.output_date, 'strategy': {'$in': if_list}},
                                                          {'_id': 0, 'code': 1, 'weight': 1, 'wind_code': 1})
        df = pd.DataFrame(list(cursor))
        df = df.groupby(['code', 'wind_code']).agg({'weight': sum})
        df = df.reset_index()
        df = df[~df.code.isin(self.out_strategy_list)]
        df.loc[:, 'weight'] = df['weight'] / self.total_weight
        self.if_df = df

    def fetch_ih_df(self):
        # 获取IH部分票池
        ih_list = list(set(self.strategy_list).difference(set(self.ih_reduce)))
        print('ih_list length:' + str(len(ih_list)))
        if len(ih_list) < 1:
            return
        cursor = output_db['strategy_list_output01'].find({'date': self.output_date, 'strategy': {'$in': ih_list}},
                                                          {'_id': 0, 'code': 1, 'weight': 1, 'wind_code': 1})
        df = pd.DataFrame(list(cursor))
        df = df.groupby(['code', 'wind_code']).agg({'weight': sum})
        df = df.reset_index()
        df = df[~df.code.isin(self.out_strategy_list)]
        df.loc[:, 'weight'] = df['weight'] / self.total_weight
        self.ih_df = df

    def get_last_tradeDay(self):
        #获取上一个交易日
        trade_days = monitor_mongo_client['fire_data']['trade_day'].find({'date': {'$lt': self.today}},
                                                                         {'_id': False}).sort('date', -1).limit(1)
        self.lastTradeDay = list(trade_days)[0]['date']

    def isConvertibleDay(self):
        # 判断今日是不是换仓日，如果是，则需要获取策略生成的股票池，作为持仓列表
        # 否则就复制上个交易日的持仓
        if self.lastTradeDay < self.output_date:
            # 在上一个交易日之后策略有生成票池，说明今天是换仓日
            return True
        else:
            return False

    def is_funture_convertibleDay(self):
        # 判断是不是期货换仓日（固定在每个期货交割日换仓），用主力合约
        if self.today in delivery_dates:
            return True
        else:
            return False

    def start(self):
        pass


if __name__ == '__main__':
    strategyListDailyMonitor = StrategyListDailyMonitor()
    strategyListDailyMonitor.start()
