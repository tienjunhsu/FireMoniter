#! /user/bin/env python
# encoding:utf-8
# 子策略监控
import datetime
import pymongo
import pandas as pd
from WindPy import w

indexes = ['000016.SH', '000300.SH', '000905.SH']  # 指数万得代码，分别是上证50、沪深300和中证500
index_ratio = [0.75, 1.0, 0.7]  # IH,IF和IC对应的系数分别是0.75,1.0和0.7

output_mongo_client = pymongo.MongoClient('192.168.2.181', 27017)
output_db = output_mongo_client['fire_trade']

monitor_mongo_client = pymongo.MongoClient('192.168.2.112', 27017)
moniter_db = monitor_mongo_client['fire_moniter']


class StrategyListMonitor(object):
    def __init__(self):
        self.output_date = None  # 策略生成日期
        self.strategy_list = None  # 策略名称列表
        self.strategy_weight_dict = {}  # 子策略权重
        self.strategy_weight_list = []
        self.strategy_list_chg_dict = {}  # 子策略表现
        self.strategy_list_chg_list = []
        self.bull_reduce_list = []  # 裸多需要减仓的策略
        self.ic_reduce_list = []  # 对冲IC需要减仓的策略
        self.if_reduce_list = []  # 对冲IF需要减仓的策略
        self.ih_reduce_list = []  # 对冲IH需要减仓的策略
        self.indexes_chg = None
        self.today = datetime.date.today().strftime('%Y-%m-%d')

        self.fetch_strategy_list()

    def fetch_strategy_list(self):
        self.output_date = output_db['strategy_output_date'].find().sort('date', -1).limit(1)[0]['date']
        cursor = output_db['strategy_list'].find({'date': self.output_date}, {'_id': 0, 'strategy_list': 1})
        strategy_list = list(cursor)[0]['strategy_list']
        print(strategy_list)
        self.strategy_list = strategy_list.split(',')

    def start(self):
        w.start()
        func = None
        now = datetime.datetime.now().strftime('%H:%M')
        if now > '15:30':
            func = 'self.fetch_chg_from_wind_wsd'
        else:
            func = 'self.fetch_chg_from_wind_wsq'
        func = eval(func)
        self.indexes_chg = func(','.join(indexes))  # 计算(获取指数的涨跌幅)

        for strategy in self.strategy_list:
            df = self.fetch_startegy_stock(strategy)
            self.strategy_weight_dict[strategy] = df.weight.sum()
            self.strategy_weight_list.append(df.weight.sum())
            df['pct_chg'] = func(','.join(df.wind_code.tolist()))
            self.strategy_list_chg_dict[strategy] = (df['pct_chg'] * (df['weight'] / df['weight'].sum())).sum()
            self.strategy_list_chg_list.append(self.strategy_list_chg_dict[strategy])

        self.get_signal()
        self.get_result()

    def get_signal(self):
        self.get_long_signal()
        self.get_hedge_signal()

    def get_long_signal(self):
        # 获取多头减仓信号
        for strategy in self.strategy_list:
            if self.indexes_chg[2] < 0:
                # 中证500指数下跌
                if self.strategy_list_chg_dict[strategy] < self.indexes_chg[2]:
                    # 策略跑输中证500指数
                    self.bull_reduce_list.append(strategy)

    def get_result(self):
        # 获取最后的结果
        result = dict()
        result['date'] = self.today
        result['index_list'] = indexes
        result['index_pct_chg'] = self.indexes_chg
        result['strategy_list'] = self.strategy_list
        result['strategy_weight'] = self.strategy_weight_list
        result['strategy_pct_chg'] = self.strategy_list_chg_list
        result['bull_reduce'] = self.bull_reduce_list
        result['ic_reduce'] = self.ic_reduce_list
        result['if_reduce'] = self.if_reduce_list
        result['ih_reduce'] = self.ih_reduce_list
        bull_reduce_weight = 0.0
        for strategy in self.bull_reduce_list:
            bull_reduce_weight += self.strategy_weight_dict[strategy]
        ic_reduce_weight = 0.0
        for strategy in self.ic_reduce_list:
            ic_reduce_weight += self.strategy_weight_dict[strategy] / 3
        if_reduce_weight = 0.0
        for strategy in self.if_reduce_list:
            if_reduce_weight += self.strategy_weight_dict[strategy] / 3
        ih_reduce_weight = 0.0
        for strategy in self.ih_reduce_list:
            ih_reduce_weight += self.strategy_weight_dict[strategy] / 3
        result['bull_reduce_weight'] = bull_reduce_weight
        result['ic_reduce_weight'] = ic_reduce_weight
        result['ih_reduce_weight'] = ih_reduce_weight
        result['if_reduce_weight'] = if_reduce_weight
        moniter_db['daily_strategy_list'].insert_one(result)

    def get_hedge_signal(self):
        # 获取对冲减仓信号
        for strategy in self.strategy_list:
            if self.strategy_list_chg_dict[strategy] < self.indexes_chg[0] * index_ratio[0]:
                self.ih_reduce_list.append(strategy)
            if self.strategy_list_chg_dict[strategy] < self.indexes_chg[1] * index_ratio[1]:
                self.if_reduce_list.append(strategy)
            if self.strategy_list_chg_dict[strategy] < self.indexes_chg[2] * index_ratio[2]:
                self.ic_reduce_list.append(strategy)

    def fetch_startegy_stock(self, strategy):
        cursor = output_db['strategy_list_output01'].find({'date': self.output_date, 'strategy': strategy},
                                                          {'_id': 0, 'code': 1, 'weight': 1, 'wind_code': 1})
        df = pd.DataFrame(list(cursor))
        return df

    def fetch_chg_from_wind_wsd(self, secs):
        # 从万得接口获取涨跌幅数据,通过日期函数获取，这个得到每天15：30后才能获取当前的数据
        r = w.wsd(secs, "pct_chg", self.today, self.today, "")
        data = r.Data[0]
        return data

    def fetch_chg_from_wind_wsq(self, secs):
        # 从万得接口获取涨跌幅数据,通过实时行情获取数据快照数据，带15:30之前获取,后面数据就会不准确
        r = w.wsq(secs, "rt_pct_chg")
        data = r.Data[0]
        return data

    def pretty_result(self):
        cc = moniter_db['daily_strategy_list']
        record = cc.find_one({'date': self.today}, {'_id': 0})
        result = dict()
        result[u'子策略'] = record['strategy_list']
        result[u'策略权重'] = record['strategy_weight']
        result[u'策略表现'] = record['strategy_pct_chg']
        result[u'指数增强'] = []
        result[u'IC'] = []
        result[u'IF'] = []
        result[u'IH'] = []
        result[u'strategy_number'] = []
        columns = {u'指数增强': 'bull_reduce', u'IC': 'ic_reduce', u'IF': 'if_reduce', u'IH': 'ih_reduce'}
        for i in range(len(record['strategy_list'])):
            result[u'strategy_number'].append(int(record['strategy_list'][i].split('_')[1]))
            for k, v in columns.items():
                if record['strategy_list'][i] in record[v]:
                    result[k].append(u'减')
                else:
                    result[k].append(u'加|持')
        df = pd.DataFrame(result)
        df = df.sort_values(by=u'strategy_number')
        df.to_csv('startegy_signal_' + self.today + '.csv',
                  columns=[u'子策略', u'策略权重', u'策略表现', u'指数增强', u'IC', u'IF', u'IH'], encoding='gbk',
                  index=False)  # 编码方式选为gbk,excel打开没有乱码


if __name__ == '__main__':
    strategyListMonitor = StrategyListMonitor()
    strategyListMonitor.start()
    strategyListMonitor.pretty_result()
