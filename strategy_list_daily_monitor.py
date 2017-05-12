#! /user/bin/env python
# encoding:utf-8
# 子策略净值跟踪
import datetime
import pymongo
import pandas as pd

from WindPy import w

output_mongo_client = pymongo.MongoClient('192.168.2.181', 27017)
output_db = output_mongo_client['fire_trade']
stock_position_cc = output_db['daily_strategies_position01']  # 策略每日的股票持仓
future_position_cc = output_db['strategies_future_position01']  # 策略每日的期货持仓

monitor_mongo_client = pymongo.MongoClient('192.168.2.112', 27017)
moniter_db = monitor_mongo_client['fire_moniter']
netvalue_cc = moniter_db['daily_netvalue']

holidays = ['2017-01-02', '2017-01-27', '2017-01-30', '2017-01-31', '2017-02-01', '2017-02-02', '2017-04-03',
            '2017-04-04', '2017-05-01', '2017-05-29', '2017-05-30', '2017-10-02',
            '2017-10-03', '2017-10-04', '2017-10-05', '2017-10-06']

delivery_dates = ['2017-01-20', '2017-02-17', '2017-03-17', '2017-04-21', '2017-05-19', '2017-06-16', '2017-07-21',
                  '2017-08-18', '2017-09-15', '2017-10-20', '2017-11-17', '2017-12-15']  # 期货换仓日

totalAmount = 10000000.0  # 股票端初始总金额
stock_open_commission = 0.0002  # 股票开仓手续费
stock_close_commission = 0.0012  # 股票平仓手续费
ic_open_commission = 0.00005  # 股指开仓手续费
ic_close_commission = 0.00005  # 股指平仓手续费

signal_strategy_names = ['signal', 'signal_alpha']  # 信号对应的策略名和alpha策略名


class StrategyListDailyMonitor(object):
    def __init__(self):
        self.w = w
        self.output_date = None  # 策略生成日期
        self.strategy_list = None  # 策略名称列表
        self.today = datetime.date.today().strftime('%Y-%m-%d')
        self.lastTradeDay = None  # 上一个交易日
        self.bull_reduce = None
        self.ic_reduce = None
        self.if_reduce = None
        self.ih_reduce = None
        self.bull_df = None  # 裸多部分票池
        self.ic_df = None  # ic部分票池
        self.if_df = None  # if部分票池
        self.ih_df = None  # ih部分票池
        self.out_strategy_list = []  # 不在策略内的股票，需要从票池里面去掉，并调整权重
        self.net_value_dict = dict()  # 用于保存各策略前一交易日的净值数据

        self.get_last_tradeDay()
        self.fetch_strategy_list()
        self.fetch_last_net_value()

    def set_date(self, date):
        # 把today 设置为其他日期，用于生成历史数据
        print('next set today as %s' % date)
        self.today = date

    def fetch_last_net_value(self):
        # 获取策略上一个交易日的净值数据
        # structure: {'date':'2017-05-10','name':'list_1_0','net_value':1.0013,'equity':10013435.0}
        cursor = netvalue_cc.find({'date': self.lastTradeDay}, {'_id': 0, 'name': 1, 'net_value': 1, 'equity': 1})
        strategies = signal_strategy_names + self.strategy_list  # 信号对应的裸多策略、对冲策略，以及各个子策略
        if cursor.count() > 0:
            fetched_strategies = []
            for item in cursor:
                if item['name'] in strategies:
                    self.net_value_dict[item['name']] = item
                    fetched_strategies.append(item['name'])
            init_strategy_list = [s_name for s_name in strategies if s_name not in fetched_strategies]
            for strategy_name in init_strategy_list:
                self.net_value_dict[strategy_name] = self.insert_init_net_value(strategy_name)
        else:
            for strategy_name in strategies:
                self.net_value_dict[strategy_name] = self.insert_init_net_value(strategy_name)

    def update_net_value(self):
        # 把策略净值存储到数据库
        pass

    def insert_init_net_value(self, strategy_name):
        # 策略还没有数据的时候，在上一个交易日插入净值为1.0000，初始资金为1000万的记录
        # 插入之前还是先查询一下（冗余操作）,如果发现已经有符合条件的记录，就抛出异常
        cursor = netvalue_cc.find({'date': self.lastTradeDay, 'name': strategy_name})
        if cursor.count() > 0:
            raise Exception('Strategy %s already has record at date %s ' % (strategy_name, self.lastTradeDay))
        data = {'date': self.lastTradeDay, 'name': strategy_name, 'net_value': 1.0000, 'equity': totalAmount}
        netvalue_cc.insert_one(data)
        del data['date']
        return data

    def fetch_strategy_list(self):
        # 获取策略列表
        self.output_date = output_db['strategy_output_date'].find().sort('date', -1).limit(1)[0]['date']
        cursor = output_db['strategy_list'].find({'date': self.output_date}, {'_id': 0, 'strategy_list': 1})
        strategy_list = list(cursor)[0]['strategy_list']
        print(strategy_list)
        self.strategy_list = strategy_list.split(',')

    def fetch_signal(self):
        # 获取加减仓信号
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

    def fetch_strategy_df(self, strategy_name):
        # 获取指定策略的票池,需要进行归一化
        cursor = output_db['strategy_list_output01'].find({'date': self.output_date, 'strategy': strategy_name},
                                                          {'_id': 0, 'code': 1, 'weight': 1, 'wind_code': 1})
        df = pd.DataFrame(list(cursor))
        df.loc[:, 'weight'] = df.weight / df.weight.sum()
        return df

    def fetch_wsd_data(self, secs, ind_name):
        # 通过万得客户端获取数据
        result = self.w.wsd(secs, ind_name, self.today, self.today, "Fill=Previous")
        return result.Data[0]

    def get_last_tradeDay(self):
        # 获取上一个交易日
        trade_days = monitor_mongo_client['fire_data']['trade_day'].find({'date': {'$lt': self.today}},
                                                                         {'_id': False}).sort('date', -1).limit(1)
        self.lastTradeDay = list(trade_days)[0]['date']

    def get_current_position(self, strategy_name):
        # 获取当前持仓
        pass

    def __get_stock_position(self, strategy_name, date):
        # 获取股票持仓
        cursor = stock_position_cc.find({'date': date, 'strategy': strategy_name},
                                        {'_id': 0, 'strategy': 1, 'code': 1, 'name': 1, 'close': 1,
                                         'average_position': 1, 'position_num': 1, 'position_value': 1})
        if cursor.count() > 0:
            df = pd.DataFrame(list(cursor))
        else:
            # 一致性，返回一个空的DataFrame
            df = pd.DataFrame(
                    {'strategy': [], 'code': [], 'name': [], 'close': [], 'average_position': [], 'position_num': [],
                     'position_value': []})
        return df

    def __get_future_position(self, strategy_name, date):
        # 获取期货持仓
        cursor = future_position_cc.find({'date': date, 'strategy': strategy_name},
                                         {'_id': 0, 'strategy': 1, 'code': 1, 'name': 1, 'close': 1,
                                          'average_position': 1, 'position_num': 1, 'position_value': 1})
        if cursor.count() > 0:
            pass
        else:
            pass

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

    def settlement(self):
        # 进行结算
        self.__settlement_stock()
        self.__settlement_future()

    def __settlement_stock(self, strategy_name):
        # 股票端结算
        pass

    def __settlement_future(self, strategy_name):
        # 期货端结算
        pass

    def get_wind_code(self, df):
        # 获取万得代码
        df01 = df[~df.code.str.startswith('60')]
        df = df[df.code.str.startswith('60')]
        df01['wind_code'] = df01.code + '.SZ'
        df['wind_code'] = df.code + '.SH'
        df = df.append(df01)
        return df

    def __close_last_stock_position(self, df):
        # 平掉上个上个交易日的股票持仓
        # df 是上个交易日的股票持仓
        # 返回平仓后的权益
        df = self.get_wind_code(df)
        df['vwap'] = self.fetch_wsd_data(','.join(df.wind_code.tolist()),'vwap')
        halt_df = df[df.vwap.isnull()]  # 停牌票
        equity = halt_df.position_value.sum()   # 停牌票按照前一天的收盘价处理掉
        df = df.dropna()
        equity += (df.vwap * df.position_num).sum()
        equity *= (1 - stock_close_commission)  # 佣金和印花税
        return equity

    def __open_stock_position(self, strategy_name):
        # 股票端开仓
        pass

    def __convert_stock(self, strategy_name):
        # 股票换仓
        now_position = self.__get_stock_position(strategy_name, self.lastTradeDay)
        if len(now_position) > 0:
            # 先平掉之前的仓位，然后建立新仓
            self.net_value_dict[strategy_name]['equity'] = self.__close_last_stock_position(now_position)
        self.__open_stock_position(strategy_name)

    def calculate_signal_net_value(self):
        # 计算信号的净值数据
        pass

    def process_sub_strategy(self):
        # 处理子策略
        isConvertibleDay = self.isConvertibleDay()
        if isConvertibleDay:
            # 子策略换仓
            pass
        else:
            # 逐日结算
            pass

    def process_signal_strategy(self):
        # 处理信号策略
        pass

    def start(self):
        self.w.start()


if __name__ == '__main__':
    strategyListDailyMonitor = StrategyListDailyMonitor()
    strategyListDailyMonitor.start()
