#! /user/bin/env python
# encoding:utf-8
# 生成各产品篮子
from __future__ import division
import json

import datetime

import math
import pandas as pd
from pandas import ExcelWriter
import pymongo
from WindPy import w

output_mongo_client = pymongo.MongoClient('192.168.2.181', 27017)
output_db = output_mongo_client['fire_trade']

monitor_mongo_client = pymongo.MongoClient('192.168.2.112', 27017)
moniter_db = monitor_mongo_client['fire_moniter']

delivery_dates = ['2017-01-20', '2017-02-17', '2017-03-17', '2017-04-21', '2017-05-19', '2017-06-16', '2017-07-21',
                  '2017-08-18', '2017-09-15', '2017-10-20', '2017-11-17', '2017-12-15']


class BasketGenerator(object):
    def __init__(self):
        self.products = None  # 产品列表
        self.today = datetime.date.today().strftime('%Y-%m-%d')
        self.get_last_tradeDay()  # 如果是非交易日，需要获取上一个交易日
        self.total_weight = None  # 票池合计权重
        self.output_date = None  # 策略生成日期
        self.strategy_list = None
        self.bull_reduce = None
        self.ic_reduce = None
        self.if_reduce = None
        self.ih_reduce = None
        self.bull_df = None  # 裸多部分票池
        self.ic_df = None  # ic部分票池
        self.if_df = None  # if部分票池
        self.ih_df = None  # ih部分票池
        self.hedge_close = {} #对冲期货上个交易日的收盘价
        self.hedge_multiplier = {'IC':200,'IF':300,'IH':300} #合约乘数
        self.out_strategy_list = []  # 不在策略内的股票，需要从票池和当前持仓里面去掉，并调整权重
        self.position_files = {u'一号产品': u'20170425 1号当日现货持仓.xlsx',
                               u'术源九州': u'20170425术源九州现货持仓信息.xls01', u'自强一号': u'自强.xls01',
                               u'兴鑫': u'兴鑫.xls', u'华泰吴雪敏': u'吴雪敏.xls01'}
        self.load_config()

    def load_config(self):
        # 导入产品配置
        filename = 'product_config.json'
        with open(filename, 'r') as f:
            config = json.load(f)
        self.products = config['products']

    def get_last_tradeDay(self):
        #获取最近的一个交易日
        trade_days = monitor_mongo_client['fire_data']['trade_day'].find({'date': {'$lte': self.today}},
                                                                         {'_id': False}).sort('date', -1).limit(1)
        self.today = list(trade_days)[0]['date']

    def start(self):
        self.get_adjust_weight()
        self.fetch_signal()
        self.fetch_bull_df()
        self.fetch_ic_df()
        self.fetch_if_df()
        self.fetch_ih_df()
        self.fetch_close_price()
        self.gen_position_num()

    def gen_position_num(self):
        # 生成持仓数量
        for product in self.products:
            name = product['name']
            plan_ic_hand = plan_if_hand = plan_ih_hand = 0 #计划IC,IF和IH的持仓手数的初始值都设置为0
            bull_amount = product['bull_amount']
            plan_df = None
            if bull_amount > 0:
                if self.bull_df is not None:
                    bull_df = self.bull_df
                    bull_df['position_num'] = 0
                    bull_df = self.approach_totalAmount(bull_amount, bull_df)
                    plan_df = bull_df
                    bull_df.to_excel(name + u'裸多计划持仓.xlsx', index=False)
            ic_amount = product['ic_amount']
            if ic_amount > 0:
                if self.ic_df is not None:
                    ic_adj_result = self.__adjust_hedge_amount(ic_amount,'IC')
                    ic_amount = ic_adj_result['amount']
                    plan_ic_hand = ic_adj_result['hand']
                    if ic_amount > 0:
                        ic_df = self.ic_df
                        ic_df['position_num'] = 0
                        ic_df = self.approach_totalAmount(ic_amount, ic_df)
                        if plan_df is not None:
                            plan_df = plan_df.append(ic_df)
                        else:
                            plan_df = ic_df
                        ic_df.to_excel(name + u'IC计划持仓'+str(plan_ic_hand)+u'手.xlsx', index=False)
            if_amount = product['if_amount']
            if if_amount > 0:
                if self.if_df is not None:
                    if_adj_result = self.__adjust_hedge_amount(if_amount,'IF')
                    if_amount = if_adj_result['amount']
                    plan_if_hand = if_adj_result['hand']
                    if if_amount > 0:
                        if_df = self.if_df
                        if_df['position_num'] = 0
                        if_df = self.approach_totalAmount(if_amount, if_df)
                        if plan_df is not None:
                            plan_df = plan_df.append(if_df)
                        else:
                            plan_df = if_df
                        if_df.to_excel(name + u'IF计划持仓'+str(plan_if_hand)+u'手.xlsx', index=False)
            ih_amount = product['ih_amount']
            if ih_amount > 0:
                if self.ih_df is not None:
                    ih_adj_result = self.__adjust_hedge_amount(ih_amount,'IH')
                    ih_amount = ih_adj_result['amount']
                    plan_ih_hand = ih_adj_result['hand']
                    if ih_amount > 0:
                        ih_df = self.ih_df
                        ih_df['position_num'] = 0
                        ih_df = self.approach_totalAmount(ih_amount, ih_df)
                        if plan_df is not None:
                            plan_df = plan_df.append(ih_df)
                        else:
                            plan_df = ih_df
                        ih_df.to_excel(name + u'IH计划持仓'+str(plan_ih_hand)+u'手.xlsx', index=False)
            now_df = self.parse_position(product, self.position_files[name])
            if plan_df is not None:
                plan_df = plan_df.groupby('code').agg(
                        {'name': 'first', 'wind_code': 'first', 'close': 'first', 'weight': sum, 'position_num': sum})
                plan_df = plan_df.reset_index()
                now_df = now_df[['code', 'name', 'position_num']]
                now_df.columns = ['code', 'name', 'old_position_num']
                plan_df = plan_df.set_index(['code', 'name'])
                now_df = now_df.set_index(['code', 'name'])
                plan_df = plan_df.join(now_df, how='outer')
                plan_df = plan_df.fillna(0)
                plan_df.loc[:, 'position_num'] = plan_df['position_num'] - plan_df['old_position_num']
                plan_df = plan_df.reset_index()
                # plan_df.to_excel(name + u'加减仓.xlsx', index=False)
            else:
                plan_df = now_df[['code', 'name', 'position_num']]
                plan_df[:, 'position_num'] = -plan_df.position_num
            self.format_basket(name, product['clientType'], plan_df)

    def __adjust_hedge_amount(self,amount,type):
        #调整对冲端资金额度，使得是整手的期货合约
        #type 必须是'IC','IF','IH'中的一个
        #返回结果为{'hand':hand,'amount':amount}
        #其中的hand为期货手数,amount为调整后的期货端金额
        print('...............')
        print(amount)
        print(type)
        if type == 'IC':
            total_weight = self.ic_df.weight.sum()
            f_close = self.hedge_close['IC']
            f_multiplier = self.hedge_multiplier['IC']
        elif type == 'IF':
            total_weight = self.if_df.weight.sum()
            f_close = self.hedge_close['IF']
            f_multiplier = self.hedge_multiplier['IF']
        elif type == 'IH':
            total_weight = self.ih_df.weight.sum()
            f_close = self.hedge_close['IH']
            f_multiplier = self.hedge_multiplier['IH']
        else:
            raise Exception('Unknown hedge type:'+type)
        print(f_close)
        print(f_multiplier)
        hands = amount*total_weight/(f_close*f_multiplier)
        print(hands)
        if hands - math.floor(hands) >= 0.7:
            hands = math.ceil(hands)
        else:
            hands = math.floor(hands)
        result = {}
        result['hand'] = hands
        result['amount'] = hands*f_close*f_multiplier/total_weight
        return result

    def format_basket(self, product, clientType, df):
        # 将票池转化为对应的格式
        if clientType == 'ims':
            df01 = df[df['code'].str.startswith('60')]
            df01['market_type'] = 0
            df = df[~df['code'].str.startswith('60')]
            df['market_type'] = 1
            df = df.append(df01)
            df = df[['code', 'market_type', 'position_num']]
            df.columns = [u'合约代码', u'市场', u'数量/权重']
            reduce_df = df[df[u'数量/权重'] < 0]
            reduce_df.loc[:, u'数量/权重'] = - reduce_df[u'数量/权重']
            df = df[df[u'数量/权重'] >= 0]
            writer = ExcelWriter(product + u'加减仓.xlsx')
            df.to_excel(writer, u'加仓篮子', index=False)
            reduce_df.to_excel(writer, u'减仓篮子', index=False)
            writer.save()
        elif clientType == 'pb':
            df01 = df[df['code'].str.startswith('60')]
            df01['market_type'] = 1
            df = df[~df['code'].str.startswith('60')]
            df['market_type'] = 2
            df = df.append(df01)
            df = df[['code', 'name', 'market_type', 'position_num']]
            df.columns = [u'证券代码', u'证券名称', u'交易市场', u'数量/权重']
            reduce_df = df[df[u'数量/权重'] < 0]
            reduce_df.loc[:, u'数量/权重'] = - reduce_df[u'数量/权重']
            df = df[df[u'数量/权重'] >= 0]
            writer = ExcelWriter(product + u'加减仓.xlsx')
            df.to_excel(writer, u'加仓篮子', index=False)
            reduce_df.to_excel(writer, u'减仓篮子', index=False)
            writer.save()
        else:
            df['d_weight'] = 1
            df['d_dr'] = 1
            df = df[['code', 'name', 'position_num', 'd_weight', 'd_dr']]
            df.columns = [u'证券代码', u'证券名称', u'目标数量', u'目标权重', u'方向']
            reduce_df = df[df[u'目标数量'] < 0]
            reduce_df.loc[:, u'目标数量'] = - reduce_df[u'目标数量']
            df = df[df[u'目标数量'] >= 0]
            writer = ExcelWriter(product + u'加减仓.xlsx')
            df.to_excel(writer, u'加仓篮子', index=False)
            reduce_df.to_excel(writer, u'减仓篮子', index=False)
            writer.save()

    def fetch_close_price(self):
        # 获取股票池的名字和当天收盘价
        w.start()
        index_secs = self.__get_future_contracts()
        index_close = w.wsd(index_secs,'close',self.today, self.today,'Fill=Previous').Data[0]
        self.hedge_close = dict(zip(['IC','IF','IH'],index_close))
        if self.bull_df is not None:
            self.bull_df['close'] = \
                w.wsd(','.join(self.bull_df['wind_code'].tolist()), "close", self.today, self.today,
                      "Fill=Previous").Data[
                    0]
            self.bull_df['name'] = \
                w.wsd(','.join(self.bull_df['wind_code'].tolist()), "sec_name", self.today, self.today,
                      "Fill=Previous").Data[0]
        if self.ic_df is not None:
            self.ic_df['close'] = \
                w.wsd(','.join(self.ic_df['wind_code'].tolist()), "close", self.today, self.today,
                      "Fill=Previous").Data[0]
            self.ic_df['name'] = \
                w.wsd(','.join(self.ic_df['wind_code'].tolist()), "sec_name", self.today, self.today,
                      "Fill=Previous").Data[
                    0]
        if self.if_df is not None:
            self.if_df['close'] = \
                w.wsd(','.join(self.if_df['wind_code'].tolist()), "close", self.today, self.today,
                      "Fill=Previous").Data[0]
            self.if_df['name'] = \
                w.wsd(','.join(self.if_df['wind_code'].tolist()), "sec_name", self.today, self.today,
                      "Fill=Previous").Data[
                    0]
        if self.ih_df is not None:
            self.ih_df['close'] = \
                w.wsd(','.join(self.ih_df['wind_code'].tolist()), "close", self.today, self.today,
                      "Fill=Previous").Data[0]
            self.ih_df['name'] = \
                w.wsd(','.join(self.ih_df['wind_code'].tolist()), "sec_name", self.today, self.today,
                      "Fill=Previous").Data[
                    0]
        w.stop()

    def __get_future_contracts(self):
        #获取期货当月合约
        today = datetime.date.today().strftime('%Y-%m-%d')
        month = int(today[5:7])
        year = int(today[2:4])
        if today > delivery_dates[month -1]:
            month += 1
        if month > 12:
            month %= 12
            year += 1
        contract =  str(year) + '%02d'% month
        return ','.join([prefix + contract+'.CFE' for prefix in ['IC','IF','IH']])

    def get_adjust_weight(self):
        # 获取权重调整
        self.output_date = output_db['strategy_output_date'].find().sort('date', -1).limit(1)[0]['date']
        cursor = output_db['strategy_list_output01'].find({'date': self.output_date},
                                                          {'_id': 0, 'code': 1, 'weight': 1, 'wind_code': 1})
        df = pd.DataFrame(list(cursor))
        df = df.groupby(['code', 'wind_code']).agg({'weight': sum})
        df = df.reset_index()
        df = df[~df.code.isin(self.out_strategy_list)]
        self.total_weight = df.weight.sum()
        print('Total weight is:' + str(self.total_weight))

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
        print('ih_list lenght:' + str(len(ih_list)))
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

    def fetch_signal(self):
        # 获取当天的策略信号
        cc = moniter_db['daily_strategy_list']
        record = cc.find_one({'date': self.today}, {'_id': 0})
        self.strategy_list = record['strategy_list']
        self.bull_reduce = record['bull_reduce']
        self.ic_reduce = record['ic_reduce']
        self.if_reduce = record['if_reduce']
        self.ih_reduce = record['ih_reduce']

    def parse_position(self, product, filename):
        # 获取现在的持仓
        if product['clientType'] == 'pb':
            df = self.parse_hs_excel_position(filename)
        elif product['clientType'] == 'ims':
            df = self.parse_ims_excel_position(filename)
        elif product['clientType'] == 'xt':
            df = self.parse_xt_excel_position(filename)
        else:
            raise Exception(
                    'Unknown clientType,product is:' + product['name'] + ',clientType is:' + product['clientType'])
        df = df[~df.code.isin(self.out_strategy_list)]
        if len(product['insist_code']) > 0:
            insist_df = df[df.code.isin(product['insist_code'])]
            if len(insist_df) > 0:
                insist_df = insist_df.set_index('code')
                df = df[~df.code.isin(product['insist_code'])]
                insist_position = pd.Series(product['insist_num'], index=pd.Index(product['insist_code'],name='code'), name='insist_num')
                insist_df = insist_df.join(insist_position,how='inner')
                insist_df.loc[:, 'position_num'] = insist_df.position_num - insist_df.insist_num
                insist_df = insist_df[insist_df.position_num > 0]
                if len(insist_df) > 0:
                    insist_df = insist_df.reset_index()
                    insist_df = insist_df[['code', 'name', 'position_num', 'position_value']]
                    df = df.append(insist_df, ignore_index=True)
        return df

    def parse_ims_excel_position(self, filename):
        df = pd.read_excel(filename, converters={u'证券代码': str})
        df = df[[u'证券代码', u'证券名称', u'持仓数量', u'持仓市值']]
        df.columns = ['code', 'name', 'position_num', 'position_value']
        df = df[df.code.str.startswith('30') | df.code.str.startswith('60') | df.code.str.startswith('00')]
        df = df[df.position_num > 0]
        return df

    def parse_hs_excel_position(self, filename):
        df = pd.read_excel(filename, converters={u'证券代码': str})
        df = df[[u'证券代码', u'证券名称', u'持仓数量', u'持仓市值(全价)']]
        df.columns = ['code', 'name', 'position_num', 'position_value']
        df = df[df.code.str.startswith('30') | df.code.str.startswith('60') | df.code.str.startswith('00')]
        df = df[df.position_num > 0]
        return df

    def parse_xt_excel_position(self, filename):
        df = pd.read_excel(filename, converters={u'证券代码': str})
        df = df[[u'证券代码', u'证券名称', u'当前拥股', u'最新价']]
        df.columns = ['code', 'name', 'position_num', 'price']
        df = df[df.code.str.startswith('30') | df.code.str.startswith('60') | df.code.str.startswith('00')]
        df['position_value'] = df['position_num'] * df['price']
        df = df[['code', 'name', 'position_num', 'position_value']]
        df = df[df.position_num > 0]
        return df

    def approach_totalAmount(self, totalAmount, df):
        # 获取计划持仓股数，逼近总金额
        # totalAmount是计划持仓的总金额，df是带有收盘价的策略生成的票池DataFrame
        delta = 1000.0  # 最小的迭代步长
        totalAmount = totalAmount * df.weight.sum()  # 总金额根据权重进行调整
        df.position_num = 100 * ((totalAmount * df.weight) // (100 * df.close))
        real_total = (df.position_num * df.close).sum()
        new_totalAmount = totalAmount
        while real_total < totalAmount:
            new_totalAmount += min(totalAmount - real_total, delta)
            df.position_num = 100 * ((new_totalAmount * df.weight) // (100 * df.close))
            real_total = (df.position_num * df.close).sum()
        return df


if __name__ == '__main__':
    generator = BasketGenerator()
    generator.start()
