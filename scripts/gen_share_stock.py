#! /user/bin/env python
# encoding:utf-8
# 吴雪敏账户根据指数成分股生成票池

from __future__ import division

import datetime
import pandas as pd
import pymongo

from WindPy import w

output_mongo_client = pymongo.MongoClient('192.168.2.181', 27017)
output_db = output_mongo_client['fire_trade']

monitor_mongo_client = pymongo.MongoClient('192.168.2.112', 27017)


class ShareStockTool(object):
    def __init__(self):
        self.w = w
        self.output_date = None
        self.stock_df = None
        self.share50_df = None
        self.share300_df = None
        self.share500_df = None
        self.today = datetime.date.today().strftime('%Y-%m-%d')
        self.lastTradeDay = None
        self.totalAmount = 24000000.0
        self.product_name = u'吴雪敏'

        self.__wind_init()
        self.get_last_tradeDay()
        self.fetch_stock()

    def __wind_init(self):
        self.w.start()

    def get_last_tradeDay(self):
        # 获取上一个交易日
        trade_days = monitor_mongo_client['fire_data']['trade_day'].find({'date': {'$lt': self.today}},
                                                                         {'_id': False}).sort('date', -1).limit(1)
        self.lastTradeDay = list(trade_days)[0]['date']

    def set_totalAmount(self, totalAmount):
        self.totalAmount = totalAmount

    def set_productName(self,product_name ):
        self.product_name = product_name

    def fetch_stock(self):
        self.fetch_output_stock()
        self.get_share50_stock()
        self.get_share300_stock()
        self.get_share500_stock()

    def fetch_output_stock(self):
        self.output_date = output_db['strategy_output_date'].find().sort('date', -1).limit(1)[0]['date']
        cursor = output_db['strategy_list_output01'].find({'date': self.output_date},
                                                          {'_id': 0, 'code': 1, 'weight': 1, 'wind_code': 1})
        df = pd.DataFrame(list(cursor))
        df = df.groupby(['code', 'wind_code']).agg({'weight': sum})
        df = df.reset_index()
        self.stock_df = df

    def get_share500_stock(self):
        # 获取中证500指数成分股
        para = 'date={0};sectorid=1000008491000000;field=wind_code'.format(self.today)
        wind_code = w.wset("sectorconstituent", para).Data[0]
        self.share500_df = self.stock_df[self.stock_df.wind_code.isin(wind_code)]

    def get_share300_stock(self):
        # 获取沪深300指数成分股
        para = 'date={0};sectorid=1000000090000000;field=wind_code'.format(self.today)
        wind_code = w.wset("sectorconstituent", para).Data[0]
        self.share300_df = self.stock_df[self.stock_df.wind_code.isin(wind_code)]

    def get_share50_stock(self):
        # 获取上证50指数成分股
        para = 'date={0};sectorid=1000000087000000;field=wind_code'.format(self.today)
        wind_code = w.wset("sectorconstituent", para).Data[0]
        self.share50_df = self.stock_df[self.stock_df.wind_code.isin(wind_code)]

    def fetch_wsd_data(self, secs, ind_name, date):
        # 通过万得客户端获取数据
        result = self.w.wsd(secs, ind_name, date, date, "Fill=Previous")
        return result.Data[0]

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

    def gen_equal_ratio_open_stock(self):
        # 按资金1:1:1 的分配给成分股
        amount = self.totalAmount/3
        out_df = None
        for df in [self.share500_df, self.share300_df , self.share50_df]:
            df.loc[:, 'weight'] = df.weight/df.weight.sum()
            df['close'] = self.fetch_wsd_data(','.join(df.wind_code.tolist()), 'close', self.lastTradeDay)
            df['position_num'] = 0
            df = self.approach_totalAmount(amount, df)
            if out_df is None:
                out_df = df
            else:
                out_df = out_df.append(df, ignore_index=True)
        out_df = out_df[['code', 'wind_code', 'position_num']]
        out_df = out_df.groupby(['code', 'wind_code']).agg({'position_num': sum})
        out_df = out_df.reset_index()
        out_df['name'] = self.fetch_wsd_data(','.join(out_df.wind_code.tolist()), 'sec_name', self.lastTradeDay)
        out_df['close'] = self.fetch_wsd_data(','.join(out_df.wind_code.tolist()), 'close', self.lastTradeDay)
        out_df.to_excel(self.product_name + u'成分股资金等比例配比票池.xlsx', index=False)

    def gen_absolute_weight_open_stock(self):
        # 按照成分股的权重的绝对权重分配资金
        share_codes = self.share50_df.code.tolist() + self.share300_df.code.tolist() + self.share500_df.code.tolist()
        share_codes = list(set(share_codes))  # 去除重复的
        out_df = self.stock_df[self.stock_df.code.isin(share_codes)]
        out_df.loc[:, 'weight'] = out_df.weight/out_df.weight.sum()
        out_df['close'] = self.fetch_wsd_data(','.join(out_df.wind_code.tolist()), 'close', self.lastTradeDay)
        out_df['position_num'] = 0
        out_df = self.approach_totalAmount(self.totalAmount, out_df)
        out_df['name'] = self.fetch_wsd_data(','.join(out_df.wind_code.tolist()), 'sec_name', self.lastTradeDay)
        out_df.to_excel(self.product_name + u'成分股绝对权重归一化票池.xlsx', index=False)

if __name__ == '__main__':
    shareStockTool = ShareStockTool()
    shareStockTool.gen_equal_ratio_open_stock()
    #shareStockTool.gen_absolute_weight_open_stock()
