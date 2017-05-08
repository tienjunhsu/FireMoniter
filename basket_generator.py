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

basket_unit = 1500000.0 #一个交易篮子150万


class BasketGenerator(object):
    def __init__(self):
        self.w = w #万得接口
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
        self.position_files = {u'一号产品': u'0505中金1号收盘持仓统计.xlsx01',
                               u'术源九州': u'0505九州持仓统计.xls01', u'自强一号': u'自强.xls01',
                               u'兴鑫': u'兴鑫现货0504.xls', u'华泰吴雪敏': u'吴雪敏.xls01'}
        self.hedge_position_files = {u'一号产品':None,u'兴鑫':u'兴鑫期货0504.xls'}
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
        self.w.start() #万得接口初始化
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
            now_ic_hand, now_if_hand, now_ih_hand = self.parse_hedge_position(product)
            ic_diff_hand = plan_ic_hand - now_ic_hand
            if_diff_hand = plan_if_hand - now_if_hand
            ih_diff_hand = plan_ih_hand - now_ih_hand
            if plan_df is not None:
                # plan_df = plan_df.groupby('code').agg(
                #         {'name': 'first', 'wind_code': 'first', 'close': 'first', 'weight': sum, 'position_num': sum})
                plan_df = plan_df.groupby('code').agg(
                        {'wind_code': 'first', 'close': 'first', 'weight': sum, 'position_num': sum})
                plan_df = plan_df.reset_index()
                now_df = now_df[['code', 'position_num']]
                now_df.columns = ['code', 'old_position_num']
                plan_df = plan_df.set_index('code')
                now_df = now_df.set_index('code')
                plan_df = plan_df.join(now_df, how='outer')
                plan_df = plan_df.fillna(0)
                plan_df.loc[:, 'position_num'] = plan_df['position_num'] - plan_df['old_position_num']
                plan_df = plan_df.reset_index()
                plan_df01 = plan_df[~plan_df.code.str.startswith('60')]
                plan_df = plan_df[plan_df.code.str.startswith('60')]
                plan_df01.loc[:, 'wind_code'] = plan_df01.code +'.SZ'
                plan_df.loc[:, 'wind_code'] = plan_df.code +'.SH'
                plan_df = plan_df.append(plan_df01)
                secs = ','.join(plan_df.wind_code.tolist())
                plan_df.loc[:, 'close'] = self.fetch_wsd_data(secs,'close')
                plan_df['position_value'] = plan_df.position_num * plan_df.close
                plan_df['name'] = self.fetch_wsd_data(secs,'sec_name')
                # plan_df.to_excel(name + u'加减仓.xlsx', index=False)
            else:
                plan_df = now_df[['code', 'name', 'position_num', 'position_value']]
                plan_df[:, 'position_num'] = -plan_df.position_num
                plan_df[:, 'position_value'] = -plan_df.position_value
            self.format_basket(name, product['clientType'], plan_df,ic_diff_hand,if_diff_hand,ih_diff_hand)

    def fetch_wsd_data(self,secs,ind_name):
        #通过万得客户端获取数据
        result = self.w.wsd(secs,ind_name, self.today, self.today, "Fill=Previous")
        return result.Data[0]

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

    def format_basket(self, product, clientType, df, ic_diff_hand=0, if_diff_hand=0, ih_diff_hand=0):
        # 将票池转化为对应的格式
        if clientType == 'ims':
            df01 = df[df['code'].str.startswith('60')]
            df01['market_type'] = 0
            df = df[~df['code'].str.startswith('60')]
            df['market_type'] = 1
            df = df.append(df01)
            filename = product + u'加减仓.xlsx'
            columns = ['code', 'market_type', 'position_num']
            header = [u'合约代码', u'市场', u'数量/权重']
        elif clientType == 'pb' or clientType == 'tdx':
            df01 = df[df['code'].str.startswith('60')]
            df01['market_type'] = 1
            df = df[~df['code'].str.startswith('60')]
            df['market_type'] = 2
            df = df.append(df01)
            filename = product + u'加减仓.xlsx'
            columns = ['code', 'name', 'market_type', 'position_num']
            header = [u'证券代码', u'证券名称', u'交易市场', u'数量/权重']
        else:
            df['d_weight'] = 1
            df['d_dr'] = 1
            filename = product + u'加减仓.xlsx'
            columns = ['code', 'name', 'position_num', 'd_weight', 'd_dr']
            header = [u'证券代码', u'证券名称', u'目标数量', u'目标权重', u'方向']
        reduce_df = df[df.position_num < 0]
        df = df[df.position_num >= 0]
        reduce_df.loc[:, 'position_num'] = - reduce_df.position_num
        reduce_df.loc[:, 'position_value'] = - reduce_df.position_value
        if ic_diff_hand > 0 or if_diff_hand > 0 or ih_diff_hand > 0:
            self.save_hedge_to_excel(filename, df, reduce_df, columns, header, ic_diff_hand, if_diff_hand, ih_diff_hand)
            #self.save_to_excel(filename, df, reduce_df, columns, header)
        else:
            self.save_to_excel(filename, df, reduce_df, columns, header)

    def save_hedge_to_excel(self,filename, add_df, reduce_df, columns, header, ic_diff_hand,if_diff_hand,ih_diff_hand):
        #将篮子保存为excel文件
        #其中filename 是文件名,add_df是需要加仓的DataFrame,reduce_df是需要减仓的DataFrame
        #columns是一个list，是需要输出到excel里面的列
        #header是columns输出时对应的头名字，类型为list
        #适用于期货端有变动的情况，需要将期货端对应的股票分离出来
        prefixes = ['IC','IF','IH']
        #hedge_basket_num_dict = {'IC':0,'IF':0,'IH':0}
        writer = ExcelWriter(filename)
        add_df.to_excel(writer, u'加仓汇总', columns=columns, header=header, index=False)
        reduce_df.to_excel(writer, u'减仓汇总', columns=columns, header=header, index=False)
        columns_str = ','.join(columns)
        print(ic_diff_hand,if_diff_hand,ih_diff_hand)
        for i,diff_hand in enumerate([ic_diff_hand,if_diff_hand,ih_diff_hand]):
            prefix = prefixes[i]
            #hedge_basket_num_dict[prefix] = int(math.ceil(abs(diff_hand)/2))
            hedge_basket_num = int(math.ceil(abs(diff_hand)/2))
            column_name = prefix+'_position_num'
            n_column = columns_str.replace('position_num',column_name)
            n_column = n_column.split(',')
            hedge_value = self.hedge_close[prefix]*self.hedge_multiplier[prefix]
            if diff_hand > 0:
                if add_df.position_num.sum() <= 0:
                    #剩余股票已经分完了，跳过下面的步奏
                    continue
                sheet_name = prefix+u'加仓第{0}篮'
                if hedge_basket_num < 2:
                    #只有1篮子
                    hedge_amount = hedge_value * diff_hand
                    add_df[column_name] = self.approach_hedgeAmount(hedge_amount,add_df)
                    add_df.loc[:,'position_num'] = add_df.position_num - add_df[column_name]
                    add_df.to_excel(writer,sheet_name.format(str(hedge_basket_num)), columns=n_column, header=header, index=False)
                else:
                    for i in range(hedge_basket_num - 1):
                        if add_df.position_num.sum() > 0:
                            #还有剩余股票的时候才进行分篮,很粗略的一个计算
                            hedge_amount = hedge_value * 2
                            add_df.loc[:,column_name] = self.approach_hedgeAmount(hedge_amount,add_df)
                            add_df.loc[:,'position_num'] = add_df.position_num - add_df[column_name]
                            add_df.to_excel(writer,sheet_name.format(str(i+1)), columns=n_column, header=header, index=False)
                    if add_df.position_num.sum() > 0:
                        hedge_amount = hedge_value * (diff_hand-2*(hedge_basket_num - 1))
                        add_df.loc[:,column_name] = self.approach_hedgeAmount(hedge_amount,add_df)
                        add_df.loc[:,'position_num'] = add_df.position_num - add_df[column_name]
                        add_df.to_excel(writer,sheet_name.format(str(hedge_basket_num)), columns=n_column, header=header, index=False)

            elif diff_hand < 0:
                if reduce_df.position_num.sum() <= 0:
                    #剩余股票已经分完，直接跳过下面的步奏
                    continue
                sheet_name = prefix+u'减仓第{0}篮'
                diff_hand = -diff_hand
                if hedge_basket_num < 2:
                    #只有1篮子
                    hedge_amount = hedge_value * diff_hand
                    reduce_df[column_name] = self.approach_hedgeAmount(hedge_amount,reduce_df)
                    reduce_df.loc[:,'position_num'] = reduce_df.position_num - reduce_df[column_name]
                    reduce_df.to_excel(writer,sheet_name.format(str(hedge_basket_num)), columns=n_column, header=header, index=False)
                else:
                    for i in range(hedge_basket_num - 1):
                        if reduce_df.position_num.sum() > 0:
                            hedge_amount = hedge_value * 2
                            reduce_df.loc[:,column_name] = self.approach_hedgeAmount(hedge_amount,reduce_df)
                            reduce_df.loc[:,'position_num'] = reduce_df.position_num - reduce_df[column_name]
                            reduce_df.to_excel(writer,sheet_name.format(str(i+1)), columns=n_column, header=header, index=False)
                    if reduce_df.position_num.sum() > 0:
                        hedge_amount = hedge_value * (diff_hand-2*(hedge_basket_num - 1))
                        reduce_df.loc[:,column_name] = self.approach_hedgeAmount(hedge_amount,reduce_df)
                        reduce_df.loc[:,'position_num'] = reduce_df.position_num - reduce_df[column_name]
                        reduce_df.to_excel(writer,sheet_name.format(str(hedge_basket_num)), columns=n_column, header=header, index=False)
        #剩余部分分为敞口的
        n_columns = columns_str.replace('position_num','p_position_num')
        n_columns = n_columns.split(',')
        l_columns = columns_str.replace('position_num','l_position_num')
        l_columns = l_columns.split(',')
        add_df,add_basket_num = self.calculate_basket_num(add_df)
        if add_basket_num > 1:
            if add_basket_num > 2:
                sheet_name = u'敞口加仓第1-'+str(add_basket_num - 1)+u'篮'
            else:
                sheet_name = u'敞口加仓第'+str(add_basket_num - 1)+u'篮'
            add_df.to_excel(writer, sheet_name,columns=n_columns, header=header, index=False)
        if (add_df.position_num * add_df.close).sum() > basket_unit*2:
            add_df['l_position_num'] = 100*((add_df.position_num//2)//100)
            add_df.loc[:,'position_num'] = add_df.position_num - add_df.l_position_num
            add_df.to_excel(writer, u'敞口加仓第'+str(add_basket_num)+u'篮', columns=l_columns, header=header, index=False)
            add_df.to_excel(writer, u'敞口加仓第'+str(add_basket_num + 1)+u'篮', columns=columns, header=header, index=False)
        else:
            add_df.to_excel(writer, u'敞口加仓第'+str(add_basket_num)+u'篮', columns=columns, header=header, index=False)
        reduce_df,reduce_basket_num = self.calculate_basket_num(reduce_df)
        if reduce_basket_num > 1:
            if reduce_basket_num > 2:
                sheet_name = u'敞口减仓第1-'+str(reduce_basket_num - 1)+u'篮'
            else:
                sheet_name = u'敞口减仓第'+str(reduce_basket_num - 1)+u'篮'
            reduce_df.to_excel(writer, sheet_name,columns = n_columns,header=header, index=False)
        if (reduce_df.position_num * reduce_df.close).sum() > basket_unit*2:
            reduce_df['l_position_num'] = 100*((reduce_df.position_num//2)//100)
            reduce_df.loc[:,'position_num'] = reduce_df.position_num - reduce_df.l_position_num
            reduce_df.to_excel(writer, u'敞口减仓第'+str(reduce_basket_num)+u'篮', columns=l_columns, header=header, index=False)
            reduce_df.to_excel(writer, u'敞口减仓第'+str(reduce_basket_num + 1)+u'篮', columns=columns, header=header, index=False)
        else:
            reduce_df.to_excel(writer, u'敞口减仓第'+str(reduce_basket_num)+u'篮', columns=columns, header=header, index=False)
        #粗略的进行一下有效性检查
        if len(add_df[add_df.position_num < 0]) > 0:
            raise Exception('Calculte basket Wrong!add_df position_num have negative number')
        if len(reduce_df[reduce_df.position_num < 0]) > 0:
            raise Exception('Calculte basket Wrong!reduce_df position_num have negative number')
        writer.save()

    def approach_hedgeAmount(self, hedgeAmount, df):
        #用逼近的方法获取对冲端的篮子股票数量
        total_value = (df.position_num * df.close).sum() #注意position_num的数值可能是分篮后剩下的，所以市值需要重新计算
        upper = df.position_num  #上确界
        if hedgeAmount >= total_value:
            #仓位已经不够分了，直接返回剩余的作为一篮子
            return df.position_num
        df.loc[:,'weight'] = (df.position_num * df.close)/total_value
        df['hedge_num'] = 100 * ((hedgeAmount * df.weight) // (100 * df.close))
        df.loc[:,'hedge_num'] = df.hedge_num.clip_upper(upper)
        df.loc[:,'position_num'] = df.position_num - df.hedge_num
        df.loc[:,'weight'] = (df.position_num * df.close)/(df.position_num * df.close).sum()
        real_total = (df.hedge_num * df.close).sum()
        delta = 1000.0
        loop = 0
        while real_total < hedgeAmount:
            loop += 1
            new_totalAmount = max(hedgeAmount - real_total, delta)
            remainder = 100 * ((new_totalAmount * df.weight) // (100 * df.close))
            if len(remainder[remainder > 0]) < 1:
                df.to_csv('remain.csv',encoding='utf-8',index=False)
                raise Exception('break')
            # while len(remainder[remainder > 0]) < 1:
            #     new_totalAmount += delta
            #     print('inner while:'+str(new_totalAmount))
            #     remainder = 100 * ((new_totalAmount * df.weight) // (100 * df.close))
            df.loc[:,'hedge_num'] += remainder
            if loop % 40 == 0:
                print((df.hedge_num * df.close).sum())
            df.loc[:,'hedge_num'] = df.hedge_num.clip_upper(upper)
            df.loc[:,'position_num'] = upper - df.hedge_num
            df.loc[:,'weight'] = (df.position_num * df.close)/(df.position_num * df.close).sum()
            real_total = (df.hedge_num * df.close).sum()
            if loop % 20 == 0:
                print(df.weight.sum())
                print(loop,hedgeAmount - real_total,new_totalAmount)
        print(loop,hedgeAmount - real_total,new_totalAmount)
        return df.hedge_num

    def save_to_excel(self,filename, add_df, reduce_df, columns, header):
        #将篮子保存为excel文件
        #其中filename 是文件名,add_df是需要加仓的DataFrame,reduce_df是需要减仓的DataFrame
        #columns是一个list，是需要输出到excel里面的列
        #header是columns输出时对应的头名字，类型为list
        writer = ExcelWriter(filename)
        add_df.to_excel(writer, u'加仓汇总', columns=columns, header=header, index=False)
        reduce_df.to_excel(writer, u'减仓汇总', columns=columns, header=header, index=False)
        n_columns = ','.join(columns)
        n_columns = n_columns.replace('position_num','p_position_num')
        n_columns = n_columns.split(',')
        l_columns = ','.join(columns)
        l_columns = l_columns.replace('position_num','l_position_num')
        l_columns = l_columns.split(',') #最后一篮子比较大的时候，再次平分，这是倒数第二篮
        add_df,add_basket_num = self.calculate_basket_num(add_df)
        if add_basket_num > 1:
            if add_basket_num > 2:
                sheet_name = u'加仓第1-'+str(add_basket_num - 1)+u'篮'
            else:
                sheet_name = u'加仓第'+str(add_basket_num - 1)+u'篮'
            add_df.to_excel(writer, sheet_name,columns=n_columns, header=header, index=False)
        if (add_df.position_num * add_df.close).sum() > basket_unit*2:
            add_df['l_position_num'] = 100*((add_df.position_num//2)//100)
            add_df.loc[:,'position_num'] = add_df.position_num - add_df.l_position_num
            add_df.to_excel(writer, u'加仓第'+str(add_basket_num)+u'篮', columns=l_columns, header=header, index=False)
            add_df.to_excel(writer, u'加仓第'+str(add_basket_num + 1)+u'篮', columns=columns, header=header, index=False)
        else:
            add_df.to_excel(writer, u'加仓第'+str(add_basket_num)+u'篮', columns=columns, header=header, index=False)
        reduce_df,reduce_basket_num = self.calculate_basket_num(reduce_df)
        if reduce_basket_num > 1:
            if reduce_basket_num > 2:
                sheet_name = u'减仓第1-'+str(reduce_basket_num - 1)+u'篮'
            else:
                sheet_name = u'减仓第'+str(reduce_basket_num - 1)+u'篮'
            reduce_df.to_excel(writer, sheet_name,columns = n_columns,header=header, index=False)
        if (reduce_df.position_num * reduce_df.close).sum() > basket_unit*2:
            reduce_df['l_position_num'] = 100*((reduce_df.position_num//2)//100)
            reduce_df.loc[:,'position_num'] = reduce_df.position_num - reduce_df.l_position_num
            reduce_df.to_excel(writer, u'减仓第'+str(reduce_basket_num)+u'篮', columns=l_columns, header=header, index=False)
            reduce_df.to_excel(writer, u'减仓第'+str(reduce_basket_num + 1)+u'篮', columns=columns, header=header, index=False)
        else:
            reduce_df.to_excel(writer, u'减仓第'+str(reduce_basket_num)+u'篮', columns=columns, header=header, index=False)
        writer.save()

    def calculate_basket_num(self,df):
        #生成交易篮子
        if len(df) > 0:
            basket_num = df.position_value.sum()/basket_unit
            basket_num = int(math.ceil(basket_num)) #向上取整
            if basket_num < 3:
                df['p_position_num'] = 100*((df.position_num//basket_num)//100) #前面的n-1篮子，平分
            else:
                #篮子比较多，最后一个有可能会比较大，需要再次平分，所以前面n-1个先略微设置得大一些
                df['p_position_num'] = 100*((df.position_num//(basket_num - 1))//100)
            #余下的一个篮子
            df.loc[:,'position_num'] = df.position_num - (basket_num - 1)*df.p_position_num
            if len(df[df.position_num < 0]) > 0:
                #有效性检查，如果发现余下的一个篮子里面出现了负数，说明计算出现错误
                raise Exception("Wrong!Have negative number in df's position_num")
            return df,basket_num
        else:
            return df,1

    def fetch_close_price(self):
        # 获取股票池的名字和当天收盘价
        index_secs = self.__get_future_contracts()
        index_close = self.w.wsd(index_secs,'close',self.today, self.today,'Fill=Previous').Data[0]
        self.hedge_close = dict(zip(['IC','IF','IH'],index_close))
        if self.bull_df is not None:
            self.bull_df['close'] = \
                self.w.wsd(','.join(self.bull_df['wind_code'].tolist()), "close", self.today, self.today,
                      "Fill=Previous").Data[
                    0]
            # self.bull_df['name'] = \
            #     self.w.wsd(','.join(self.bull_df['wind_code'].tolist()), "sec_name", self.today, self.today,
            #           "Fill=Previous").Data[0]
        if self.ic_df is not None:
            self.ic_df['close'] = \
                self.w.wsd(','.join(self.ic_df['wind_code'].tolist()), "close", self.today, self.today,
                      "Fill=Previous").Data[0]
            # self.ic_df['name'] = \
            #     self.w.wsd(','.join(self.ic_df['wind_code'].tolist()), "sec_name", self.today, self.today,
            #           "Fill=Previous").Data[
            #         0]
        if self.if_df is not None:
            self.if_df['close'] = \
                self.w.wsd(','.join(self.if_df['wind_code'].tolist()), "close", self.today, self.today,
                      "Fill=Previous").Data[0]
            # self.if_df['name'] = \
            #     self.w.wsd(','.join(self.if_df['wind_code'].tolist()), "sec_name", self.today, self.today,
            #           "Fill=Previous").Data[
            #         0]
        if self.ih_df is not None:
            self.ih_df['close'] = \
                self.w.wsd(','.join(self.ih_df['wind_code'].tolist()), "close", self.today, self.today,
                      "Fill=Previous").Data[0]
            # self.ih_df['name'] = \
            #     self.w.wsd(','.join(self.ih_df['wind_code'].tolist()), "sec_name", self.today, self.today,
            #           "Fill=Previous").Data[
            #         0]

    def fetch_temp_risk_ticks(self, total_df):
        #获取临时的风险票
        #参数total_df是所有子票池合起来的DataFrame
        secs = ','.join(total_df['wind_code'].tolist())
        total_df['open'] = self.w.wsd(secs,'open', self.today, self.today,"Fill=Previous").Data[0]
        total_df['close'] = self.w.wsd(secs,'close', self.today, self.today,"Fill=Previous").Data[0]
        total_df['pct_chg'] = self.w.wsd(secs,'pct_chg', self.today, self.today,"Fill=Previous").Data[0]
        total_df = total_df[(total_df.open == total_df.close) & (total_df.pct_chg < -9.97)]
        return total_df.code.tolist()

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
        self.out_strategy_list += self.fetch_temp_risk_ticks(df)
        print('out strategy code list.......')
        print(self.out_strategy_list)
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

    def parse_hedge_position(self,product):
        #获取当前持有的期货手数
        #返回的值按顺序分别是IC持仓手数、IF持仓手数、IH持仓手数
        ic_hand = if_hand = ih_hand = 0
        product_name = product['name']
        if product_name in self.hedge_position_files.keys():
            filename = self.hedge_position_files[product_name]
            if filename is not None:
                if product['clientType'] == 'ims':
                    #一号产品
                    ic_hand, if_hand, ih_hand = self.parse_zj_hedge_excel_position(filename)
                elif product['clientType'] == 'pb':
                    #兴鑫
                    ic_hand, if_hand, ih_hand = self.parse_pb_hedge_excel_position(filename)
        return ic_hand,if_hand,ih_hand

    def parse_pb_hedge_excel_position(self,filename):
        #获取期货持仓，用于兴鑫
        ic_hand = if_hand = ih_hand = 0
        df = pd.read_excel(filename)
        df = df[[u'合约代码', u'买卖', u'总持仓']]
        df.columns = ['code','side','position']
        if len(df) < 1:
            return ic_hand, if_hand,ih_hand
        df = df[df.side == u'卖'] #只需要卖出的
        if len(df) < 1:
            return ic_hand, if_hand,ih_hand
        df =df[df.code.str.startswith('IC') | df.code.str.startswith('IF') | df.code.str.startswith('IH')]
        if len(df) < 1:
            return ic_hand, if_hand,ih_hand
        df = df[df.position > 0]
        if len(df) < 1:
            return ic_hand, if_hand,ih_hand
        #下面的代码是安全的，即使没有记录了，返回的是0，并不会返回错误
        ic_hand = df[df.code.str.startswith('IC')].position.sum()
        if_hand = df[df.code.str.startswith('IF')].position.sum()
        ih_hand = df[df.code.str.startswith('IH')].position.sum()
        return ic_hand, if_hand,ih_hand

    def parse_zj_hedge_excel_position(self,filename):
        #获取期货持仓，用于一号产品，中金客户端
        ic_hand = if_hand = ih_hand = 0
        df = pd.read_excel(filename)
        df = df[[u'合约', u'买卖', u'总卖持']]
        df.columns = ['code','side','position']
        df = df[df.side == u'卖出'] #只需要卖出的
        df =df[df.code.str.startswith('IC') | df.code.str.startswith('IF') | df.code.str.startswith('IH')]
        ic_hand = df[df.code.str.startswith('IC')].position.sum()
        if_hand = df[df.code.str.startswith('IF')].position.sum()
        ih_hand = df[df.code.str.startswith('IH')].position.sum()
        return ic_hand, if_hand,ih_hand

    def parse_position(self, product, filename):
        # 获取现在的持仓
        if product['clientType'] == 'pb':
            df = self.parse_hs_excel_position(filename)
        elif product['clientType'] == 'ims':
            df = self.parse_ims_excel_position(filename)
        elif product['clientType'] == 'xt':
            df = self.parse_xt_excel_position(filename)
        elif product['clientType'] == 'tdx':
            df = self.parse_tdx_excel_position(filename)
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

    def parse_tdx_excel_position(self, filename):
        #通达信
        df = pd.read_csv(filename,sep='\t',skiprows=3,encoding='gbk')
        df = df[[u'="证券代码"',u'="证券名称"',u'="证券数量"',u'="最新市值"']]
        df.columns = ['code','name','position_num','position_value']
        df.loc[:,'code'] = df.code.str.slice(2,8)
        df.loc[:,'name']=df.name.str.slice(2,-1)
        df=df[df.code.str.startswith('30')|df.code.str.startswith('60')|df.code.str.startswith('00')]
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
