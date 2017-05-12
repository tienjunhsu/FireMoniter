#! /user/bin/env python
#encoding:utf-8
from __future__ import division
from WindPy import w

import datetime
import pymongo
import pandas as pd
from pandas import ExcelWriter

output_mongo_client = pymongo.MongoClient('192.168.2.181', 27017)
output_db = output_mongo_client['fire_trade']

monitor_mongo_client = pymongo.MongoClient('192.168.2.112', 27017)
moniter_db = monitor_mongo_client['fire_moniter']

today = datetime.date.today().strftime('%Y-%m-%d')
#today = '2017-05-09'
output_date = output_db['strategy_output_date'].find().sort('date', -1).limit(1)[0]['date']
cc = moniter_db['daily_strategy_list']
record = cc.find_one({'date': today}, {'_id': 0})
strategy_list = record['strategy_list']
bull_reduce = record['bull_reduce']

bull_list = list(set(strategy_list).difference(set(bull_reduce)))
cursor = output_db['strategy_list_output01'].find({'date': output_date, 'strategy': {'$in': bull_list}},
                                                          {'_id': 0, 'code': 1, 'weight': 1, 'wind_code': 1})
df = pd.DataFrame(list(cursor))
df = df.groupby(['code', 'wind_code']).agg({'weight': sum})
df = df.reset_index()
df = df.sort_values(by='weight',ascending=False)
df = df[:50]
df.loc[:, 'weight'] = 1.0*df['weight'] / df.weight.sum()
w.start()
df['close'] = w.wsd(','.join(df['wind_code'].tolist()), "close", today, today, "Fill=Previous").Data[0]
df['position_num'] = 0
totalAmount = 0.0
delta = 1000.0  # 最小的迭代步长
totalAmount = totalAmount * df.weight.sum()  # 总金额根据权重进行调整
df.position_num = 100 * ((totalAmount * df.weight) // (100 * df.close))
real_total = (df.position_num * df.close).sum()
new_totalAmount = totalAmount
while real_total < totalAmount:
    new_totalAmount += min(totalAmount - real_total, delta)
    df.position_num = 100 * ((new_totalAmount * df.weight) // (100 * df.close))
    real_total = (df.position_num * df.close).sum()
df.to_excel(u'莎莎计划持仓.xlsx', index=False)
filename = u'0511莎莎现货持仓.xls'
now_df = pd.read_csv(filename,sep='\t',encoding='gbk',converters={u'证券代码':str})
now_df = now_df[[u'证券代码',u'证券名称',u'股票余额',u'市值']]
now_df.columns = ['code','name','position_num','position_value']
#now_df.loc[:,'code'] = now_df.code.str.slice(2,8)
#now_df.loc[:,'name']=now_df.name.str.slice(2,-1)
now_df=now_df[now_df.code.str.startswith('30')|now_df.code.str.startswith('60')|now_df.code.str.startswith('00')]
now_df = now_df[now_df.position_num > 0]
plan_df = df
plan_df = plan_df.set_index('code')
now_df = now_df[['code', 'position_num']]
now_df.columns = ['code', 'old_position_num']
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
plan_df.loc[:, 'close'] = w.wsd(secs, "close", today, today, "Fill=Previous").Data[0]
plan_df['position_value'] = plan_df.position_num * plan_df.close
plan_df['name'] = w.wsd(secs, "sec_name", today, today, "Fill=Previous").Data[0]
plan_df = plan_df[['code','name','position_num','position_value']]
reduce_df = plan_df[plan_df.position_num < 0]
add_df = plan_df[plan_df.position_num >= 0]
reduce_df.loc[:, 'position_num'] = - reduce_df.position_num
reduce_df.loc[:, 'position_value'] = - reduce_df.position_value
writer = ExcelWriter(u'莎莎加减仓.xlsx')
columns = ['code','name','position_num']
header = [u'证券代码', u'证券名称', u'目标数量']
add_df.to_excel(writer, u'加仓汇总', columns=columns, header=header, index=False)
reduce_df.to_excel(writer, u'减仓汇总', columns=columns, header=header, index=False)
writer.save()

