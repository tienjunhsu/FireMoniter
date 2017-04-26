#! /user/bin/env python
#encoding:utf-8
#调整子策略权重
import pymongo
import pandas as pd

output_mongo_client = pymongo.MongoClient('192.168.2.181', 27017)
output_db = output_mongo_client['fire_trade']
need_remove_codes = ['002571']
total_weight = 0
output_date = output_db['strategy_output_date'].find().sort('date', -1).limit(1)[0]['date']
cursor = output_db['strategy_list'].find({'date': output_date}, {'_id': 0, 'strategy_list': 1})
strategy_list = list(cursor)[0]['strategy_list']
strategy_list = strategy_list.split(',')
count = 0
for strategy in strategy_list:
    cursor = output_db['strategy_list_output01'].find({'date': output_date, 'strategy': strategy},
                                                          {'_id': 0, 'code': 1, 'weight': 1})
    df = pd.DataFrame(list(cursor))
    df = df[~df.code.isin(need_remove_codes)]
    count += 1
    print(count)
    if len(df) > 0:
        total_weight += df.weight.sum()

print(total_weight)
# for strategy in strategy_list:
#     cursor = output_db['strategy_list_output01'].find({'date': output_date, 'strategy': strategy},
#                                                           {'_id': 0, 'code': 1, 'weight': 1, 'wind_code': 1,'strategy':1,'date':1})
#
#     df = pd.DataFrame(list(cursor))
#     df = df[~df.code.isin(need_remove_codes)]
#     df.loc[:,'weight'] = df.weight/ total_weight
#     output_db['strategy_list_output01'].remove({'date': output_date, 'strategy': strategy})
#     output_db['strategy_list_output01'].insert_many(df.to_dict('records'))