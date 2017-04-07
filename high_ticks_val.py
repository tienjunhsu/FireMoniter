#! /user/bin/env python
#encoding:utf-8
import pymongo
import pandas as pd
import requests
from WindPy import w

mongo_client = pymongo.MongoClient('192.168.2.181', 27017)

date = mongo_client['fire_trade']['strategy_output_date'].find().sort('date', -1).limit(1)[0]['date']
cursor = mongo_client['fire_trade']['strategy_output01'].find({'date': date},projection={'_id': False})
df = pd.DataFrame(list(cursor))
print(len(df))
# w.start()
# #result = w.wset("tradesuspend","startdate=2017-04-05;enddate=2017-04-05")
# result = w.wset("sectorconstituent","date=2017-04-04;sectorid=1000006526000000")
# data = {'date': result.Data[0], 'code': result.Data[1], 'name': result.Data[2]}
# halt_df = pd.DataFrame(data)
# halt_df.code = halt_df.code.str.slice(0, 6)
# halt_df = halt_df[halt_df.code.isin(df.code)]
# print(len(halt_df))
filename = u'C:/Users/asus/Desktop/xx3.6/test.xlsx'
h_df = pd.read_excel(filename,converters={'code':str})
h_df = h_df[h_df.code.isin(df.code)]
net_update_url = 'http://192.168.2.112:8000/risk/highriskticks/2017-04-02/'
high_risk_list = requests.get(net_update_url).text.split(',')
h_df = h_df[~h_df.code.isin(high_risk_list)]
h_df= h_df.reset_index()
print(h_df)


