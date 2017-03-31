#! /user/bin/env python
#encoding:utf-8
import os
import pandas as pd
import pymongo
from WindPy import w

mongo_client = pymongo.MongoClient('192.168.2.112', 27017)
collection = mongo_client['fire_data']['trade_day']


def main():
    w.start()
    result = w.tdays("2017-01-01", "2017-12-31", "")
    df = pd.DataFrame({'date':result.Data[0]})
    df.date = df.date.dt.strftime('%Y-%m-%d')
    collection.insert_many(df.to_dict('records'))



if __name__ == '__main__':
    main()