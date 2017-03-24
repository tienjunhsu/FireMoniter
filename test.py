#! /user/bin/env python
#encoding:utf-8
import os

import pandas as pd
import pymongo


def main():
    mongo_client = pymongo.MongoClient('192.168.2.112', 27017)
    collection = mongo_client['fire_moniter']['daily_moniter']
    cursor = collection.find({'date':'2017-03-23'},{'_id':False})
    df = pd.DataFrame(list(cursor))
    df.to_csv('data.csv')


if __name__ == '__main__':
    main()