#! /user/bin/env python
#encoding:utf-8
#去除重复数据
import pymongo


def main():
    client = pymongo.MongoClient('192.168.2.112',27017)
    cc=client['fire_moniter']['daily_moniter']
    time_stamps = []
    cursor = cc.find({})
    for item in cursor:
        if item['time_stamp'] in time_stamps:
             cc.remove({'_id':item['_id']})
        else:
            time_stamps.append(item['time_stamp'])


if __name__ == '__main__':
    main()