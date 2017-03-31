#! /user/bin/env python
#encoding:utf-8
import redis
import time

r = redis.Redis(host='192.168.2.112', port=6379, db=0)

while True:
    data = r.mget('time_stamp01', 'sz5001', 'hs30001', 'zz50001', 'strategy01', 'ih_spread01', 'if_spread01','ic_spread01')
    print(data)
    time.sleep(2)
