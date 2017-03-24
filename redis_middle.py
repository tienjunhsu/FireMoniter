#! /user/bin/env python
#encoding:utf-8
import redis


class Conn_db():
    def __init__(self):
        self.conn=redis.StrictRedis(host='127.0.0.1',port=6379,db=0)