#! /user/bin/env python
# encoding:utf-8
# 用tornado 实现行情和websocket服务器
import json
import logging
import os
import random
import time

import datetime
import redis

import thread
import tornado.ioloop
import tornado.web
import tornado.websocket
import tornado.options
import tornado.httpserver

strategies = ['sz50', 'hs300', 'zz500', 'strategy']
strategy_names = [u'上证50', u'沪深300', u'中证500', u'策略']
index_secs = ['000016.SH', '000300.SH', '000905.SH']

r = redis.Redis(host='192.168.2.112', port=6379, db=0)
#r = redis.Redis(host='192.168.2.112', port=6379, db=1)#db 1 for test


# r = None


def isTrading():
    # 当前是否是交易时段
    now = datetime.datetime.now().strftime('%H:%M')
    if now < '09:15':
        return False
    elif (now > '11:32') and (now < '12:58'):
        return False
    elif now > '15:02':
        return False
    else:
        return True


class QuotWebSocketHandler(tornado.websocket.WebSocketHandler):
    def allow_draft76(self):
        return True

    def check_origin(self, origin):
        return True

    def open(self):
        # self.write_message('Welcome to WebSocket')
        logging.debug('new client opened')
        self.client_closed = False
        #self.tick()

    def tick(self,name):
        #发送tick数据
        strategy_name = name
        def run(*args):
            while True and not self.client_closed:
                # now = datetime.datetime.now().strftime('%H:%M:%S')
                # self.write_message(now)
                time.sleep(2)
                #print('close code is ...:'+str(self.close_code))
                if isTrading():
                    data = r.mget('time_stamp', 'sz50', 'hs300', 'zz500', strategy_name, 'ih_spread', 'if_spread',
                                  'ic_spread')
                    data[0] = 1000 * float(data[0])
                    for i in range(1, len(data)):
                        data[i] = float(data[i])
                    try:
                        self.write_message(json.dumps(
                                {'time_stamp': data[0], 'sz50': data[1], 'hs300': data[2], 'zz500': data[3],
                                 strategy_name: data[4], 'ih_spread': data[5], 'if_spread': data[6], 'ic_spread': data[7]}))
                    except:
                         logging.error("Error sending message")
                    # self.write_message(json.dumps({'time_stamp': 1000*time.mktime(datetime.datetime.now().timetuple()), 'sz50': random.random(), 'hs300': random.random(), 'zz500': random.random(), 'strategy': random.random()}))

        thread.start_new_thread(run, ())

    def on_close(self):
        logging.debug('client closed')
        print('client closed')
        self.client_closed = True

    def on_message(self, message):
        logging.debug('on_message:' + message)
        self.tick(message)


class ApiHandler(tornado.web.RequestHandler):
    def get(self, *args):
        # respon = {'issuccess':'5555'}
        # respon_json = tornado.escape.json_encode(respon)
        # self.write(respon_json)
        self.write("Hello, world00000")

    def post(self):
        pass


ws_app = tornado.web.Application([
    (r"/", ApiHandler),
    (r"/api", ApiHandler),
    (r'/ws', QuotWebSocketHandler),
])


def main():
    tornado.options.parse_command_line()
    server = tornado.httpserver.HTTPServer(ws_app)
    print('start....01')
    server.listen(5555, address='192.168.2.185')
    # server.listen(5555)
    print('start....02')
    tornado.ioloop.IOLoop.instance().start()
    print('start....03')
    logging.debug('start.....')


if __name__ == '__main__':
    main()
