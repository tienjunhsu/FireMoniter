#! /user/bin/env python
#encoding:utf-8
import logging

import tornado.ioloop
import tornado.web
import tornado.websocket

class MainHandler(tornado.web.RequestHandler):
    def get(self):
        self.write("Hello, world00000")


class ApiHandler(tornado.web.RequestHandler):
    def get(self, *args):
        # respon = {'issuccess':'5555'}
        # respon_json = tornado.escape.json_encode(respon)
        # self.write(respon_json)
        self.write("kkkkkkkk")

    def post(self):
        pass

class QuotWebSocketHandler(tornado.websocket.WebSocketHandler):
    # def allow_draft76(self):
    #     return True
    #
    # def check_origin(self, origin):
    #     return True

    def open(self):
        self.write_message('Welcome to WebSocket')
        logging.debug('new client opened')

    def on_close(self):
        logging.debug('client closed')

    def on_message(self, message):
        logging.debug('on_message:' + message)
        self.write_message(message)

application = tornado.web.Application([
    (r"/", MainHandler),
    (r"/api", ApiHandler),
    (r'/ws', QuotWebSocketHandler),
])

if __name__ == "__main__":
    application.listen(8888,'192.168.1.100')
    tornado.ioloop.IOLoop.instance().start()
    print('hahahaha')