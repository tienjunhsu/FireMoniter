#! /user/bin/env python
#encoding:utf-8
#用tornado 实现行情和websocket服务器
import tornado.httpserver
from tornado.web import Application
from tornado.websocket import WebSocketHandler


class QuotWebSocketHandler(WebSocketHandler):
    def allow_draft76(self):
        return True

    def check_origin(self, origin):
        return True

    def open(self):
        print 'new client opened'

    def on_close(self):
        print 'client closed'

    def on_message(self, message):
        print('on_message:'+message)
        self.write_message(message)


class QuotApp(Application):
    def __init__(self):
        handlers = [
            #(r'/', IndexPageHandler),
            (r'/ws', WebSocketHandler),
        ]
        super(QuotApp, self).__init__(self, handlers=handlers)


def main():
    ws_app = Application()
    server = tornado.httpserver.HTTPServer(ws_app)
    server.listen(8080)
    tornado.ioloop.IOLoop.instance().start()

if __name__ == '__main__':
    main()