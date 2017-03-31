#! /user/bin/env python
#encoding:utf-8
import SocketServer


class MyHandler(SocketServer.BaseRequestHandler):
    def handle(self):
        conn = self.request
        conn.sendall('我是多线程')
        flag = True
        try:
            while flag:
                data = conn.recv(1024)
                print('data:'+data)
                if data == 'exit':
                    Flag = False
                elif data == '0':
                    conn.sendall('您输入的是0')
                else:
                    resp = 'your data:'+data
                    conn.sendall(resp)
        except (KeyboardInterrupt, SystemExit):
            flag = False
            server.shutdown()

server = SocketServer.ThreadingTCPServer(('127.0.0.1',8009),MyHandler)
if __name__ == '__main__':
    server.serve_forever()

