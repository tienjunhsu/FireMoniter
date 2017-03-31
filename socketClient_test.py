#! /user/bin/env python
#encoding:utf-8
import socket
import time

import datetime

ip_port = ('192.168.2.112',8009)
sk = socket.socket()
sk.connect(ip_port)
flag = True
count = 0
try:
    while flag:
        data = sk.recv(1024)
        print 'receive:',data
        now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        sk.sendall(now)
        count += 1
        if count >=20:
            sk.sendall('exit')
        time.sleep(2)
except (KeyboardInterrupt, SystemExit):
    flag = False

sk.close()