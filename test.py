#! /user/bin/env python
#encoding:utf-8
import os
import threading

import time
from WindPy import w


def callback(indata):
    print('callback....')
    print(threading.currentThread())


def main():
    w.start()
    w.wsq('000905.SH,600507.SH,600612.SH,600687.SH', "rt_pct_chg", func=callback)
    print('main thread is:')
    print(threading.currentThread())
    print('_____________')
    while True:
        time.sleep(5)


if __name__ == '__main__':
    main()