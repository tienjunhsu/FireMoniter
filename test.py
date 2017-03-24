#! /user/bin/env python
#encoding:utf-8
import os

import time
from WindPy import w


def callback(indata):
    print('callback....')


def main():
    w.start()
    w.wsq('000905.SH', "rt_pct_chg", func=callback)
    while True:
        time.sleep(5)


if __name__ == '__main__':
    main()