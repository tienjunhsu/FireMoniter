#! /user/bin/env python
#encoding:utf-8
from apscheduler.schedulers.blocking import BlockingScheduler
import time
import tushare as ts


def main():
    df = ts.get_index()
    df = df[df.code.isin(['000016','000300','000905'])][['code','name','change']]
    print(df)


if __name__ == '__main__':
    scheduler = BlockingScheduler()
    scheduler.add_job(main, "cron", second="*/2")
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()