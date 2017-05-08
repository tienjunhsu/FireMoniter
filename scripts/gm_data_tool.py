#! /user/bin/env python
# encoding:utf-8
# 处理掘金下载下来的数据，用于生成高级别K线等

import pandas as pd
from pandas.tseries.offsets import BusinessDay
import datetime


def load_gm_csv(filename):
    # 导入gm下载保存的csv数据,返回DataFrame
    df = pd.read_csv(filename, parse_dates=['strtime'])
    return df


def gen_trade_date(df):
    # 根据strtime生成交易日
    df['date'] = df.strtime.dt.date
    df['time'] = df.strtime.dt.time
    close_time = datetime.time(15)  # 15：00收盘
    # 夜盘的算到下个交易日里面去
    # 如果下一个BusinessDay不交易，处于假期内，那么当天的夜盘是不会开盘的，所以不用考虑交易所的假期，直接用BusinessDay来
    # 把周末去掉就可以了
    df['delta'] = (df.time > close_time).astype(int)
    delta = BusinessDay()
    df.loc[:, 'date'] = df.date + df.delta * delta
    df.drop('delta', axis=1, inplace=True)
    return df


def reSampleDailyKData(kdata, periods):
    '''合成长周期K线，所有低周期K线等距离合成,要合成常用的（只有日内合成），需要先用交易日group'''
    # 由于交易只出现在某些时间段里面，并不是连续的，直接resample会把中间的空白时间算进去。所以构造一个连续的时间序列
    # 作为index，然后用这个虚拟的index来resample
    # 周期如5T,15T,30T,1H,4H分别代表5分、15分、30分钟和1小时、4小时
    conversion = {'strtime': 'first', 'date': 'first', 'open': 'first', 'high': 'max', 'low': 'min', 'close': 'last',
                  'volume': 'sum', 'amount': 'sum', 'utc_time': 'first', 'symbol': 'first', 'time': 'first'}
    # 这种情况下实际上tradingdate已经没用了，上面随便取了第一个而已
    index = pd.date_range('20160101', periods=len(kdata), freq='T')
    # print(index)
    kdata.set_index(index, inplace=True)
    #return kdata.resample(periods, how=conversion)
    return kdata.resample(periods).apply(conversion)


def resampleKData(kdata, periods):
    '''合成大周期K线数据'''
    grouped = kdata.groupby('date')
    after_kdata = None
    for name, group in grouped:
        resample_df = reSampleDailyKData(group, periods)
        if after_kdata is None:
            after_kdata = resample_df
        else:
            after_kdata = after_kdata.append(resample_df, ignore_index=True)
    return after_kdata


def getResampleKData(df,periods):
    #获取大周期K线数据
    df = gen_trade_date(df)
    df = resampleKData(df,periods)
    return df


def test():
    filename = u'F:/trade/Riemann/data/SHFE.rb1705_1m_bars.csv'
    df = load_gm_csv(filename)
    df = getResampleKData(df,'5T')
    print(df.head())


if __name__ == '__main__':
    test()
