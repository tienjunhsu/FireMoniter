#! /user/bin/env python
# encoding:utf-8
# 挑选出指数成分股

import pandas as pd
from  WindPy import w

today = '2017-05-07'


def get_share500_stock():
    # 获取中证500指数成分股
    para = 'date={0};sectorid=1000008491000000;field=wind_code'.format(today)
    wind_code = w.wset("sectorconstituent", para).Data[0]
    return wind_code


def get_share300_stock():
    # 获取沪深300指数成分股
    para = 'date={0};sectorid=1000000090000000;field=wind_code'.format(today)
    wind_code = w.wset("sectorconstituent", para).Data[0]
    return wind_code


def get_share50_stock():
    # 获取上证50指数成分股
    para = 'date={0};sectorid=1000000087000000;field=wind_code'.format(today)
    wind_code = w.wset("sectorconstituent", para).Data[0]
    return wind_code


def main():
    filename = 'lists_outputlist_selectedlist_5_8_5_37_35.xlsx'
    df = pd.read_excel(filename)
    df = df[['code', 'weight']]
    df['wind_code'] = df.code.str.slice(2, 8) + '.' + df.code.str.slice(0, 2)
    w.start()
    s300 = df[df.wind_code.isin(get_share300_stock())]
    s300.loc[:, 'weight'] = s300.weight/s300.weight.sum()
    s300.to_excel('300share.xlsx', index=False)
    s500 = df[df.wind_code.isin(get_share500_stock())]
    s500.loc[:, 'weight'] = s500.weight/s500.weight.sum()
    s500.to_excel('500share.xlsx', index=False)
    s50 = df[df.wind_code.isin(get_share50_stock())]
    s50.loc[:, 'weight'] = s50.weight/s50.weight.sum()
    s50.to_excel('50share.xlsx', index=False)

if __name__ == '__main__':
    main()
