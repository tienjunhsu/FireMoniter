#! /user/bin/env python
# encoding:utf-8
# 检查加减仓篮子市值

import pandas as pd
import os

from WindPy import w


def parse_data(filename, sheet_name, header, code_name):
    #header 是对应的证券代码和数量两项
    df = pd.read_excel(filename, sheetname=sheet_name, converters={code_name: str})
    df = df[header]
    df.columns = ['code', 'position_num']
    df01 = df[~df.code.str.startswith('60')]
    df = df[df.code.str.startswith('60')]
    df01['wind_code'] = df01.code + '.SZ'
    df['wind_code'] = df.code + '.SH'
    df = df.append(df01)
    return df

def fetch_wsd_data(secs, ind_name, date):
    # 通过万得客户端获取数据
    result = w.wsd(secs, ind_name, date, date, "Fill=Previous")
    return result.Data[0]


def main():
    rootdir = u'F:/firecapital/code/FireMoniter'
    date = '2017-05-11'
    xx_filename = u'兴鑫加减仓.xlsx'
    sheet_names = [u'减仓第1-18篮',u'减仓第19篮',u'减仓第20篮']
    xx_header = [u'证券代码', u'数量/权重']
    xx_value = 0
    print('xx:')
    w.start()
    loop = 0
    for sheet_name in sheet_names:
        df = parse_data(os.path.join(rootdir,xx_filename), sheet_name, xx_header, u'证券代码')
        df['close'] = fetch_wsd_data(','.join(df.wind_code.tolist()),'close',date)
        df['position_value'] = df.close * df.position_num
        basket_value = df.position_value.sum()
        print(sheet_name)
        print(basket_value)
        if loop == 0:
            xx_value += basket_value*18
        else:
            xx_value += basket_value
        loop += 1
    print('xx all value is %s' % str(xx_value))

    wy_filename = u'一号产品加减仓.xlsx'
    sheet_names = [u'减仓第1-19篮',u'减仓第20篮',u'减仓第21篮']
    wy_header = [u'合约代码', u'数量/权重']
    wy_value = 0
    print('wy:')
    loop = 0
    for sheet_name in sheet_names:
        df = parse_data(os.path.join(rootdir,wy_filename), sheet_name, wy_header, u'合约代码')
        df['close'] = fetch_wsd_data(','.join(df.wind_code.tolist()),'close',date)
        df['position_value'] = df.close * df.position_num
        basket_value = df.position_value.sum()
        #wy_value += basket_value
        print(sheet_name)
        print(basket_value)
        if loop == 0:
            wy_value += basket_value*19
        else:
            wy_value += basket_value
        loop += 1
    print('wy all value is %s' % str(wy_value))
    # filename = u'C:/Users/asus/Desktop/xx3.6/test.xlsx'
    # df = pd.read_excel(filename, converters={'code': str})
    # #print(df.head())
    # df01 = df[~df.code.str.startswith('60')]
    # df = df[df.code.str.startswith('60')]
    # df01['wind_code'] = df01.code + '.SZ'
    # df['wind_code'] = df.code + '.SH'
    # df = df.append(df01)
    # df['close'] = fetch_wsd_data(','.join(df.wind_code.tolist()),'close',date)
    # df['position_value'] = df.close * df.position_num
    # print(df.position_value.sum())
    # print(df.head())


if __name__ == '__main__':
    main()