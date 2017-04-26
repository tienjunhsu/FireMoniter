#! /user/bin/env python
#encoding:utf-8
import pandas as pd
from WindPy import w

def parse_xt_excel_position(filename):
    df = pd.read_excel(filename, converters={u'证券代码': str})
    #df = pd.read_csv(filename,encoding='gbk', converters={u'证券代码': str})
    df = df[[u'证券代码', u'证券名称', u'数量/权重']]
    df.columns = ['code', 'name', 'position_num']
    df = df[df.code.str.startswith('30') | df.code.str.startswith('60') | df.code.str.startswith('00')]
    return df


def main():
    df = parse_xt_excel_position(u'莎莎加减仓.xlsx')
    df01 = df[~df.code.str.startswith('60')]
    df = df[df.code.str.startswith('60')]
    df01['wind_code'] = df01.code +'.SZ'
    df['wind_code'] = df.code +'.SH'
    df = df.append(df01)
    date = "2017-04-21"
    w.start()
    df['close'] = w.wsd(','.join(df.wind_code.tolist()),"close", date, date, "Fill=Previous").Data[0]
    df['value'] = df.close*df.position_num
    print(df.value.sum())

if __name__ == '__main__':
    main()