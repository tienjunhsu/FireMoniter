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
    filename = 'maichuqu.xlsx'
    df = pd.read_excel(filename,converters={'code':str})
    df.loc[:,'code'] = df.code.str.slice(2,8)
    df = df.groupby('code').agg({'position_num': sum})
    df = df.reset_index()
    df01 = df[df['code'].str.startswith('60')]
    df01['market_type'] = 0
    df = df[~df['code'].str.startswith('60')]
    df['market_type'] = 1
    df = df.append(df01)
    df.to_excel('caozuo.xlsx',index=False)

if __name__ == '__main__':
    main()