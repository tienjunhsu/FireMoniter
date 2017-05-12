#! /user/bin/env python
#encoding:utf-8
from __future__ import division
import os
import pandas as pd

rootdir = u'C:/Users/asus/Desktop/xx3.6'
def main():
    dates = ['0510']
    for date in dates:
        plan_name = u'%s一号产品加减仓.xlsx' % date
        now_name = u'%s一号产品持仓.xlsx' % date
        p_df = parse_plan_position(os.path.join(rootdir,plan_name))
        df = parse_ims_excel_position(os.path.join(rootdir,now_name))
        df = df[df.code.isin(p_df.code.tolist())]
        # p_df = p_df[p_df.code.isin(df.code.tolist())]
        # df = df.set_index('code')
        # p_df = p_df.set_index('code')
        # df['dif'] = p_df.position_num
        # df['p_diff'] = df.dif/df.position_num
        # df = df.reset_index()
        # #df.to_excel(date+'.xlsx',index=False)
        # df = df[df.p_diff <= 0.1]
        print('Date %s T+0 position value is:' % date)
        df.to_excel(date+'t.xlsx',index=False)
        print(df.position_value.sum())


def parse_ims_excel_position(filename):
    df = pd.read_excel(filename, converters={u'证券代码': str})
    df = df[[u'证券代码', u'证券名称', u'持仓数量', u'持仓市值']]
    df.columns = ['code', 'name', 'position_num', 'position_value']
    df = df[df.code.str.startswith('30') | df.code.str.startswith('60') | df.code.str.startswith('00')]
    df = df[df.position_num > 0]
    return df


def parse_plan_position(filename):
    df = pd.read_excel(filename, sheetname=u'加仓汇总', converters={u'合约代码': str})
    df = df[[u'合约代码', u'市场', u'数量/权重']]
    df.columns = ['code', 'market', 'position_num']
    # df01 = pd.read_excel(filename, sheetname=u'减仓汇总', converters={u'合约代码': str})
    # df01 = df01[[u'合约代码', u'市场', u'数量/权重']]
    # df01.columns = ['code', 'market', 'position_num']
    # df = df.append(df01,ignore_index= True)
    df = df[df.position_num == 0]
    return df

if __name__ == '__main__':
    main()
