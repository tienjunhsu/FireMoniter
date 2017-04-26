#! /user/bin/env python
#encoding:utf-8

import os
import pandas as pd

from WindPy import w



def parse_other_high_risk(filename,df):
    if os.path.exists(filename):
        other_df = pd.read_excel(filename,converters={'code':str})
        other_df = other_df[other_df.code.isin(df)]
        pass


def psrseStockPool():
    filename = u'F:/firecapital/数据/20170416_outputlist_selectedlist_4_16_14_55_2.xlsx'
    df = pd.read_excel(filename,converters = {'code':str})
    df = df[['period','code','weight']]
    if len(df.code[0]) > 6:
        df.code = df.code.str.slice(2,8)
    df['date'] = '2017-04-16'
    df01 = df[~df.code.str.startswith('60')]
    df = df[df.code.str.startswith('60')]
    df01['wind_code'] = df01.code +'.SZ'
    df['wind_code'] = df.code +'.SH'
    df = df.append(df01)
    return df




def main():
    df = psrseStockPool()
    secs = df.wind_code.tolist()
    last_trade_day = '2017-04-14'
    w.start()
    result = w.wsd(secs,"bps_new", last_trade_day, last_trade_day, "Fill=Previous")
    if result.ErrorCode != 0:
        raise Exception('Error in get bps_new :' + str(result.ErrorCode))
    df.loc[:,'bps'] = result.Data[0]
    bps_df = df[df.bps <=0]
    if len(bps_df) > 0:
        print('bps:'+str(len(bps_df)))
        df = df[~df['code'].isin(bps_df.code)]
    result = w.wset("sectorconstituent",'date=2017-04-16;sectorid=1000006526000000')
    if result.ErrorCode != 0:
        raise Exception('Error in get ST sec :' + str(result.ErrorCode))
    data = {'date': result.Data[0], 'code': result.Data[1], 'name': result.Data[2]}
    st_df = pd.DataFrame(data)
    st_df.code = st_df.code.str.slice(0, 6)
    st_df = st_df[st_df.code.isin(df.code)]
    if len(st_df.index) > 0:
        print('st:'+str(len(st_df)))
        df = df[~df['code'].isin(st_df.code)]
    result = w.wset("tradesuspend","startdate={0};enddate={1};field={2}".format('2017-04-14','2017-04-16','date,wind_code,sec_name'))
    data = {'date': result.Data[0], 'code': result.Data[1], 'name': result.Data[2]}
    halt_df = pd.DataFrame(data)
    halt_df.code = halt_df.code.str.slice(0, 6)
    halt_df = halt_df[halt_df.code.isin(df.code)]
    if len(halt_df.index) > 0:
        print('halt:'+str(len(halt_df)))
        df = df[~df['code'].isin(halt_df.code)]
    secs = df.wind_code.tolist()
    open = w.wsd(secs,'open',last_trade_day,last_trade_day,'').Data[0]
    close =w.wsd(secs,'close',last_trade_day,last_trade_day,'').Data[0]
    pct_chg = w.wsd(secs,'pct_chg',last_trade_day,last_trade_day,'').Data[0]  # 涨跌幅度

    n_df = pd.DataFrame({'open': open, 'close': close, 'pct_chg': pct_chg}, index=df.index)
    df = df.join(n_df)

    l_df = df[['date', 'code']][((df.pct_chg > 9.97) | (df.pct_chg < -9.97)) & (df.open == df.close)]
    if len(l_df.index) > 0:
        print('limit:'+str(len(l_df)))
        df = df[~df['code'].isin(l_df.code)]

    other_df = pd.read_excel(u'F:/firecapital/文档/其他风险票.xlsx',converters={'code':str})
    print(other_df.head())
    other_df = other_df[other_df.code.isin(df.code.tolist())]
    if len(other_df) > 0:
        print('other：'+str(len(other_df)))
        other_df.to_excel('other_high.xlsx')

if __name__ == '__main__':
    main()
