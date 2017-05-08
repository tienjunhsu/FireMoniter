#! /user/bin/env python
# encoding:utf-8
import requests
import pandas as pd
import json

markets = ['szmb','szsme','szcn','shmb']
market_names = ['深市主板','中小板','创业板','沪市']
market_name_dict = dict(zip(markets, market_names))


def fetch_prbookinfo_from_cninfo(market):
    #从巨潮资讯获取报告披露时间信息
    #market是市场代码
    print('开始获取'+market_name_dict[market]+'数据.......')
    base_url = 'http://www.cninfo.com.cn/cninfo-new/information/prbookinfo-1'
    data = {'sectionTime': '2016-12-31', 'market': market, 'orderClos': '', 'isDesc': '', 'stockCode': '',
            'firstTime': u'请输入预约披露时间', 'lastTime': '请输入实际披露时间'}
    r = requests.post(base_url,data=data)
    if r.status_code != 200:
        raise Exception('Error in get data,status_code is:'+str(r.status_code))
    datas = json.loads(r.text)
    df = pd.DataFrame(datas)
    print('共获取'+market_name_dict[market]+'股票 '+ str(len(df)) + '只')
    return df


def fetch_no_annualReport_stocks():
    # 筛选没有出年报的股票
    df = None
    for market in markets:
        t_df = fetch_prbookinfo_from_cninfo(market)
        #实际披露时间形如2017-03-17，至少有10个字符
        t_df = t_df[t_df.f006d_0102.str.len() < 10]
        print(market_name_dict[market]+'尚未披露年报的股票有' + str(len(t_df)) + '只')
        if df is None:
            df = t_df
        else:
            df = df.append(t_df,ignore_index=True)
    print('还没有披露年报的股票一共有 '+ str(len(df))+ '只')
    return df


def main():
    hdf = fetch_no_annualReport_stocks()
    print(hdf)
    # filename = u'F:/firecapital/数据/output20170501/普通总列表.xlsx'
    # df = pd.read_excel(filename,converters = {'code':str})
    # df = df[['period','code','weight']]
    # if len(df.code[0]) > 6:
    #     df.code = df.code.str.slice(2,8)


if __name__ == '__main__':
    main()
