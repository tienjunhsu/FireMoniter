#! /user/bin/env python
#encoding:utf-8
#中山200亿以上换仓
from __future__ import  division
import math
import pandas as pd
import pymongo
import requests
from pandas import ExcelWriter
from WindPy import w

basket_unit = 1500000.0 #一个交易篮子150万
mongo_client = pymongo.MongoClient('192.168.2.181', 27017)
db = mongo_client['fire_trade']

def parse_xt_excel_position(filename):
    df = pd.read_excel(filename, converters={u'证券代码': str})
    #df = pd.read_csv(filename,encoding='gbk', converters={u'证券代码': str})
    df = df[[u'证券代码', u'证券名称', u'当前拥股', u'最新价']]
    df.columns = ['code', 'name', 'position_num', 'price']
    df = df[df.code.str.startswith('30') | df.code.str.startswith('60') | df.code.str.startswith('00')]
    df['position_value'] = df['position_num'] * df['price']
    df = df[['code', 'name', 'position_num', 'position_value']]
    df = df[df.position_num > 0]
    return df


def parse_hs_excel_position(filename):
    df = pd.read_excel(filename, converters={u'证券代码': str})
    df = df[[u'证券代码', u'证券名称', u'持仓数量', u'持仓市值(全价)']]
    df.columns = ['code', 'name', 'position_num', 'position_value']
    df = df[df.code.str.startswith('30') | df.code.str.startswith('60') | df.code.str.startswith('00')]
    df = df[df.position_num > 0]
    return df


def parse_ss_csv_position(filename):
    #莎莎（同花顺?）导出的持仓
    df = pd.read_csv(filename,encoding='gbk',sep='\t', converters={u'证券代码': str})
    df = df[[u'证券代码', u'证券名称', u'股票余额', u'市值']]
    df.columns = ['code', 'name', 'position_num', 'position_value']
    df = df[df.code.str.startswith('30') | df.code.str.startswith('60') | df.code.str.startswith('00')]
    df = df[df.position_num > 0]
    return df

def approach_totalAmount(totalAmount, df):
    # 获取计划持仓股数，逼近总金额
    # totalAmount是计划持仓的总金额，df是带有收盘价的策略生成的票池DataFrame
    delta = 1000.0  # 最小的迭代步长
    totalAmount = totalAmount * df.weight.sum()  # 总金额根据权重进行调整
    df.position_num = 100 * ((totalAmount * df.weight) // (100 * df.close))
    real_total = (df.position_num * df.close).sum()
    new_totalAmount = totalAmount
    while real_total < totalAmount:
        new_totalAmount += min(totalAmount - real_total, delta)
        df.position_num = 100 * ((new_totalAmount * df.weight) // (100 * df.close))
        real_total = (df.position_num * df.close).sum()
    return df


def main():
    #中山14000万,莎莎500万
    filename = u'F:/firecapital/数据/realoutput20170508/lists_outputlist_selectedlist_5_8_5_37_35.xlsx'
    df = pd.read_excel(filename,converters = {'code':str})
    df = df[['period','code','weight']]
    if len(df.code[0]) > 6:
        df.code = df.code.str.slice(2,8)
    df01 = df[~df.code.str.startswith('60')]
    df = df[df.code.str.startswith('60')]
    df01['wind_code'] = df01.code +'.SZ'
    df['wind_code'] = df.code +'.SH'
    df = df.append(df01)
    print(len(df))
    #risk_url = 'http://180.168.45.126:60618/risk/highriskticks/2017-05-01/'
    risk_url = 'http://192.168.2.112：8000/risk/highriskticks/2017-05-07/'
    high_risk_list = requests.get(risk_url).text.split(',')
    df = df[~df.code.isin(high_risk_list)]
    w.start()
    date = "2017-05-05"
    df['float_a_shares'] = w.wsd(','.join(df.wind_code.tolist()),"float_a_shares", date, date, "unit=1;currencyType=;Fill=Previous").Data[0]
    df['close'] = w.wsd(','.join(df.wind_code.tolist()),"close", date, date, "Fill=Previous").Data[0]
    df['mkt'] = df.close * df.float_a_shares
    df= df[df.mkt >=20000000000]
    print(len(df))
    df.loc[:,'weight'] = df.weight/df.weight.sum()
    df['close'] =  w.wsd(','.join(df.wind_code.tolist()),"close", date, date, "Fill=Previous").Data[0]
    #df['name'] =  w.wsd(','.join(df.wind_code.tolist()),"sec_name", date, date, "Fill=Previous").Data[0]
    #df.to_excel(u'gte200.xlsx',index=False)
    df['position_num'] = 0
    #plan_df = approach_totalAmount(14000000,df)
    plan_df = approach_totalAmount(5000000,df)
    plan_df.to_excel(u'莎莎计划持仓.xlsx',index=False)
    #plan_df.to_excel(u'中山计划持仓.xlsx',index=False)
    #now_df = parse_xt_excel_position(u'0505中山持仓统计.xls')
    #now_df = parse_hs_excel_position(u'0505莎莎持仓统计.xls')
    now_df = parse_ss_csv_position(u'0505莎莎持仓统计.xls')
    now_df = now_df[['code', 'position_num']]
    now_df.columns = ['code', 'old_position_num']
    plan_df = plan_df.set_index('code')
    now_df = now_df.set_index('code')
    plan_df = plan_df.join(now_df, how='outer')
    plan_df = plan_df.fillna(0)
    plan_df.loc[:, 'position_num'] = plan_df['position_num'] - plan_df['old_position_num']
    plan_df = plan_df.reset_index()
    #plan_df.to_excel('kkkkkk.xlsx',index=False)
    plan_df01 = plan_df[~plan_df.code.str.startswith('60')]
    plan_df = plan_df[plan_df.code.str.startswith('60')]
    plan_df01['wind_code'] = plan_df01.code +'.SZ'
    plan_df['wind_code'] = plan_df.code +'.SH'
    plan_df = plan_df.append(plan_df01)
    #由持仓获取出来的收盘价和计算时候的收盘价不一定是一样的，说不定有一定的差异（数据误差或者在盘中某个时候导出的持仓等）
    #所以需要重新获取收盘价
    #股票名称有可能不是严格相同的(比如内部有空格等等)，所以重新获取名字更可靠
    result = w.wsd(','.join(plan_df.wind_code.tolist()),"close", date, date, "Fill=Previous")
    print(result)
    plan_df['close'] =  result.Data[0]
    plan_df['position_value'] = plan_df.position_num * plan_df.close
    plan_df['name'] =  w.wsd(','.join(plan_df.wind_code.tolist()),"sec_name", date, date, "Fill=Previous").Data[0]
    format_2_pb(plan_df)
    #format_2_xt(plan_df)


def calculate_basket_num(df):
    #生成交易篮子
    if len(df) > 0:
        basket_num = df.position_value.sum()/basket_unit
        basket_num = int(math.ceil(basket_num)) #向上取整
        df['p_position_num'] = 100*((df.position_num//basket_num)//100) #前面的n-1篮子，平分
        #余下的一个篮子
        df.loc[:,'position_num'] = df.position_num - (basket_num - 1)*df.p_position_num
        if len(df[df.position_num < 0]) > 0:
            #有效性检查，如果发现余下的一个篮子里面出现了负数，说明计算出现错误
            raise Exception("Wrong!Have negative number in df's position_num")
        return df,basket_num
    else:
        return df,1


def format_2_pb(df):
    df01 = df[df['code'].str.startswith('60')]
    df01['market_type'] = 1
    df = df[~df['code'].str.startswith('60')]
    df['market_type'] = 2
    df = df.append(df01)
    reduce_df = df[df.position_num < 0]
    df = df[df.position_num >= 0]
    reduce_df.loc[:, 'position_num'] = - reduce_df.position_num
    reduce_df.loc[:, 'position_value'] = - reduce_df.position_value
    filename = u'莎莎加减仓.xlsx'
    columns = ['code', 'name', 'market_type', 'position_num']
    header = [u'证券代码', u'证券名称', u'交易市场', u'数量/权重']
    save_to_excel(filename, df, reduce_df, columns, header)


def save_to_excel(filename, add_df, reduce_df, columns, header):
    #将篮子保存为excel文件
    #其中filename 是文件名,add_df是需要加仓的DataFrame,reduce_df是需要减仓的DataFrame
    #columns是一个list，是需要输出到excel里面的列
    #header是columns输出时对应的头名字，类型为list
    writer = ExcelWriter(filename)
    add_df.to_excel(writer, u'加仓汇总', columns=columns, header=header, index=False)
    reduce_df.to_excel(writer, u'减仓汇总', columns=columns, header=header, index=False)
    n_columns = ','.join(columns)
    n_columns = n_columns.replace('position_num','p_position_num')
    n_columns = n_columns.split(',')
    add_df,add_basket_num = calculate_basket_num(add_df)
    if add_basket_num > 1:
        if add_basket_num > 2:
            sheet_name = u'加仓第1-'+str(add_basket_num - 1)+u'篮'
        else:
            sheet_name = u'加仓第'+str(add_basket_num - 1)+u'篮'
        add_df.to_excel(writer, sheet_name,columns=n_columns, header=header, index=False)
    add_df.to_excel(writer, u'加仓第'+str(add_basket_num)+u'篮', columns=columns, header=header, index=False)
    reduce_df,reduce_basket_num = calculate_basket_num(reduce_df)
    if reduce_basket_num > 1:
        if reduce_basket_num > 2:
            sheet_name = u'减仓第1-'+str(reduce_basket_num - 1)+u'篮'
        else:
            sheet_name = u'减仓第'+str(reduce_basket_num - 1)+u'篮'
        reduce_df.to_excel(writer, sheet_name,columns = n_columns,header=header, index=False)
    reduce_df.to_excel(writer, u'减仓第'+str(reduce_basket_num)+u'篮', columns=columns, header=header, index=False)
    writer.save()


def format_2_xt(df):
    df['d_weight'] = 1
    df['d_dr'] = 1
    reduce_df = df[df.position_num < 0]
    df = df[df.position_num >= 0]
    reduce_df.loc[:, 'position_num'] = - reduce_df.position_num
    reduce_df.loc[:, 'position_value'] = - reduce_df.position_value
    filename = u'中山加减仓.xlsx'
    columns = ['code', 'name', 'position_num', 'd_weight', 'd_dr']
    header = [u'证券代码', u'证券名称', u'目标数量', u'目标权重', u'方向']
    save_to_excel(filename, df, reduce_df, columns, header)

def check_diff():
    old = u'市值200亿以上.xlsx'
    new = u'200亿市值以上.xlsx'
    df = pd.read_excel(new,converters={'code':str})
    last_df = pd.read_excel(old,converters={'code':str})
    df = df[['code', 'weight']]
    df = df.set_index('code')
    df.columns = ['n_weight']
    last_df = last_df[['code', 'weight']]
    last_df = last_df.set_index('code')
    last_df.columns = ['o_weight']
    df = df.join(last_df, how='outer')
    df = df.fillna(0.0)
    weight_changed = df['n_weight'] - df['o_weight']
    weight_changed = weight_changed[weight_changed > 0].sum()
    print(weight_changed)


if __name__ == '__main__':
    main()
    #check_diff()
    #parse_ss_csv_position(u'0505莎莎持仓统计.xls')