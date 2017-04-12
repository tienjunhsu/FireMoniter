#! /user/bin/env python
#encoding:utf-8
import pymongo
import pandas as pd
from WindPy import w

output_mongo_client = pymongo.MongoClient('192.168.2.181', 27017)
output_db = output_mongo_client['fire_trade']

def gen_long_basket():
    pass

def gen_ic_basket():
    pass

def gen_if_basket():
    pass

def get_all_df():
    cursor = output_db['strategy_list_output01'].find({'date': '2017-04-09'},{'_id': 0, 'code': 1, 'weight': 1, 'wind_code': 1})
    df = pd.DataFrame(list(cursor))
    xan = ['600008','601258','600155','603616','300137','600874','300428','000786']
    df = df.groupby(['code','wind_code']).agg({'weight':sum})
    df = df.reset_index()
    df = df[~df.code.isin(xan)]
    print(df['weight'].sum())
    df.loc[:,'weight'] = df['weight']/df['weight'].sum()
    return df

def get_ic_r_df():
    r_ss = [u'list_5_0.0499', u'list_6_0.0903', u'list_7_0.0847']
    cursor = output_db['strategy_list_output01'].find({'date': '2017-04-09','strategy':{'$in':r_ss}},{'_id': 0, 'code': 1, 'weight': 1, 'wind_code': 1})
    df = pd.DataFrame(list(cursor))
    xan = ['600008','601258','600155','603616','300137','600874','300428','000786']
    df = df.groupby(['code','wind_code']).agg({'weight':sum})
    df = df.reset_index()
    df = df[~df.code.isin(xan)]
    df.loc[:,'weight'] = df['weight']/0.997968303214
    return df

def get_ic_d_df():
    d_ss =  [u'list_8_0.0914', u'list_9_0.0867', u'list_10_0.0423',u'list_11_0.0418',u'list_12_0.0462',u'list_13_0.045',u'list_14_0.0856',u'list_15_0.0494']
    cursor = output_db['strategy_list_output01'].find({'date': '2017-04-09','strategy':{'$in':d_ss}},{'_id': 0, 'code': 1, 'weight': 1, 'wind_code': 1})
    df = pd.DataFrame(list(cursor))
    xan = ['600008','601258','600155','603616','300137','600874','300428','000786']
    df = df.groupby(['code','wind_code']).agg({'weight':sum})
    df = df.reset_index()
    df = df[~df.code.isin(xan)]
    df.loc[:,'weight'] = df['weight']/(0.997968303214*3)
    print(df['weight'].sum())
    df.to_excel('ic_cc.xlsx')
    return df

def get_if_d_df():
    d_ss = r_ss = [u'list_1_0.0506', u'list_5_0.0499', u'list_7_0.0847']
    cursor = output_db['strategy_list_output01'].find({'date': '2017-04-09','strategy':{'$nin':d_ss}},{'_id': 0, 'code': 1, 'weight': 1, 'wind_code': 1})
    df = pd.DataFrame(list(cursor))
    xan = ['600008','601258','600155','603616','300137','600874','300428','000786']
    df = df.groupby(['code','wind_code']).agg({'weight':sum})
    df = df.reset_index()
    df = df[~df.code.isin(xan)]
    df.loc[:,'weight'] = df['weight']/(0.997968303214*3)
    df.to_excel('if_cc.xlsx')
    print(df['weight'].sum())
    return df

def get_ih_d_df():
    cursor = output_db['strategy_list_output01'].find({'date': '2017-04-09'},{'_id': 0, 'code': 1, 'weight': 1, 'wind_code': 1})
    df = pd.DataFrame(list(cursor))
    xan = ['600008','601258','600155','603616','300137','600874','300428','000786']
    df = df.groupby(['code','wind_code']).agg({'weight':sum})
    df = df.reset_index()
    df = df[~df.code.isin(xan)]
    df.loc[:,'weight'] = df['weight']/(0.997968303214*3)
    print(df['weight'].sum())
    df.to_excel('ih_cc.xlsx')
    return df

def adjust_hedge():
    #ic_df = get_ic_d_df()
    #if_df = get_if_d_df()
    ih_df = get_ih_d_df()
    # df = ic_df.append(if_df)
    # df = df.append(ih_df)
    # df = df.groupby(['code','wind_code']).agg({'weight':sum})
    # df = df.reset_index()
    # df.to_excel('hedge_jhcc.xlsx')
    #df = ic_df
    #df = if_df
    df = ih_df
    w.start()
    df['close'] = w.wsd(','.join(df['wind_code'].tolist()), "close", "2017-04-11", "2017-04-11", "Fill=Previous").Data[0]
    df['position_num'] = 0
    products =[u'一号产品',u'兴鑫']
    totalAmounts = [7650000*3,11500000*3]
    for i in range(len(products)):
        product = products[i]
        totalAmount = totalAmounts[i]*df.weight.sum()
        print(df['weight'].sum())
        df = approach_totalAmount(totalAmount,df)
        save_name = product +u'IH计划持仓.xlsx'
        df.to_excel(save_name)
        df.position_num = 0



def approach_totalAmount(totalAmount,df):
    #获取计划持仓股数，逼近总金额
    #totalAmount是计划持仓的总金额，df是带有收盘价的策略生成的票池DataFrame
    delta = 1000.0 #最小的迭代步长
    df.position_num = 100 * ((totalAmount * df.weight) // (100 * df.close))
    real_total = (df.position_num * df.close).sum()
    new_totalAmount = totalAmount
    while real_total < totalAmount:
        new_totalAmount +=  min(totalAmount - real_total,delta)
        df.position_num = 100 * ((new_totalAmount * df.weight) // (100 * df.close))
        real_total = (df.position_num * df.close).sum()
    return df

def main():
    df = get_all_df()
    w.start()
    df['close'] = w.wsd(','.join(df['wind_code'].tolist()), "close", "2017-04-11", "2017-04-11", "Fill=Previous").Data[0]
    df['position_num'] = 0
    products = [u'一号产品敞口裸多',u'兴鑫敞口裸多',u'华泰吴雪敏',u'华泰沙莎',u'术源九州',u'自强一号']
    totalAmounts = [8220000,2500000,20600000,5500000,16500000,6000000]
    for i in range(len(products)):
        product = products[i]
        totalAmount = totalAmounts[i]
        print(df['weight'].sum())
        df = approach_totalAmount(totalAmount,df)
        save_name = product +'.xlsx'
        df.to_excel(save_name)
        df.position_num = 0


def approach_totalAmount(totalAmount,df):
    #获取计划持仓股数，逼近总金额
    #totalAmount是计划持仓的总金额，df是带有收盘价的策略生成的票池DataFrame
    delta = 1000.0 #最小的迭代步长
    df.position_num = 100 * ((totalAmount * df.weight) // (100 * df.close))
    real_total = (df.position_num * df.close).sum()
    new_totalAmount = totalAmount
    while real_total < totalAmount:
        new_totalAmount +=  min(totalAmount - real_total,delta)
        df.position_num = 100 * ((new_totalAmount * df.weight) // (100 * df.close))
        real_total = (df.position_num * df.close).sum()
    return df

def adjust():
    product = u'术源九州'
    plans =product + u'.xlsx'
    now = u'20170411术源九州.xls'
    plan_df = pd.read_excel(plans,converters = {'code':str})
    xan = ['600008','601258','600155','603616','300137','600874','300428','000786']
    now_df = parse_hs_excel_position(now)
    #now_df =parse_ims_excel_position(now)
    now_df = now_df[['code','position_num']]
    now_df.columns = ['code','old_position_num']
    now_df = now_df[~now_df.code.isin(xan)]
    plan_df = plan_df.set_index('code')
    now_df = now_df.set_index('code')
    df = plan_df.join(now_df,how='outer')
    df = df.fillna(0)
    df.loc[:,'position_num'] = df['position_num'] - df['old_position_num']
    df = df.reset_index()
    df.to_excel(product+'_diff.xlsx')

def parse_ims_excel_position(filename):
    df = pd.read_excel(filename,converters = {u'证券代码':str})
    df = df[[u'证券代码',u'证券名称',u'持仓数量',u'持仓市值']]
    df.columns = ['code','name','position_num','position_value']
    df=df[df.code.str.startswith('30')|df.code.str.startswith('60')|df.code.str.startswith('00')]
    df = df[df.position_num > 0]
    return df


def parse_hs_excel_position(filename):
    df = pd.read_excel(filename,converters = {u'证券代码':str})
    df = df[[u'证券代码',u'证券名称',u'持仓数量',u'持仓市值(全价)']]
    df.columns = ['code','name','position_num','position_value']
    df=df[df.code.str.startswith('30')|df.code.str.startswith('60')|df.code.str.startswith('00')]
    df = df[df.position_num > 0]
    return df


def parse_xt_excel_position(filename):
    df = pd.read_excel(filename,converters = {u'证券代码':str})
    df = df[[u'证券代码',u'证券名称',u'当前拥股',u'最新价']]
    df.columns = ['code','name','position_num','price']
    df=df[df.code.str.startswith('30')|df.code.str.startswith('60')|df.code.str.startswith('00')]
    df['position_value'] = df['position_num']*df['price']
    df = df[['code','name','position_num','position_value']]
    df = df[df.position_num > 0]
    return df

def adjust_hedge():
    plannames =[u'一号产品IC计划持仓.xlsx',u'一号产品IF计划持仓.xlsx',u'一号产品IH计划持仓.xlsx',u'一号产品敞口裸多.xlsx']
    now = u'20170411一号产品敞口裸多.xlsx'
    plan_df = None
    for plan in plannames:
        k_df = pd.read_excel(plan,converters = {'code':str})
        if plan_df is None:
            plan_df = k_df
        else:
            plan_df = plan_df.append(k_df)
    plan_df = plan_df[['code','wind_code','close','position_num']]
    plan_df = plan_df.groupby(['code','wind_code','close']).agg({'position_num':sum})
    plan_df = plan_df.reset_index()
    xan = ['600008','601258','600155','603616','300137','600874','300428','000786']
    #now_df = parse_hs_excel_position(now)
    now_df =parse_ims_excel_position(now)
    now_df = now_df[['code','position_num']]
    now_df.columns = ['code','old_position_num']
    now_df = now_df[~now_df.code.isin(xan)]
    plan_df = plan_df.set_index('code')
    now_df = now_df.set_index('code')
    df = plan_df.join(now_df,how='outer')
    df = df.fillna(0)
    df.loc[:,'position_num'] = df['position_num'] - df['old_position_num']
    df = df.reset_index()
    df.to_excel(u'一号产品total'+'_diff.xlsx')

if __name__ == '__main__':
    #main()
    #adjust()
    #get_all_df()
    adjust_hedge()