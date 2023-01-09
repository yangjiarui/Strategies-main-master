#!/usr/bin/env python
# -*- coding: utf-8 -*-
__title__ = 'My py ctp of se'
__author__ = 'HaiFeng'
__mtime__ = '20190506'

import datetime
import sys,redis
from py_ctp.trade import CtpTrade
from py_ctp.quote import CtpQuote
from py_ctp.enums import *
import time,re
import dolphindb as ddb
import threading
import pandas as pd


class MyTrade(object):
    def __init__(self, addr: str, broker: str, investor: str, pwd: str, appid: str, auth_code: str):
        self.front = addr
        self.broker = broker
        self.investor = investor
        self.pwd = pwd
        self.appid = appid
        self.authcode = auth_code
        self.proc = '@haifeng'

        self.t = CtpTrade()
        self.t.OnConnected = self.on_connect
        self.t.OnUserLogin = lambda o, x: print('Trade logon:', x)
        self.t.OnDisConnected = lambda o, x: print(x)
        self.t.OnRtnNotice = lambda obj, time, msg: print(f'OnNotice: {time}:{msg}')
        self.t.OnErrRtnQuote = lambda obj, quote, info: None
        self.t.OnErrRtnQuoteInsert = lambda obj, o: None
        self.t.OnOrder = lambda obj, o: None
        self.t.OnErrOrder = lambda obj, f, info: None # print(info)
        self.t.OnTrade = lambda obj, o: None
        self.t.OnCancel = lambda obj, o: None
        self.t.OnInstrumentStatus = lambda obj, inst, stat: None
        self.t.OnRspError = lambda obj, info: print(info)
        

    def on_connect(self, obj):
        self.t.ReqUserLogin(self.investor, self.pwd, self.broker, self.proc, self.appid, self.authcode)

    def run(self):
        print(self.t.GetVersion())
        print('trade connect...')
        self.t.ReqConnect(self.front)

    def release(self):
        self.t.ReqUserLogout()


class MyQuote(object):
    """MyQuote"""

    def __init__(self, addr: str, broker: str, investor: str, pwd: str):
        """"""
        self.front = addr
        self.broker = broker
        self.investor = investor
        self.pwd = pwd
        self.tempdf=pd.DataFrame()
        # self.connect_dolphin()
        self.q = CtpQuote()
        self.q.OnTick = lambda o, x:self.to_df(x.__str__().split(',')) ####打印每一笔tick进行
        self.q.OnConnected = lambda x: self.q.ReqUserLogin(self.investor, self.pwd, self.broker)
        self.q.OnUserLogin = lambda o, i:self.q.ReqSubscribeMarketData(self.sublist)#, self.q.ReqSubscribeMarketData('sa2201')
        self.flag=False
        # self.q.sswe=lambda g:self.q.ReqSubscribeMarketData('jd2201')
        threading.Thread(target=self.to_dolphin).start()

    # self.q.OnUserLogin = lambda o, i: self.q.ReqSubscribeMarketData(['jd2201,rb2201'])

    def connect_dolphin(self):
        self.con=ddb.session()
        self.con.connect('localhost',8922,'admin','123456')
#         self.con.connect('server.natappfree.cc,38815,'admin','123456')
#         self.con.connect(DBhost,DBport,'admin','123456')
        self.con.run('''login(`admin,`123456)
        db=database('dfs://jerry');
        if ( not existsTable('dfs://jerry',`futures) ){ futures= db.createPartitionedTable(  table(1000000:0,`合约名称`当前时间`开盘价`最高价`最低价`昨日收盘价`买价`卖价`最新价`结算价`昨结算`买量`卖量`持仓量`成交量`商品交易所简称`种名简称`日期,[SYMBOL,TIMESTAMP,DOUBLE,DOUBLE,DOUBLE,DOUBLE,DOUBLE,DOUBLE,DOUBLE,DOUBLE,DOUBLE,DOUBLE,DOUBLE,DOUBLE,DOUBLE,SYMBOL,SYMBOL,DATETIME]),`futures,`日期`种名简称);  }else {  futures=loadTable(db,`futures)}

        '''
        )

    def run(self):
        print('quote connecting...')

        self.q.ReqConnect(self.front)

    def to_df(self,x):
        ###更新最新价格
        # print(x)
        # lastprice.update({x[0]:float( x[1] )})
        res={"合约名称":x[0],"最新价":float( x[1] ),"卖价":float( x[2]),"卖量":float( x[3]),"买价":float( x[4]),"买量":float( x[5]),
             "更新时间":x[6],"成交量":float( x[7]),"持仓量":float( x[8]),"平均价":float( x[9]),"上限价":float( x[10]),"下限价":float( x[11]),
             "昨持仓量":float( x[12])}
        res=pd.DataFrame(res,index=[0])
        self.tempdf=self.tempdf.append(res,ignore_index=False)
        # print(self.tempdf)


    def to_dolphin(self):
        while 1:
            if self.flag:
                return
            if self.tempdf.empty:
                continue
            self.tempdf.sort_values(by=['合约名称','更新时间'],inplace=True,ascending=[1,1])
            time.sleep(4.5)   ######每隔8秒生成k线
            # print(self.tempdf)
            ####insert TO dolpindb
            c=self.tempdf.groupby('合约名称')
            good=c.last().reset_index()
            now=datetime.datetime.now().strftime('%Y-%m-%d')
            good['Instruments']=good['合约名称'].to_list()
            good['time']=pd.to_datetime(now+' '+c.last()['更新时间']).to_list()
            good['开盘价']= c.first()['最新价'].to_list()
            good['昨日收盘价']=0.0
            good['最高价']= c.max()['最新价'].to_list()
            good['最低价']=c.min()['最新价'].to_list()
            good['昨结算']=0.0
            good['结算价']=0.0
            good['商品交易所简称']=''
            good['种名简称']=good['合约名称'].apply(lambda x:re.findall('\D+',x)[-1].lower()).to_list()
            good['date']=pd.to_datetime(now)
            # print(good[["Instruments","time","开盘价","最高价","最低价","最新价"]].head(2))

            try:
                self.con.run("tableInsert{{loadTable('{db}','{tb}')}}".format(db='dfs://jerry', tb="futures"),
                             good[["Instruments","time","开盘价","最高价","最低价",'昨日收盘价',"买价","卖价","最新价",
                                   "结算价","昨结算","买量","卖量","持仓量","成交量","商品交易所简称","种名简称","date"]])
                # print('yes')
            except:
                pass
            self.tempdf=pd.DataFrame()
            # c.last()['最新价']

    def release(self):
        self.q.ReqUserLogout()

        #3##关闭to_dolphin汉函数
        self.flag=True
        sys.exit(0)

def CTPmarketfetch():
    front_trade = 'tcp://180.168.146.187:10101'
    ###24环境
    front_quote = 'tcp://180.168.146.187:10131'
    # front_quote = 'tcp://180.168.146.187:10111'
    broker = '9999'
    investor = '008107'
    ####我的
    # investor = '192220'
    pwd = '1'
    appid = 'simnow_client_My'
    auth_code = '0000000000000000'
    # loginfield = mdapi.CThostFtdcReqUserLoginField()
    # loginfield.BrokerID="8000"
    # loginfield.UserID="000005"
    # loginfield.Password="123456"
    # loginfield.UserProductInfo="python dll"
    tt = MyTrade(front_trade, broker, investor, pwd, appid, auth_code)
    tt.run()
    while not tt.t.logined:
        time.sleep(3)
    print('account info')
    print(tt.t.account)
    print(len(tt.t.instruments))
    # tt.t.ReqOrderInsert('rb2101', DirectType.Buy, OffsetType.Open, 4000, 3)

    qq = MyQuote(front_quote, broker, investor, pwd)
    ###########订阅的instruments  list
    ins=list(tt.t.instruments.keys())
    ins=list(set(filter(lambda x:len(x)<7,ins)))
    print(list(set(map(lambda x:re.findall('\D+',x)[0],ins))))
    qq.sublist=ins[:3]
    # qq.sublist=['jd2201','rb2201','zn2109','cu2110C71000']
    qq.run()

    time.sleep(5)
    print('press ENTER key to release')
    input()
    # for inst in tt.t.instruments.values():
    #     print(inst)
    print('trade release')
    # tt.release()
    print('quote release')
    qq.release()

#
#
if __name__ == '__main__':
    CTPmarketfetch()