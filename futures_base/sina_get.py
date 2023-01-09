# -*- encoding: utf-8 -*-
"""
@File    : sina_get.py
@Time    : 2021/9/9 下午1:35
@Author  : handsomejerry
@Email   : 834235185@qq.com
@Software: PyCharm
"""
import datetime
import re
import time
import concurrent.futures

import pandas as pd
import requests
import dolphindb as ddb
from apscheduler.schedulers.blocking import BlockingScheduler

con=ddb.session()
# con.connect('localhost',8922,'admin','123456')
con.connect('server.natappfree.cc',35298,'admin','123456')
con.run('''login(`admin,`123456)
        db=database('dfs://jerry');
        if ( not existsTable('dfs://jerry',`futures) ){ futures= db.createPartitionedTable(  table(1000000:0,`合约名称`当前时间`开盘价`最高价`最低价`昨日收盘价`买价`卖价`最新价`结算价`昨结算`买量`卖量`持仓量`成交量`商品交易所简称`种名简称`日期,[SYMBOL,TIMESTAMP,DOUBLE,DOUBLE,DOUBLE,DOUBLE,DOUBLE,DOUBLE,DOUBLE,DOUBLE,DOUBLE,DOUBLE,DOUBLE,DOUBLE,DOUBLE,SYMBOL,SYMBOL,DATETIME]),`futures,`日期`种名简称);  }else {  futures=loadTable(db,`futures)}

        '''
        )




# code='nf_JD2112,nf_MA2111'
code='nf_AL2110,nf_ZN2109,nf_MA2111'
     # ',nf_NI2109,nf_RU2201,nf_T0,nf_TS0,nf_V0,nf_V2201,nf_V2202,nf_V2111,nf_V2112,nf_V2110,nf_V2205,nf_V2203,nf_V2204,nf_V2109,nf_V2206,nf_I2201,nf_I0,nf_I2205,nf_I2203,nf_I2202,nf_I2204,nf_I2112,nf_I2206,nf_I2207,nf_I2111,nf_I2208,nf_I2110,nf_I2109'

# last=''
def real_time(codes):
    global last
    now=datetime.datetime.now()
    ###10:15-10:30                                 ###11:30-12:00                   02:30-03:00
    if (now.hour==10 and 15<now.minute<30) or (now.hour==11 and 30<now.minute<59) or (now.hour==13 and 00<now.minute<29) or ( now.hour==2 and 30<now.minute<59 ):
        return
    # global last
    df=s.get(f"https://hq.sinajs.cn/list={codes}").text
    if last==df:
        return
    last=df
    q=re.finditer('var.*?="(.*?)"',df)
    print('pass Check!!!')
    def pocess(inee):
        res={}
        str2=inee.split(',')
        # if str2[1].endswith('00'): return
        try:
            res.update({"Instruments":str2[0],
            "time": pd.to_datetime( str2[17]+ ' ' +(':'.join(re.findall('\d{2}',str2[1]))) ),
            '开盘价':float(str2[2]),
            "最高价":float(str2[3]),
            "最低价": float(str2[4]),
            "昨日收盘价":float(str2[5]),
            "买价":float(str2[6]),
            "卖价":float(str2[7]),
            "最新价":float(str2[8]),
            "结算价":float(str2[9]),
            '昨结算':float(str2[10]),
            '买量':float(str2[11]),
            '卖量':float(str2[12]),
            "持仓量":float(str2[13]),
            '成交量':float(str2[14]),
            '商品交易所简称':str2[15],
            '种名简称':str2[16],
                        'date':pd.to_datetime(str2[17])
            })
            return res
        except Exception as e:
            print(str2)

    for i in q:
        task=pools.submit(pocess,(i.group(1)))
        task.add_done_callback(hh)

    # [pools.submit(pocess,i.group(1),hh) for i in q ]


def hh(rss):
    rss=rss.result()
    df=pd.DataFrame(rss,index=[0])
    # print(df.shape)
    try:
        con.run("tableInsert{{loadTable('{db}','{tb}')}}".format(db='dfs://jerry', tb="futures"),df)
        print('yes')
    except Exception as e:
        print(e.__str__())
    # con.upload({
    #     "insertData":df
    # })
    # con.run('''
    # futures.append!(insertData)''')

    # print(rss)

if __name__ == '__main__':
    pools=concurrent.futures.ThreadPoolExecutor(max_workers=2)
    s=requests.Session()
    sched1=BlockingScheduler()

    last=''
    # real_time(code)


    # sched1.add_job(real_time,'interval',seconds=2,args=[code])
    sched1.add_job(real_time,trigger= 'cron',
                   hour="9-11,13-14",
                   month="1-12",
                   minute="*",
                   day_of_week="mon-fri",
                   second='*/3',args=[code])

    sched1.add_job(real_time,trigger= 'cron',
                   hour="21-23,0-2",
                   month="1-12",
                   minute="*",
                   day_of_week="mon-fri",
                   second='*/3',args=[code])
    sched1.start()
    #

    # # while 1:
    #     now=datetime.datetime.now().second
    #     if (now==59):
    #         print(now)
            # real_time(code)
    # pools.close()