# -*- encoding: utf-8 -*-
"""
@File    : macd_inverse_bak.py
@Time    : 2021/7/20 18:26
@Author  : handsomejerry
@Email   : 834235185@qq.com
@Software: PyCharm
"""
import copy
import json
import sys

import dolphindb as ddb
import pandas as pd
import numpy as np
from apscheduler.schedulers.blocking import BlockingScheduler

from myself.mystock_final_now.mystock_final_now.futures_get.sendmsg import sen_email
# from myself.mystock_final_now.mystock_final_now.配对交易.日度级别.xgboostclf import load_xgboost
# from myself.mystock_final_now.laptop.futures_get.sendmsg import sen_email


def pre_con():
    q='''
    login(`admin,`123456)
db=database('dfs://jerry');
futures=loadTable(db,`futures)
use ta;
all2=select  *  from futures where 日期<=2021.12.21  and 当前时间>=2021.09.16 10:00:00 and 种名简称  in `鸡蛋`甲醇`铝`锌  // order by 当前时间 desc 
'''
    # print(q)
    con.run(q)

def run(st,ed):
    global first
    q='''
       tq=select top 100000000 * ,move(最新价,1) as `前收盘价 , (ta::adx( 最高价,最低价,最新价,14 )-move(ta::adx( 最高价,最低价,最新价,14 ),1)) as `adx_diif , ta::aroon(最高价,最低价, 7)[0] as `aroondown,
    ta::aroon(最高价,最低价, 7)[1] as `aroonup , ta::cmo(ta::ma(最新价, 3, 0),14) as `CMO,ta::macd(最新价,12,26,5)[0] as `diif,ta::macd(最新价,12,26,5)[1] as `dea,ta::macd(最新价,12,26,5)[2] as `macd, ta::ad(最高价,最低价,最新价,成交量) as `AD,move( ta::ad(最高价,最低价,最新价,成交量),1) as `前AD , ta::obv(最新价,成交量) as `obv , move(ta::obv(最新价,成交量),1) as `前obv,ta::rsi(最新价,14) as `rsi ,ta::mom(最新价,5) as `mom, ta::bBands(最新价,5,2,2,0)[0] as `upper,ta::bBands(最新价,5,2,2,0)[1] as `middle ,ta::bBands(最新价,5,2,2,0)[2] as `lower  ,move(ta::mom(最新价,5),1) `前mom   ,       move(最新价,-1) as `后1 ,move(最新价,-2) as `后2 , move(最新价,-3) as `后3   ,   moving(first,最新价 ,12 )  as `前一 ,   moving(last,最新价 ,12 )  as `后一  ,   moving(first,ta::macd(最新价,12,26,5)[2] ,6 )  as `macd前一   ,    moving(last,ta::macd(最新价,12,26,5)[2] ,6 )  as `macd后一  ,  moving(median ,最新价,6) as `前价    from all2 context by 合约名称 csort 当前时间 asc
        nullFill!(tq,0)
        
    signals= select 当前时间, 合约名称  ,最新价 ,  add( (  macd后一/ macd前一 ) , -1 )  as `macd速度  ,  add ( (  后一/前一 ) ,  -1  )as `price速度 ,  前价  from tq 

    signals=select * from signals where price速度>mean(price速度) and macd速度<mean(macd速度)
            '''.format(st=st,ed=ed)
    # print(q)
    con.run(q)


    q='''select top 1 * , iif( 最新价>前价,`sell, iif(  最新价<前价 ,  `buy , `TODO  )     )  as `操作   from signals where ( price速度>0 and macd速度<0 ) or ( price速度<0 and macd速度>0 ) context by 合约名称 csort 当前时间 desc
    
	'''

    nowvalue='''
    select top 1 当前时间,合约名称,最新价 from tq context by 合约名称 csort 当前时间 desc
    '''
    nowvalue=con.run(nowvalue)
    nowvalue=nowvalue.to_dict('record')
    res=con.run(q)
    # res=res[res['时间']>=ed.replace('.','-')]
    if res[res['操作']!='TODO'].empty:
        if first:
            [handstate.update({i.get("合约名称"):{'buy_position':0,
                                              "sell_position":0,
                                              # "price":i.get('最新价')
                                              }}) for i in nowvalue]
            [spread.update({i.get("合约名称"):0}) for i in nowvalue]
            first=False
        return None,nowvalue

    # elif (res.iloc[0,-1] == 'TODO') and ( res.iloc[-1,-1] == 'TODO' ) :
    #     return None
    else:
        res['当前时间']=res['当前时间'].apply(lambda x:x.strftime("%Y-%m-%d %H:%M:%S"))
        res=res[['当前时间','合约名称','最新价','操作']].to_dict('record')
        # return json.dumps(res,ensure_ascii=False)
        if first:
            [handstate.update({i.get("合约名称"):{'buy_position':0,
                                              "sell_position":0,
                                              # "price":i.get('最新价')
                                              }}) for i in nowvalue]
            [spread.update({i.get("合约名称"):0}) for i in nowvalue]
            first=False
        return res,nowvalue

def buy(contractname,currentprice,flag='close'):
    if flag=='close':
        # handstate[contractname]['buy_position']+=1
        # handstate[contractname]['buy_price']=currentprice
        spread[contractname]-=currentprice*handstate[contractname]['sell_position']

        # [handstate.update({i.get("合约名称"):{'buy_position':0,
        #                                   "sell_position":0,
        #                                   # "price":i.get('最新价')
        #                                   }}) for i in res]
        handstate[contractname]={'buy_position':0,"sell_position":0}

    else:
        handstate[contractname]['buy_position']+=1
        # handstate[contractname]['buy_price']=0
        try:
            handstate[contractname]['buy_price'].append( currentprice )
        except:
            handstate[contractname]['buy_price']=[currentprice]
        # handstate[contractname]['buy_price']/=handstate[contractname]['buy_position']
        spread[contractname]-=currentprice*1


def sell(contractname,currentprice,flag='close'):
    if flag=='close':
        # handstate[contractname]['sell_position']=0
        # handstate[contractname]['sell_price']=currentprice
        spread[contractname]+=currentprice*handstate[contractname]['buy_position']
        # handstate[contractname]['buy_position']=0

        handstate[contractname]={'buy_position':0,
                                 "sell_position":0}

    else:
        handstate[contractname]['sell_position']+=1
        try:
            handstate[contractname]['sell_price'].append(currentprice)
        except:
            handstate[contractname]['sell_price']=[currentprice]
        # handstate[contractname]['sell_price']+=currentprice
        # handstate[contractname]['sell_price']/=handstate[contractname]['sell_position']
        spread[contractname]+=currentprice*1


def finding_2_trade(contractname,currentprice):

    if handstate[contractname]['sell_position']:
        if currentprice< np.mean(handstate[contractname]['sell_price'][-2:]):
            buy(contractname=contractname,currentprice=currentprice,flag='close')
            # sen_email(msg_to='834235185@qq.com',content=str(c))

    if handstate[contractname]['buy_position']:
        # weitged_price=
        if currentprice> np.mean(handstate[contractname]['buy_price'][-2:]):
            sell(contractname=contractname,currentprice=currentprice,flag='close')





def main():
    global last,lastspread,email
    pre_con()
    res,nowvalue=run(st='2021.08.27',ed='2021.09.10')
    # print(last,res)
    list(map(lambda i:finding_2_trade(contractname=i.get('合约名称'),currentprice=i.get('最新价')),nowvalue))
    # [finding_2_trade(contractname=i.get('合约名称'),currentprice=i.get('最新价')) for i in nowvalue]

    # if lastspread!=spread:
    #     print(spread)
    # lastspread=spread

    if res:
        flagres=copy.deepcopy(res)
        flagres=np.array([f'{i.get("合约名称")}:{i.get("当前时间")}' for i in flagres])
        # print(last)
        #print(flagres!=last)
        if np.mean(last!=flagres)  :
            c=flagres[last!=flagres]
            c=[i.split(':')[0] for i in c]
            # print(c)
            for i in filter(lambda x:x.get('合约名称') in c,res):
                if i.get('操作') == 'TODO':
                    continue
                elif i.get('操作') == 'buy':
                    # if handstate[i.get('合约名称')]['sell_position']:
                    #     # spread[i.get('合约名称')]+=(i.get('最新价')-handstate[i.get('合约名称')]['buy_price'])*handstate[i.get('合约名称')]['buy_position']
                    #     if i.get('最新价')>handstate[i.get('合约名称')]['sell_price']:
                    #         continue
                    #     buy(contractname=i.get('合约名称'),currentprice=i.get('最新价'),flag='close')
                    #     # if handstate[i.get('合约名称')]['buy_position']==0:
                    #     #     print(f"clear positions {i.get('合约名称')}",spread[i.get('合约名称')])
                    # else:
                    if spread[i.get('合约名称')]<=maxloss:
                        continue
                    elif handstate[i.get('合约名称')]['sell_position']:
                        # sell(contractname=i.get('合约名称'),currentprice=i.get('最新价'),flag='close')

                        continue

                    buy(contractname=i.get('合约名称'),currentprice=i.get('最新价'),flag='open')
                    email=True
                    # handstate[i.get('合约名称')]['buy_position']+=1
                    #     handstate[i.get('合约名称')]['buy_price']=i.get('最新价')
                    #     spread[i.get('合约名称')]-=i.get('最新价')*1

                elif i.get('操作') == 'sell':
                    #
                    # if handstate[i.get('合约名称')]['buy_position']:
                    #     if i.get('最新价')<handstate[i.get('合约名称')]['buy_price']:
                    #         continue
                    #     # # spread[i.get('合约名称')]+=(i.get('最新价')-handstate[i.get('合约名称')]['buy_price'])*handstate[i.get('合约名称')]['buy_position']
                    #     # handstate[i.get('合约名称')]['sell_position']+=1
                    #     # handstate[i.get('合约名称')]['sell_price']=i.get('最新价')
                    #     # spread[i.get('合约名称')]+=i.get('最新价')*handstate[i.get('合约名称')]['buy_position']
                    #     # handstate[i.get('合约名称')]['buy_position']=0
                    #     sell(contractname=i.get('合约名称'),currentprice=i.get('最新价'),flag='close')
                    #
                    #     # if handstate[i.get('合约名称')]['sell_position']==0:
                    #     #     print(f"clear positions {i.get('合约名称')}",spread[i.get('合约名称')])
                    # else:
                    #     # if spread[i.get('合约名称')]<=maxloss:
                    #     #     continue
                    #     # handstate[i.get('合约名称')]['sell_position']+=1
                    #     # handstate[i.get('合约名称')]['sell_price']=i.get('最新价')
                    #     # spread[i.get('合约名称')]+=i.get('最新价')*1
                    if handstate[i.get('合约名称')]['buy_position']:
                        # buy(contractname=i.get('合约名称'),currentprice=i.get('最新价'),flag='close')
                        continue
                    sell(contractname=i.get('合约名称'),currentprice=i.get('最新价'),flag='open')
                    email=True


            res.append(spread)
            res.append(handstate)
            res=json.dumps(res,ensure_ascii=False)
            c=f'''以下合约加时间点触发了反向交易信号  :{res}'''
            # print(res)
            print(handstate)
            print(spread)
            if email:
                # sen_email(msg_to='834235185@qq.com',content=str(c))
                # sen_email(msg_to='1922335454@qq.com,yangjiarui0032021@163.com',content=c)
                # sen_email(msg_to='1922335454@qq.com',content=c)
                email=False

        last=flagres
    else:
        pass
        # sys.exit(0)

    # sen_email(msg_to='834235185@qq.com')



if __name__ == '__main__':
    # pre_con()
    con=ddb.session()
    # con.connect('192.168.1.7',8922,'admin','123456')
    email=False
    first=True
    spread={}
    maxloss=-10000
    handstate={
        # "buy_position":0,
        # "sell_position":0
    }
    con.connect('localhost',8922,'admin','123456')

    last =np.array(None)
    lastspread={}
    # main()

    sched1=BlockingScheduler()
    #     # real_time(code)
    #
    #
    sched1.add_job(main,'interval',seconds=3)
    sched1.start()







