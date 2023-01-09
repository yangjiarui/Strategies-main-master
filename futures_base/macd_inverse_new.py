# -*- encoding: utf-8 -*-
"""
@File    : macd_inverse_bak.py
@Time    : 2021/7/20 18:26
@Author  : handsomejerry
@Email   : 834235185@qq.com
@Software: PyCharm
"""
import copy,futures_base
import datetime
import json
import sys

import dolphindb as ddb
import pandas as pd
import numpy as np
from apscheduler.schedulers.blocking import BlockingScheduler

# from myself.mystock_final_now.mystock_final_now.futures_get.sendmsg import sen_email
# from myself.mystock_final_now.mystock_final_now.配对交易.日度级别.xgboostclf import load_xgboost
# from myself.mystock_final_now.laptop.futures_get.sendmsg import sen_email
# from myself.mystock_final_now.mystock_final_now.futures_get.xgboostclf import load_xgboost
# from mystock_final_now.mystock_final_now.futures_get.sendmsg import sen_email
# from mystock_final_now.mystock_final_now.futures_get.xgboostclf import load_xgboost
import warnings

from sendmsg import sen_email
from xgboostclf import load_xgboost

from exceutor import buy,sell

warnings.filterwarnings('ignore')


# con.connect('localhost',8922,'admin','123456')

last =np.array(None)
lastspread={}
# main()


def pre_con(st,ed):
    # global st,ed
    q='''
    login(`admin,`123456)
db=database('dfs://jerry');
futures=loadTable(db,`futures)
use ta;
all2=select  *  from futures where 日期<=2021.12.21  and 当前时间>={st} and 当前时间 <={ed} and 种名简称  in `甲醇`铝`沪锌`玉米`豆粕  // order by 当前时间 desc 
'''.format(st=st.strftime("%Y.%m.%d %H:%M:%S"),ed=ed.strftime("%Y.%m.%d %H:%M:%S"))
    # print(q)
    con.run(q)
    return st,ed

def run(st,ed):
    global first
    q='''
       tq=select top 100000000 * ,move(最新价,1) as `前收盘价 , (ta::adx( 最高价,最低价,最新价,14 )-move(ta::adx( 最高价,最低价,最新价,14 ),1)) as `adx_diif , ta::aroon(最高价,最低价, 7)[0] as `aroondown,
    ta::aroon(最高价,最低价, 7)[1] as `aroonup , ta::cmo(ta::ma(最新价, 3, 0),14) as `CMO,ta::macd(最新价,12,26,5)[0] as `diif,ta::macd(最新价,12,26,5)[1] as `dea,ta::macd(最新价,12,26,5)[2] as `macd, ta::ad(最高价,最低价,最新价,成交量) as `AD,move( ta::ad(最高价,最低价,最新价,成交量),1) as `前AD , ta::obv(最新价,成交量) as `obv , move(ta::obv(最新价,成交量),1) as `前obv,ta::rsi(最新价,14) as `rsi ,ta::mom(最新价,5) as `mom, ta::bBands(最新价,5,2,2,0)[0] as `upper,ta::bBands(最新价,5,2,2,0)[1] as `middle ,ta::bBands(最新价,5,2,2,0)[2] as `lower  ,move(ta::mom(最新价,5),1) `前mom   ,       move(最新价,-1) as `后1 ,move(最新价,-2) as `后2 , move(最新价,-3) as `后3   ,   moving(first,最新价 ,12 )  as `前一 ,   moving(last,最新价 ,12 )  as `后一  ,   moving(first,ta::macd(最新价,12,26,5)[2] ,6 )  as `macd前一   ,    moving(last,ta::macd(最新价,12,26,5)[2] ,6 )  as `macd后一  ,  moving(median ,最新价,6) as `前价    from all2 context by 合约名称 csort 当前时间 asc
        nullFill!(tq,0)
        
    signals= select 当前时间, 合约名称  ,最新价 ,  add( (  macd后一/ macd前一 ) , -1 )  as `macd速度  ,  add ( (  后一/前一 ) ,  -1  )as `price速度 ,  前价  from tq 

    signals=select * from signals where price速度>mean(price速度) and macd速度<mean(macd速度)
            '''
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
    ###集成了xgboost算法
    xgb=load_xgboost(con=con,allcode=res['合约名称'].to_list(),st=(ed-datetime.timedelta(seconds=5*60)),ed=ed)
    xgb['time']=xgb['time'].apply(lambda x:x.strftime("%Y-%m-%d %H:%M:%S"))
    if res[res['操作']!='TODO'].empty:
        if first:
            [handstate.update({i.get("合约名称"):{'buy_position':0,
                                              "sell_position":0,
                                              # "price":i.get('最新价')
                                              }}) for i in nowvalue]
            [spread.update({i.get("合约名称"):初始金额}) for i in nowvalue]
            first=False


        return None,nowvalue,xgb.to_dict('record')

    else:
        res['当前时间']=res['当前时间'].apply(lambda x:x.strftime("%Y-%m-%d %H:%M:%S"))
        res=res[['当前时间','合约名称','最新价','操作']].to_dict('record')
        if first:
            [handstate.update({i.get("合约名称"):{'buy_position':0,
                                          "sell_position":0,
                                          # "price":i.get('最新价')
                                              }}) for i in nowvalue]
            [spread.update({i.get("合约名称"):初始金额}) for i in nowvalue]
            first=False
        return res,nowvalue,xgb.to_dict('record')





def main():
    global last,lastspread,email
    now=datetime.datetime.now()
    st,ed=pre_con(st=(now-datetime.timedelta(days=2)),ed=(now-datetime.timedelta(hours=21)))
    ###macd_inverse
    res,nowvalue,xgb=run(st,ed)
    # handstate[contractname]['upper_count']
    # print(last,res)
    # list(map(lambda i:finding_2_trade(contractname=i.get('合约名称'),currentprice=i.get('最新价')),nowvalue))

    if res:
        flagres=copy.deepcopy(res)
        flagres=np.array([f'{i.get("合约名称")}:{i.get("当前时间")}' for i in flagres])
        if np.mean(last!=flagres)  :
            c=flagres[last!=flagres]
            c=[i.split(':')[0] for i in c]
            for i in filter(lambda x:x.get('合约名称') in c,res):
                if i.get('操作') == 'TODO':
                    continue
                elif i.get('操作') == 'buy':
                    xgb_res=filter(lambda x:x.get('code')==i.get('合约名称'),xgb).__next__()

                    if xgb_res['flag']==0 and float(xgb_res['score'])>0.75:
                        continue
                    if spread[i.get('合约名称')]<=maxloss:
                        continue
                    if handstate[i.get('合约名称')]['sell_position']:
                        continue
                    if xgb_res['flag']==1 and float(xgb_res['score'])<0.25:
                        continue

                    buy(contractname=i.get('合约名称'),currentprice=i.get('最新价'),flag='open')
                    email=True

                elif i.get('操作') == 'sell':
                    xgb_res=filter(lambda x:x.get('code')==i.get('合约名称'),xgb).__next__()

                    if handstate[i.get('合约名称')]['buy_position']:
                        continue
                    if xgb_res['flag']==1 and float(xgb_res['score'])>0.75:
                        continue
                    if xgb_res['flag']==0 and float(xgb_res['score'])<0.25:
                        continue

                    sell(contractname=i.get('合约名称'),currentprice=i.get('最新价'),flag='open')
                    email=True

            res.append(spread)
            res.append(handstate)
            res.append(xgb)
            res=json.dumps(res,ensure_ascii=False)
            c=f'''以下合约加时间点触发了反向交易信号  :{res}'''

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
    con=ddb.session()
    # con.connect('192.168.1.7',8922,'admin','123456')
    con.connect('server.natappfree.cc',46191,'admin','123456')

#
    sched1=BlockingScheduler()
#     # real_time(code)
#
#
    sched1.add_job(main,'interval',seconds=3)
    try:
        sched1.start()
    except Exception as e:
        pass







