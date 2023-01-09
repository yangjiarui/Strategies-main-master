# -*- encoding: utf-8 -*-
"""
@File    : macd_inverse_bak.py
@Time    : 2021/7/20 18:26
@Author  : handsomejerry
@Email   : 834235185@qq.com
@Software: PyCharm
"""
import copy
import datetime
import json
import os.path
import sys
import time
from pprint import pprint

import dolphindb as ddb
import pandas as pd
import numpy as np
from apscheduler.schedulers.blocking import BlockingScheduler
from futures_base import *
import futures_base
import warnings,threading
warnings.filterwarnings('ignore')
# con.connect('localhost',8922,'admin','123456')
# main()


def pre_con(st,ed):
    # global st,ed
    sql='''
        login(`admin,`123456)
db=database('dfs://jerry');
futures=loadTable(db,`futures)
use ta;
if((exec count(*) from getFunctionViews() where name="pocess_ins_vol")==1){{  dropFunctionView("pocess_ins_vol")  }};
go;
    def pocess_ins_vol(futures,mutable insrtuments){{ 
    ix=0
endindex=insrtuments.regexFind("[0-9]")
if (endindex==-1){{
	品种名=insrtuments.lower()
	insrtuments=''
	}}else{{
品种名=insrtuments[0:endindex].lower()
	}}
insrtument= distinct(trim(split(insrtuments,',')));   if(insrtuments.count()>0 and insrtuments.last()=="") insrtuments.pop!();
all=select  *, move(成交量,1),move(持仓量,1),move(买量,1),move(卖量,1) from futures where 当前时间>={st} and 当前时间 <={ed}  and 种名简称 = 品种名 and  (  insrtument.count()==0 || 合约名称 in insrtument ) 
return all
}}
  addFunctionView(pocess_ins_vol);
'''.format(st=st.strftime("%Y.%m.%d %H:%M:%S"),ed=ed.strftime("%Y.%m.%d %H:%M:%S"))
    if showsql:
        print(sql)
    con.run(sql)
    q='''
all2=pocess_ins_vol(futures,ins[0])      //select  * from futures where 日期<=2021.12.21  and 当前时间>={st} and 当前时间 <={ed} and 种名简称 = ins[0]
def mergetables( futures,mutable f ,lis ){
	for ( i in lis){	
i=pocess_ins_vol(futures,i)               //select  * from futures where 日期<=2021.12.21  and 当前时间>={st} and 当前时间 <={ed} and 种名简称 = i
		f.append!(i)
		}
	}
mer=mergetables{ futures,all2}
ploop(mer, ins[1:]  )
//all2
//all2=select  *  from futures where 日期<=2021.12.21  and 当前时间>={st} and 当前时间 <={ed} and 种名简称  in 
//`jd`rb`zn`au`hc`rb`jd`al//`甲醇`铝`沪锌`玉米`豆粕  // order by 当前时间 desc 
'''
    if showsql:
        print(q)
    con.run(q)
    return st,ed

def run(st,ed):
    global first
    q='''
           tq=select top 100000000 *,( 成交量 - move_成交量 ) ,(  持仓量-move_持仓量),( 买量-move_买量 ) - ( 卖量-move_卖量  ) as `delta净买量 ,
 moving(mean,最高价,4) as `最高, moving(mean,最低价,4) as `最低 ,   moving(mean,( 买量-move_买量 ) - ( 卖量-move_卖量  ),4)  ,        
       moving(std,( 买量-move_买量 ) - ( 卖量-move_卖量  ),4)  from all2 context by 合约名称 csort 当前时间 asc
   nullFill!(tq,0)

     signals= select 当前时间, 合约名称,最高,最低 ,最新价 , iif(  持仓量_sub>0, iif(delta净买量>0,'buy'  , iif( delta净买量<0,'sell','没有意义' )  )   ,"没有意义") as `操作
   from tq where delta净买量>=moving_mean+0.85*moving_std or delta净买量<=moving_mean-0.85*moving_std 
            '''
    if showsql:
        print(q)
    con.run(q)

    ####记得实际交易时  ::   and 当前时间>= (now()+1000*3600*8-1000*20)
    q='''select top 1 * from signals where 操作 != '没有意义' and 当前时间>= (now()+1000*3600*8-1000*3)  context by 合约名称 csort 当前时间 desc  
	'''
    if showsql:
        print(q)

    nowvalue='''
    select top 1 当前时间,合约名称,最新价 from tq context by 合约名称 csort 当前时间 desc
    '''
    if showsql:
        print(nowvalue)
    if first:
        nowvalue=con.run(nowvalue)
        nowvalue=nowvalue.to_dict('record')
    res=con.run(q)
    ###集成了xgboost算法
    xgb=load_xgboost(con=con,allcode=res['合约名称'].to_list(),st=(ed-datetime.timedelta(seconds=1.2*60)),ed=ed)
    # print(xgb)
    xgb['time']=xgb['time'].apply(lambda x:x.strftime("%Y-%m-%d %H:%M:%S"))
    if res[res['操作']!='TODO'].empty:
        if first:
            [handstate.update({i.get("合约名称"):{'buy_position':0,
                                              "sell_position":0,
                                              # "price":i.get('最新价')
                                              }}) for i in nowvalue]
            [spread.update({i.get("合约名称"):初始金额}) for i in nowvalue]
            spread.update(futures_base.res[-1]) if futures_base.res[-1] else print("ok")
            handstate.update(futures_base.res[0]) if futures_base.res[0] else print("ok")
            first=False
        return None,nowvalue,xgb.to_dict('record')

    else:
        res['当前时间']=res['当前时间'].apply(lambda x:x.strftime("%Y-%m-%d %H:%M:%S"))
        res=res[['当前时间','合约名称','最新价','操作','最高','最低']].to_dict('record')
        if first:
            [handstate.update({i.get("合约名称"):{'buy_position':0,
                                          "sell_position":0,
                                          # "price":i.get('最新价')
                                              }}) for i in nowvalue]
            [spread.update({i.get("合约名称"):初始金额}) for i in nowvalue]
            spread.update(futures_base.res[-1]) if futures_base.res[-1] else print("ok")
            handstate.update(futures_base.res[0]) if futures_base.res[0] else print("ok")
            first=False
        return res,nowvalue,xgb.to_dict('record')





def main():
    global last,lastspread,email
    now=datetime.datetime.now()
    # ins=['i', 'bb', 'v', 'wr', 'LR', 'rb', 'eb', 'fu', 'eg', 'jd', 'lu', 'ni', 'WH', 'p', 'c', 'MA', 'pb', 'lh', 'bu', 'SM', 'PF', 'ru', 'OI', 'SA', 'CF', 'UR', 'ss', 'IC', 'CJ', 'JR', 'T', 'bc', 'rr', 'SR', 'y', 'IH', 'hc', 'RM', 'IF', 'AP', 'cs', 'a', 'm', 'RS', 'l', 'j', 'PM', 'sp', 'cu', 'pp', 'nr', 'b', 'TA', 'sc', 'jm', 'SF', 'ag', 'TF', 'sn', 'RI', 'zn', 'FG', 'al', 'TS', 'au', 'CY', 'fb', 'ZC', 'PK', 'pg']
    # ins=['cf209','rm201','ma202','pvc2201',"rb2201","m2201",'fu2201',"zn2201",'p2201']#[2:4]
    # insrtuments=['AP111','zn2205']
    con.upload({"ins":futures_base.ins,
                })
    st,ed=pre_con(st=(now-datetime.timedelta(days=0,hours=1,minutes=50)),ed=(now+datetime.timedelta(hours=0,minutes=0)))
    ###macd_inverse
    res,nowvalue,xgb=run(st,ed)
    # handstate[contractname]['upper_count']
    # print(last,res)
    # list(map(lambda i:finding_2_trade(contractname=i.get('合约名称'),currentprice=i.get('最新价')),nowvalue))
    # print(res)
    if res:
        now=datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        handstate.update({"time":now})
        spread.update({"time":now})
        ####记录状态
        pathlog=os.path.join(os.getcwd(),'futures_base')
        pathlog=os.path.join(pathlog,'log')
        with open(os.path.join(pathlog,'trading_bak.json'),'a',encoding='utf-8') as wd:
            json.dump([handstate,spread],wd,ensure_ascii=False)
            wd.write('\n')

    flagres=copy.deepcopy(res)
    if not flagres:
        return
    flagres=np.array([f'{i.get("合约名称")}:{i.get("当前时间")}' for i in flagres])
    # print(flagres)
    try:
        last=last.tolist()
    except:
        pass
    if ( (len(last) if last else []) != len(flagres) ) or any(last!=flagres ) :
        try:
            c=flagres[last!=flagres]
            c=[i.split(':')[0] for i in c[0]]
        except:
            c=[i.split(':')[0] for i in flagres]
        # print(c)
        for i in filter(lambda x:x.get('合约名称') in c,res):
            if i.get('操作') == 'TODO':
                continue
            elif i.get('操作') == 'buy':
                xgb_res=filter(lambda x:x.get('code')==i.get('合约名称'),xgb).__next__()

                if xgb_res['flag']==0 and float(xgb_res['score'])>0.65:
                    continue
                if spread[i.get('合约名称')]<=maxloss:
                    continue
                if handstate[i.get('合约名称')]['sell_position']:
                    continue
                if xgb_res['flag']==1 and float(xgb_res['score'])<0.35:
                    continue
                try:
                    if handstate.get(i.get('合约名称')).get('buy_price')[-1] < (lastprice.get(i.get('合约名称'))+买滑点):
                        continue
                except:
                    pass
                futures_base.xgb_res=xgb_res
                # buy(contractname=i.get('合约名称'),currentprice=i.get('最新价'),flag='open')
                if (lastprice.get(i.get('合约名称')) > i['最高']):
                    continue
                buy(contractname=i.get('合约名称'),currentprice=lastprice.get(i.get('合约名称'))+买滑点,flag='open')
                email=True

            elif i.get('操作') == 'sell':
                xgb_res=filter(lambda x:x.get('code')==i.get('合约名称'),xgb).__next__()

                if handstate[i.get('合约名称')]['buy_position']:
                    continue
                if xgb_res['flag']==1 and float(xgb_res['score'])>0.65:
                    continue
                if xgb_res['flag']==0 and float(xgb_res['score'])<0.45:
                    continue
                futures_base.xgb_res=xgb_res
                # sell(contractname=i.get('合约名称'),currentprice=i.get('最新价'),flag='open')
                try:
                    if handstate.get(i.get('合约名称')).get('sell_price')[-1] > (lastprice.get(i.get('合约名称'))-卖滑点):
                        continue
                    # continue
                except:
                    pass

                if  (lastprice.get(i.get('合约名称')) < i['最低']):
                    continue
                sell(contractname=i.get('合约名称'),currentprice=lastprice.get(i.get('合约名称'))-卖滑点,flag='open')
                email=True

        res.append(spread)
        res.append(handstate)
        res.append(xgb)

        email_content=json.dumps(res,ensure_ascii=False)
        c=f'''以下合约加时间点触发了反向交易信号  :{email_content}'''

        print(handstate)
        print(spread)
        print('\n')
        if email:
            # sen_email(msg_to='834235185@qq.com',content=str(c))
            # sen_email(msg_to='1922335454@qq.com,yangjiarui0032021@163.com',content=c)
            # sen_email(msg_to='1922335454@qq.com',content=c)
            email=False

        last=np.array( flagres)
    else:
        pass
        # sys.exit(0)

    # sen_email(msg_to='834235185@qq.com')



if __name__ == '__main__':
    # lastprice={}
    con=ddb.session()
    showsql=False
#     # con.fconnect('localhost',8922,'admin','123456')
#     con.connect('server.natappfree.cc',38815,'admin','123456')
    con.connect(futures_base.DBhost,futures_base.DBport2,'admin','123456')
    ###订阅CTP行情
    threading.Thread(target=CTPmarketfetch).start()
    time.sleep(10)
    # print('cdscdsc')
# t.daemon = True
    ###实时寻找平仓机会
    threading.Thread(target=finding_2_trade).start()
# # 'server.natappfree.cc',38057
# #
    sched1=BlockingScheduler()
#
    time.sleep(30)
    sched1.add_job(main,'interval',seconds=10)
    try:
        sched1.start()
    except Exception as e:
        pass







