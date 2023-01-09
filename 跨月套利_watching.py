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
from queue import Queue
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
all=select  *, move(成交量,1),move(持仓量,1),move(最新价,-4) as `后4  ,int(当前时间),最新价 from futures where 当前时间>={st} and 当前时间 <={ed}  and 种名简称 = 品种名 and  (  insrtument.count()==0 || 合约名称 in insrtument ) order by 当前时间 asc
	update all set 成交量=( 成交量 - move_成交量 ) , 持仓量=(  持仓量-move_持仓量) 
return select 合约名称,当前时间,开盘价,最高价,最低价,昨日收盘价,买价,卖价,最新价,结算价,昨结算,买量,卖量,持仓量,成交量,商品交易所简称,种名简称,日期,后4 from all
}}
  addFunctionView(pocess_ins_vol);
all2=pocess_ins_vol(futures,ins[0])      //select  * from futures where 日期<=2021.12.21  and 当前时间>={st} and 当前时间 <={ed} and 种名简称 = ins[0]
def mergetables( futures,mutable f ,lis ){{
	for ( i in lis){{	
i=pocess_ins_vol(futures,i)               //select  * from futures where 日期<=2021.12.21  and 当前时间>={st} and 当前时间 <={ed} and 种名简称 = i
		f.append!(i)
		}}
	}}
mer=mergetables{{ futures,all2}}
ploop(mer, ins[1:]  )
//nowvalue=all2
allt=select *, substr( string(当前时间),0,16) as `min  from all2
allt=select avg(最新价) as `最新价 from allt group by 合约名称,min
allt=select 最新价 from allt  pivot by min as `当前时间,合约名称
allt.nullFill!(0)
//allt=select * from allt order by 当前时间 asc
allt.colNames()
'''.format(st=st.strftime("%Y.%m.%d %H:%M:%S"),ed=ed.strftime("%Y.%m.%d %H:%M:%S"))
    if showsql:
        print(sql)
    ccols=con.run(sql)
    return st,ed,ccols

def run(st,ed,cols):
    global first
    # cols=con.run('''''')
    if not len(cols)<=3:
        return
    q='''
  allt=select * from allt where {contract1}>0 and {contract2}>0 
pair=select last(moving(mean,{contract1},{look_pre})) as `avg_{contract1} ,last(moving(mean,{contract2},{look_pre})) as `avg_{contract2} from allt
n=allt.当前时间.last()+':00'
//flag=1
full=table(10000000:5, `当前时间`合约名称`最新价`操作, `TIMESTAMP`SYMBOL`DOUBLE`SYMBOL)//;(当前时间, 合约名称,最新价,方程值,方向 )
if ( (pair[`avg_{contract1}]>pair[`avg_{contract2}]) and (flag==1)   ){{  
	full.append!(  table([[timestamp(n)],[`{contract1}],[0],["sell"]   ]) ) ;
             full.append!(  table([[timestamp(n)],[`{contract2}],[0],["buy"]   ]) )
             }}else if( ( pair[`avg_{contract1}]<pair[`avg_{contract2}] ) and ( flag==1)   ){{
             	full.append!(  table([[ timestamp(n)  ],[`{contract1}],[0],["buy"]   ]) ) ;
             full.append!(  table([[ timestamp(n) ],[`{contract2}],[0],["sell"]   ]) )
             }};
 full=select * from full where 当前时间>= (now()+1000*3600*8-30*60*1000)
  //if ( lasttable.values().eqObj(full.values()) ){{ }}else{{ flag=1;print full; }}
'''.format(contract1=cols[1],contract2=cols[2],look_pre=3)
    if showsql:
        print(q)
    con.run(q)
    if con.run('lasttable.values().eqObj(full.values())'):
        con.run('lasttable=full.copy()')#.iloc[-1,:])
        return
    else:
        res=con.run('full')
        con.run('lasttable=full.copy()')#.iloc[-1,:])
    print('res',res)
    # print('currentdiff',con.run('currentdiff'))
    nowvalue='''
    select top 1 当前时间,合约名称,最新价 from all2 context by 合约名称 csort 当前时间 desc
    '''
    if showsql:
        print(nowvalue)
    if first:
        nowvalue=con.run(nowvalue)
        nowvalue=nowvalue.to_dict('record')
    # print(res)
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
        res=res[['当前时间','合约名称','最新价','操作']].to_dict('record')
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

# 2021.10.22 06:12:04 and 当前时间 <=2021.10.22 16:14:19
    st,ed,cols=pre_con(st=(now-datetime.timedelta(days=5,hours=12,minutes=0,seconds=135)),ed=(now+datetime.timedelta(hours=0,minutes=0)))
    try:
        res,nowvalue,xgb=run(st,ed,cols)
    except:
        return
    # handstate[contractname]['upper_count']
    # print(last,res)
    # list(map(lambda i:finding_2_trade(contractname=i.get('合约名称'),currentprice=i.get('最新价')),nowvalue))
    # print(res)
    if res:
        now=datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        handstate.update({"time":now})
        spread.update({"time":now,"当前价差":futures_base.currentdiff})
        ####记录状态
        pathlog=os.path.join(os.getcwd(),'futures_base')
        pathlog=os.path.join(pathlog,'log')
        with open(os.path.join(pathlog,'trading.json'),'a',encoding='utf-8') as wd:
            json.dump([handstate,spread],wd,ensure_ascii=False)
            wd.write('\n')

    flagres=copy.deepcopy(res)
    if not flagres:
        return
    flagres=np.array([f'{i.get("合约名称")}:{i.get("当前时间")}' for i in flagres])
    # print(flagres).
    try:
        last=last.tolist()
    except:
        pass
    if ( (len(last) if last else []) != len(flagres) ) or any(last!=flagres ) :
        if len(last)<=1:
            c=flagres[last!=flagres]
            c=[i.split(':')[0] for i in c[0]]
        else:
            c=[i.split(':')[0] for i in flagres]
        # print(c)
        # _=qu.get()
        # _.get('配对跨月交易是否在进行交易')
        futures_base.配对跨月交易flag=False
        spread['价差info']=[]
        for i in filter(lambda x:x.get('合约名称') in c,res):
            当前实际价格=lastprice.get(i.get('合约名称'))
            if i.get('操作') == 'TODO':
                continue
            elif i.get('操作') == 'buy':
                # xgb_res=filter(lambda x:x.get('code')==i.get('合约名称'),xgb).__next__()
                # if xgb_res['flag']==0 and float(xgb_res['score'])>0.65:
                #     continue
                if spread[i.get('合约名称')]<=maxloss:
                    continue
                if handstate[i.get('合约名称')]['sell_position'] or handstate[i.get('合约名称')]['buy_position']:
                    # buy(contractname=i.get('合约名称'),currentprice=当前实际价格-卖滑点,flag='close')
                    continue
                # if xgb_res['flag']==1 and float(xgb_res['score'])<0.35:
                #     continue
                # try:
                #     if handstate.get(i.get('合约名称')).get('buy_price')[-1] < (lastprice.get(i.get('合约名称'))+买滑点):
                #         continue
                # except:
                #     pass
                # futures_base.xgb_res=xgb_res
                # buy(contractname=i.get('合约名称'),currentprice=i.get('最新价'),flag='open')
                # if (lastprice.get(i.get('合约名称')) > i['最高']):
                #     continue
                # if handstate.get(list(handstate.keys())[0]).get('buy_position')==handstate.get(list(handstate.keys())[0]).get('sell_position'):
                #     # con.upload({'currentdiff':'cas'})
                #     good=sum(list(spread.values())[:2])-2*初始金额

                # con.upload({"sellclose":i.get('合约名称')})
                # spread['价差info'].append((i.get('合约名称'),当前实际价格+买滑点))
                pbuy(contractname=i.get('合约名称'),currentprice=当前实际价格+买滑点,flag='open')
                email=True
            elif i.get('操作') == 'sell':
                # xgb_res=filter(lambda x:x.get('code')==i.get('合约名称'),xgb).__next__()
                # futures_base.xgb_res=xgb_res
                if handstate[i.get('合约名称')]['buy_position'] or handstate[i.get('合约名称')]['sell_position']:
                    # sell(contractname=i.get('合约名称'),currentprice=当前实际价格-卖滑点,flag='close')
                    continue
                # if xgb_res['flag']==1 and float(xgb_res['score'])>0.65:
                #     continue
                # if xgb_res['flag']==0 and float(xgb_res['score'])<0.45:
                #     continue
                # sell(contractname=i.get('合约名称'),currentprice=i.get('最新价'),flag='open')
                # try:
                #     if handstate.get(i.get('合约名称')).get('sell_price')[-1] > (lastprice.get(i.get('合约名称'))-卖滑点):
                #         continue
                #     # continue
                # except:
                #     pass
                # if  (lastprice.get(i.get('合约名称')) < i['最低']):
                #     continue
                ###当前pairprice记录
                # con.upload({"buyclose":i.get('合约名称')})
                # spread['价差info'].append((i.get('合约名称'),当前实际价格-卖滑点))
                psell(contractname=i.get('合约名称'),currentprice=当前实际价格-卖滑点,flag='open')
                email=True
        # try:
        #     价差=spread['价差info'][-1][-1]-spread['价差info'][0][-1]
        #     futures_base.currentdiff=abs(价差)
        #     # con.upload({"currentdiff":abs(价差)})
        # except:
        #     pass
        futures_base.配对跨月交易flag=True
        res.append(spread)
        res.append(handstate)
        res.append(xgb)
        email_content=json.dumps(res,ensure_ascii=False)
        c=f'''以下合约加时间点触发了反向交易信号  :{email_content}'''

        # print(handstate)
        # print(spread)
        # print(sum(list(spread.values())[:2])-2*初始金额)
        # print('\n')
        if email:
            # sen_email(msg_to='834235185@qq.com',content=str(c))
            # sen_email(msg_to='1922335454@qq.com,yangjiarui0032021@163.com',content=c)
            # sen_email(msg_to='1922335454@qq.com',content=c)
            email=False
        last=np.array( flagres)
    else:
        pass
        # sys.exit(0)
    #显示实时结果
    print(handstate)
    print(spread)
    # if handstate.get(list(handstate.keys())[0]).get('buy_position')==handstate.get(list(handstate.keys())[0]).get('sell_position'):
    #     # con.upload({'currentdiff':'cas'})
    #     print('盈利了:',sum(list(spread.values())[:2])-2*初始金额)
    # print('\n')

    # sen_email(msg_to='834235185@qq.com')

if __name__ == '__main__':
    # lastprice={}
    con=ddb.session()
    showsql=False
    # qu.put({'配对跨月交易是否在进行交易':True})
#     # con.fconnect('localhost',8922,'admin','123456')
#     con.connect('server.natappfree.cc',38815,'admin','123456')
    con.connect(futures_base.DBhost,futures_base.DBport2,'admin','123456')
    con.upload({"ins":futures_base.ins,
                "lasttable":pd.DataFrame({"sa":[1,2],"swd":[3,1]}),
                "flag":1
                # 'buyclose':'32','sellclose':'32'
                })
    ###订阅CTP行情
    threading.Thread(target=CTPmarketfetch).start()
    time.sleep(10)
    # print('cdscdsc')
# t.daemon = Truelasttablke
    ##实时寻找平仓机会
    threading.Thread(target=finding_2_trade_pair).start()
# 'server.natappfree.cc',38057
# #
    sched1=BlockingScheduler()
    time.sleep(3)
    sched1.add_job(main,'interval',seconds=8)
    try:
        sched1.start()
    except Exception as e:
        pass







