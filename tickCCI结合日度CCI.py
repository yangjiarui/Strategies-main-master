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
import re
import sys
import time
from pprint import pprint

import dolphindb as ddb
import pandas as pd
import numpy as np
import pypinyin
from apscheduler.schedulers.blocking import BlockingScheduler
from futures_base import *
import futures_base
import warnings,threading

from myself.mystock_final_now.laptop.mystock_final_now.配对交易.日度级别.期货日度CCI_BOL import 获取数据

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
all=select  *,  minute(当前时间) as `到分钟 ,move(成交量,1),move(持仓量,1),move(最新价,-4) as `后4  ,timestamp(当前时间) as `int_当前时间,最新价 from futures where 当前时间>={st} and 当前时间 <={ed}  and 种名简称 = 品种名 and  (  insrtument.count()==0 || 合约名称 in insrtument ) order by 当前时间 asc
	all=select top 1000000 *,move(成交量,1),move(持仓量,1),move(最新价,1) as `前1,move(最新价,-4) as `后4   from all context by 种名简称,合约名称 csort 当前时间
	update all set 成交量=( 成交量 - move_成交量 ) , 持仓量=(  持仓量-move_持仓量) 
//return select top 1 last(合约名称) as `合约名称,last(当前时间) as `当前时间,到分钟,avg(开盘价) as `开盘价,avg(最高价) as `最高价,avg(最低价) as `最低价,avg(买价) as `买价,avg(卖价) as `卖价,avg(最新价) as `最新价,avg(结算价) as `结算价,avg(买量) as `买量,avg(卖量) as `卖量,avg(持仓量) as `持仓量,avg(成交量) as `成交量,last(种名简称) as `种名简称,last(日期) as `日期,last(后4) as `后4,last(int_当前时间) as `int_当前时间 from all context by 日期,合约名称,到分钟 csort 当前时间 asc
return select 合约名称,到分钟,当前时间,开盘价,最高价,最低价,买价,卖价,最新价,结算价,买量,卖量,持仓量,成交量,种名简称,日期,后4,int_当前时间 from all 
}}.
  addFunctionView(pocess_ins_vol);
'''.format(st=st.strftime("%Y.%m.%d %H:%M:%S"),ed=ed.strftime("%Y.%m.%d %H:%M:%S"))
    if showsql:
        print(sql)
    con.run(sql)
    q='''
defg dynimic(x,y){
	ymin=string(y[y.argmin()])
	ymax=string(y[y.argmax()])
	xmin=string(x[y.argmin()])
	xmax=string(x[y.argmax()])
	return xmax+'_'+ymax+'_'+xmin+'_'+ymin
	}
all2=pocess_ins_vol(futures,ins[0])      //select  * from futures where 日期<=2021.12.21  and 当前时间>={st} and 当前时间 <={ed} and 种名简称 = ins[0]
def mergetables( futures,mutable f ,lis ){
	for ( i in lis){	
i=pocess_ins_vol(futures,i)               //select  * from futures where 日期<=2021.12.21  and 当前时间>={st} and 当前时间 <={ed} and 种名简称 = i
		f.append!(i)
		}
	}
mer=mergetables{ futures,all2}
ploop(mer, ins[1:]  )
'''
    if showsql:
        print(q)
    con.run(q)
    return st,ed

def run(st,ed):
    global first
    q='''
      t=lj(all2, lastday_W,`合约名称,`合约)
update t set 收盘价_W =最新价*占比
update t set 开盘价_W =开盘价*占比
update t set 最高价_W =最高价*占比
update t set 最低价_W =最低价*占比
update t set 成交量_W =成交量*占比
update t set 持仓量_W =持仓量*占比
all=select last(当前时间) as `当前时间,sum(收盘价_W) as `收盘价,sum(开盘价_W) as`开盘价,sum(最高价_W)  as`最高价 ,sum(最低价_W)  as`最低价,
sum(成交量_W) as `成交量,sum(持仓量_W) as `持仓量
from t group by 到分钟,种名简称
//select * from all where 收盘价>=(moving(mean,收盘价,8)-1*moving(std,收盘价,5)) and 收盘价<=(moving(mean,收盘价,8)+1*moving(std,收盘价,5))
all=select * from all where 成交量>=mean(成交量)
all
'''
    if showsql:
        print(q)
    now=con.run(q)
    # print(now)
    try:
        now.to_csv('now.csv',encoding='utf-8-sig')
    except Exception as e:
        print(e)
        # pass
    ####记得实际交易时  ::   and 当前时间>= (now()+1000*3600*8-1000*20)
    q='''   
         all=select top 1000000 timestamp(当前时间) as `int_当前时间,ta::cci(最高价,最低价,收盘价,14) as `CCI ,* from all context by 种名简称 csort 当前时间
       tq=select top 1000000 * ,种名简称 as `合约名称 ,NONE as `flag, NONE as `fx ,moving(dynimic,[int_当前时间,CCI],flag ) as `calu , move( moving(dynimic,[int_当前时间,CCI],flag ),1  ) as `diffcalu from all context by 种名简称 csort 当前时间 asc
   nullFill!(tq,0)
       full=table(10000000:5, `当前时间`合约名称`最新价`方程值`方向, `TIMESTAMP`SYMBOL`DOUBLE`DOUBLE`SYMBOL)//;(当前时间, 合约名称,最新价,方程值,方向 )
 for ( i in tq) {
 try{
 	n=i.calu.split('_')
 	l=i.diffcalu.split('_') 
 	//update tq set fx=aw where 当前时间=i.当前时间 and 合约名称=i.合约名称
if(n[3]>l[3]){ //print "下穿则卖出 " 
 	分子=( i.CCI- float(n[3] )  )/( i.int_当前时间- timestamp( n[2]))
 	分母=( i.CCI- float( l[3] )  )/( i.int_当前时间-timestamp( l[2] )) 
		 aw=分子-分母;
		 		 		 if ( aw==NULL ){continue }
//update tq set flag=1 where 当前时间=i.当前时间 and 合约名称=i.合约名称
 full.append!( table([[ i.当前时间 ],[i.合约名称],[i.最新价],[aw],["下穿则卖出"]   ])   )
}
else if(n[1]<l[1]){ //print "上穿则买入" 
 	分子=( i.CCI- float(n[1] )  )/( i.int_当前时间- timestamp( n[0]))
 	分母=( i.CCI- float( l[1] )  )/( i.int_当前时间-timestamp( l[0] )) 
		 aw=分子-分母;
		 		 		 if ( aw==NULL ){continue }
//update tq set flag=2 where 当前时间=i.当前时间 and 合约名称=i.合约名称} 	
 full.append!( table([ [i.当前时间],[i.合约名称],[i.最新价],[aw],["上穿则买入"]   ]  ) )
 	} }catch(ex){}	
 }
 full
        '''

    if showsql:
        print(q)

    nowvalue='''
    select top 1 当前时间,合约名称,最新价 from tq context by 合约名称 csort 当前时间 desc
    '''
    res=con.run(q)
    tq=con.run("tq")
    try:
        tq.to_csv("tq.csv",encoding="utf-8-sig")
    except:
        pass
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
    con.upload({"ins":futures_base.ins,
                'flag':8
                })
    st,ed=pre_con(st=(now-datetime.timedelta(days=5,hours=10,minutes=0,seconds=135)),ed=(now+datetime.timedelta(hours=0,minutes=0)))
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
        with open(os.path.join(pathlog,'trading.json'),'a',encoding='utf-8') as wd:
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
                # if (lastprice.get(i.get('合约名称')) > i['最高']):
                #     continue
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
                # if  (lastprice.get(i.get('合约名称')) < i['最低']):
                #     continue
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
def 文字2拼音(word):
    f=lambda x:''.join( i[0][0] if not re.match('.*?\d+.*',i[0]) else i[0] for i in pypinyin.pinyin(x, style=pypinyin.NORMAL))
    word.iloc[:,1]=word.iloc[:,1].apply(f)
    con.upload({
        "lastday_W":word
    })
    return word.iloc[:,1].to_list()
    # sen_email(msg_to='834235185@qq.com')

if __name__ == '__main__':
    # lastprice={}
    con=ddb.session()
    showsql=True
#     # con.fconnect('localhost',8922,'admin','123456')
#     con.connect('server.natappfree.cc',38815,'admin','123456')
    con.connect(futures_base.DBhost,futures_base.DBport2,'admin','123456')


    # con.run(获取数据)

    fetchins.update({"data":文字2拼音(con.run("""
            login(`admin,`123456)
db=database('dfs://jerry');
futuresbase=loadTable(db,`futuresbase)
    all1=select trim(商品名称)+trim(交割月份) as `合约,*  from futuresbase where 时间 >=2022.01.25 and 时间<=2023.01.01  and 商品名称 =  "鸡蛋"  order by 时间 asc ,交割月份 desc 
    select top 100000 时间,合约,商品名称,(成交额/sum(成交额) ) as `占比  from all1 context by 时间,商品名称 csort 时间 asc   
    """))})
    ###订阅CTP行情
    threading.Thread(target=CTPmarketfetch).start()
    # time.sleep(10)
    # print('cdscdsc')
# t.daemon = True
    ###实时寻找平仓机会
    threading.Thread(target=finding_2_trade).start()
# # 'server.natappfree.cc',38057
# #
    sched1=BlockingScheduler()
#
    time.sleep(30)
    sched1.add_job(main,'interval',seconds=3.5)
    try:
        sched1.start()
    except Exception as e:
        pass







