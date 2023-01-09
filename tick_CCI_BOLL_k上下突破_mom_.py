# -*- encoding: utf-8 -*-
"""
@File    : macd_inverse_bak.py
@Time    : 2021/

7/20 18:26
@Author  : handsomejerry
@Email   : 834235185@qq.com
@Software: PyCharm
"""
import copy
import datetime
import json
import os.path
import re
import subprocess
import sys
import time
from pprint import pprint

import dolphindb as ddb
import pandas as pd
import numpy as np
import pypinyin
from apscheduler.schedulers.blocking import BlockingScheduler
from numpy.lib.stride_tricks import as_strided as stride

from futures_base import *
import futures_base
import warnings,threading

warnings.filterwarnings('ignore')
# con.connect('localhost',8922,'admin','123456')
# main()

获取数据='''
login(`admin,`123456)
db=database('dfs://jerry');
futures=loadTable(db,`futuresbase)
use ta;
if((exec count(*) from getFunctionViews() where name="pocess_ins_vol2")==1){{  dropFunctionView("pocess_ins_vol2")  }};
go;
   def pocess_ins_vol2(futures,mutable insrtuments){{
    ix=0;
   bBandsperid={bBandsperid};
    //insrtuments=`鸡蛋2205;
endindex=insrtuments.regexFind("[0-9]")
if (endindex==-1){{
	品种名=insrtuments//.lower()
	交割月份l=''
	}}else{{
品种名=insrtuments[0:endindex]//.lower();
交割月份l=insrtuments[endindex:];
	}}
交割月份l= distinct(trim(split(交割月份l,',')));   if(交割月份l.count()>0 and 交割月份l.last()=="") 交割月份l.pop!();
all=select  *,trim(商品名称)+trim(交割月份) as `合约名称, timestamp(时间) as `int_当前时间 from futures where 时间>={st} and 时间 <={ed}  and 商品名称 = 品种名 and  (  交割月份l.count()==0 || 交割月份 in 交割月份l ) order by 时间 asc
all=select top 10000000 *,move(成交量,1),move(持仓量,1),move(收盘价,1) as `前1,move(收盘价,-4) as `后4 ,
 (ta::adx( 最高价,最低价,收盘价,14 )-move(ta::adx( 最高价,最低价,收盘价,14 ),1)) as `adx_diff , ta::aroon(最高价,最低价, 14)[0] as `aroondown,
ta::aroon(最高价,最低价, 14)[1] as `aroonup , ta::cmo(ta::ma(收盘价, 3, 0),14) as `CMO,ta::macd(最低价,12,26,5)[0] as `diff,ta::macd(最低价,12,26,5)[1] as `dea,ta::macd(最低价,12,26,5)[2] as `macd, ta::ad(最高价,最低价,最低价,成交量) as `AD,move( ta::ad(最高价,最低价,最低价,成交量),1) as `前AD , ta::obv(最低价,成交量) as `obv , move(ta::obv(最低价,成交量),1) as `前obv,ta::rsi(收盘价,14) as `rsi ,ta::mom(收盘价,10) as `mom, ta::bBands(收盘价,bBandsperid,2,2,0)[0] as `upper,ta::bBands(收盘价,bBandsperid,2,2,0)[1] as `middle ,ta::bBands(收盘价,bBandsperid,2,2,0)[2] as `lower  ,move(ta::mom(收盘价,10),1) `前mom,ta::cci(最高价,最低价,收盘价,{CCIperiod}) as `CCI
from all context by 商品名称,合约名称 csort 时间
//	update all set 成交量=( 成交量 - move_成交量 ) , 持仓量=(  持仓量-move_持仓量) 
nullFill!(all,0)
return  all 
}};
  addFunctionView(pocess_ins_vol2);

ins={ins}

all3=pocess_ins_vol2(futures,ins[0])      
def mergetables( futures,mutable f ,lis ){{
	for ( i in lis){{	
	try{{
i=pocess_ins_vol2(futures,i)            
		f.append!(i) }}catch(ex){{}}
		}}
	}}
mer=mergetables{{ futures,all3}}
ploop(mer, ins[1:]  );
'''


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
//insrtuments=`jd
	品种名=insrtuments.lower()
	insrtuments=''
	}}else{{
品种名=insrtuments[0:endindex].lower()
	}}
insrtument= distinct(trim(split(insrtuments,',')));   if(insrtuments.count()>0 and insrtuments.last()=="") insrtuments.pop!();
all=select  *,minute(当前时间) as `到分钟 ,timestamp(当前时间) as `int_当前时间,最新价 from futures where 当前时间>={st} and 当前时间 <={ed}  and 种名简称 = 品种名 and  (  insrtument.count()==0 || 合约名称 in insrtument ) order by 当前时间 asc
all=select top 10000000 *,move(成交量,1),move(持仓量,1),move(最新价,1) as `前1,move(最新价,-4) as `后4
,ta::macd(最新价,12,26,5)[0] as `diff,ta::macd(最新价,12,26,5)[1] as `dea,ta::macd(最新价,12,26,5)[2] as `macd
, ta::bBands(最新价,{bbandperiod},2,2,0)[0] as `upper,ta::bBands(最新价,{bbandperiod},2,2,0)[1] as `middle ,ta::bBands(最新价,{bbandperiod},2,2,0)[2] as `lower  ,move(ta::mom(最新价,10),1) `前mom,ta::cci(最高价,最低价,最新价,{CCIperiod}) as `CCI
, moving(max,最高价,10) as `HH , moving(min,最新价,10) as `LC, moving(max,最新价,10) as `HC,moving(min,最低价,10) as `LL
, ta::adx( 最高价,最低价,最新价,7 ) as `adx ,   //adx越大 （买卖）趋势越明显
ta::aroon(最高价,最低价, 7)[0] as `aroondown,  //aroondown<aroonup  and  aroonup between 70.000001:99.9999999  buy
        ta::aroon(最高价,最低价, 7)[1] as `aroonup ,  ////aroondown>aroonup  and  aroondown between 70.000001:99.9999999  sell 
        ta::cmo(ta::ma(最新价, 3, 0),14) as `CMO,  //本指标下穿-50水平时是买入信号，上穿+50水平是卖出信号。
        ta::ad(最高价,最低价,最新价,成交量) as `AD,move( ta::ad(最高价,最低价,最新价,成交量),1) as `前AD , 
        ta::obv(最新价,成交量) as `obv , move(ta::obv(最新价,成交量),1) as `前obv,ta::rsi(最新价,6) as `rsi6 
        ,ta::rsi(最新价,12) as `rsi12
        ,ta::mom(最新价,5) as `mom 
from all context by 种名简称,合约名称 csort 当前时间;

all= select top 100000000 *,
mbeta( mom,int( int_当前时间 ), 3) as `mombeta,
iif( adx>=moving(mean,adx,7)+0.65*moving(std,adx,7) and moving(std,adx,7)>0 ,`adx_take,`TODO) as `adxflag,
iif(aroondown<aroonup  and  aroonup between 70.000001:99.9999999,`buy,iif(aroondown>aroonup  and  aroondown between 70.000001:99.9999999, `sell,`TODO    )) as `aroonflag,
iif( CMO<-50 and move(CMO,1)>=-50 , `buy, iif(  CMO>50 and move(CMO,1)<=50 ,`sell ,`TODO  ) ) as `CMOflag,
iif(  rsi6>rsi12,`buy, iif( rsi6<rsi12,`sell ,`TODO ) ) as `rsiflag,
iif(   ( move(mom,1)<0 and mom>0 ) or (  move(mom,1)<moving(mean,mom,7) and mom>moving(mean,mom,7)                ) ,`buy ,iif( ( move(mom,1)>0 and mom<0 ) or (  move(mom,1)>moving(mean,mom,7) and mom<moving(mean,mom,7) )    ,`sell  ,`TODO    )) as `momflag
from all context by 种名简称,合约名称 csort 当前时间;

update all set Range = max(HH-LC,HC-LL)
K1=0.65
K2=0.85
update all set Buyline=开盘价+K1*Range
update all set Sellline=开盘价-K2*Range
all=select top 1000000 *, iif( (最新价>= 开盘价 ),最高价-最新价,最高价-开盘价   ) as `上影线长度 , iif( (最新价>= 开盘价 ),开盘价-最低价,最新价-最低价   ) as `下影线长度
, iif( (最新价>= 开盘价 ), 最新价 - 开盘价 ,  开盘价 -最新价   ) as `实体线长度 
, mbeta( CCI,int( int_当前时间 ), 3) as `movbeta
,iif( (最新价>开盘价 ),`阳线,`阴线) as `flag线
,iif( mombeta>=moving(mean,mombeta,3)+0.65*moving(std,mombeta,2) and moving(mean,mombeta,7)>0 ,`mom_take,`TODO) as `mombetaflag
from all context by 种名简称,合约名称 csort 当前时间;

nullFill!(all,0)
return  all 
}};
  addFunctionView(pocess_ins_vol);
'''.format(bbandperiod=14,CCIperiod=14,st=st.strftime("%Y.%m.%d %H:%M:%S"),ed=ed.strftime("%Y.%m.%d %H:%M:%S"))
    if showsql:
        print(sql)
    try:
        con.run(sql)
    except:
        print('exit()')
        time.sleep(1)
        return
    q='''
all2=pocess_ins_vol(futures,ins[0])      //select  * from futures where 日期<=2021.12.21  and 当前时间>={st} and 当前时间 <={ed} and 种名简称 = ins[0]
def mergetables( futures,mutable f ,lis ){
	for ( i in lis){	
	try{
i=pocess_ins_vol(futures,i)               //select  * from futures where 日期<=2021.12.21  and 当前时间>={st} and 当前时间 <={ed} and 种名简称 = i
		f.append!(i) }catch(ex){}
		}
	}
mer=mergetables{ futures,all2}
ploop(mer, ins[1:]  );
'''
    if showsql:
        print(q)
    try:
        con.run(q)
    except:
        print('pocess_ins_vol.exit()')
        time.sleep(1)
        return
        # os._exit(1)
        # 多列滚动函数
    # handle对滚动的数据框进行处理
    return st,ed

def run(st,ed,onruncontract=None):
    global first
    q=f'''
    all2=select top 1000000 *, 
    iif( ((前1<=move(middle,1) and 最新价>middle) or (最新价<lower)),'buy',iif( ((前1>=move(middle,1) and 最新价<middle) or (最新价>upper)) ,'sell','TODO' )   )   as `BOLL_flag from all2 context by 合约名称 csort 当前时间 asc;
  //  move(dydt,1) as `前dydt,move(dy2dt2,1) as `前dy2dt2,  ( iif(  dydt>0,1,0 )  -  iif(  dydt<0,1,0 ) ) *  iif(dy2dt2>0,1,0) as `拟合flag from all2 context by 合约名称 csort 当前时间 asc;
//all2=select *,iif( ((前1<=move(middle,1) and 最新价>middle) or (最新价<lower)),'buy',iif( ((前1>=move(middle,1) and 最新价<middle) or (最新价>upper)) ,'sell','TODO' )   )   as `BOLL_flag 
//from all2 where   middle!=upper!=lower!=0 //and (upper-lower)>2
//update all2 set BOLL_flag=`buy where (最新价>middle)  or (最新价<lower) 
//update all2 set BOLL_flag=`sell where 最新价<middle  or 最新价>upper 
//update all2 set 时间= datetime(date( all2[`当前时间] ))
all2=lj(all2,all3,`合约名称)
all2=select * from all2 where 最新价>all3_lower and 最新价<all3_upper // and (拟合flag !=0 and (dydt*前dydt<0 or dy2dt2*前dy2dt2<0  )) ;
nullFill!(all2,0)
//   tq=select top 100000 *  ,NONE as `CCIflag, NONE as `方程 ,moving(dynimic,[int_当前时间,CCI],flag ) as `calulast,moving(dynimic,[int_当前时间,CCI],flag-4 ) as `calunow 
   tq=select top 100000 *  ,iif(movbeta>=move(movbeta,1),"buy", "sell"  ) as `CCI操作
   ,iif((上影线长度>实体线长度 and 1.65*下影线长度<上影线长度),"sell",iif((下影线长度>实体线长度 and 下影线长度>上影线长度*1.65),"buy","TODO" )) as `影线操作
   ,iif( 
   (flag线=="阴线" and move(flag线,1)=="阴线" and move(flag线,2)=="阴线" and move(flag线,3)=="阳线" and 最新价<move(最新价,1) and move(最新价,1)<move(最新价,2) and 实体线长度>move(实体线长度,1)>0 and move(最低价,1) <= move(最低价,2) ) ,"sell"
      , iif(
      (flag线=="阳线" and move(flag线,1)=="阳线" and move(flag线,2)=="阳线" and move(flag线,3)=="阴线" and 最新价>move(最新价,1) and move(最新价,1)>move(最新价,2) and 实体线长度>move(实体线长度,1)>move(实体线长度,2)>0 and move(最高价,1) >= move(最高价,2) ) or
   (flag线=="阳线" and move(flag线,1)=="阴线" and move(flag线,2)=="阳线" and move(flag线,3)=="阴线" and 最新价>move(开盘价,1) and move(开盘价,1)>move(最新价,2) and 实体线长度>move(实体线长度,1)>move(实体线长度,2)>0  and move(最高价,1) >= move(最高价,2) ),"buy"   
      ,"TODO" )) as `三兵指标
   from all2 context by 合约名称 csort 当前时间 asc;
   
'''
    if showsql:
        print(q)
    try:
        con.run(q)
    except:
        print('tq.exit()')
        return
        # os._exit(1)
    # print(now)
    ####记得实际交易时  ::   and 当前时间>= (now()+1000*3600*8-1000*20)
    qout='''
///fulll=select *, iif( ( ( 方程值-move(方程值,1)>0) and ( CCI方向=="上穿则买入" ) ) ,"buy" , iif( ( ( 方程值-move(方程值,1)<0) and ( CCI方向=="下穿则卖出" ) ),"sell" ,"TODO") ) as `CCI操作 from fulll context  by 合约名称 csort 当前时间 ;
//fulll = select * from fulll where  ( 当前时间.hour()>=9 and 当前时间.minute()>=50 and 当前时间.hour()<=15) or ( 当前时间.hour()>=21 and 当前时间.minute()>=45 and 当前时间.hour()<=23);

//select top 1 当前时间,合约名称,最新价,iif( ((CCI操作=="buy") and (影线操作 =="buy") and (BOLL_flag="buy")),"buy",iif( ((CCI操作=="sell") and (影线操作 =="sell") and (BOLL_flag="sell")),"sell","TODO"  )) as `操作 from tq context  by 合约名称 csort 当前时间 desc having ( 当前时间.hour()>=9 and 当前时间.minute()>=43 and 当前时间.hour()<=15) or ( 当前时间.hour()>=21 and 当前时间.minute()>=15 and 当前时间.hour()<=23) and 当前时间>=(now()+1000*3600*8-1000*60*0.95);
//select top 1 当前时间,合约名称,最新价,iif( ((CCI操作=="buy") and (影线操作 =="buy") and (BOLL_flag="buy")),"buy",iif( ((CCI操作=="sell") and (影线操作 =="sell") and (BOLL_flag="sell")),"sell","TODO"  )) as `操作 from tq context  by 合约名称 csort 当前时间 desc having ( 当前时间.hour()>=9 and 当前时间.minute()>=43 and 当前时间.hour()<=15) or ( 当前时间.hour()>=21 and 当前时间.minute()>=15 and 当前时间.hour()<=23) and 当前时间>=(now()-1000*60*0.95);
//  select top 1000000 当前时间,合约名称,最新价,iif( (((CCI操作=="buy") and (影线操作 =="buy") and (BOLL_flag="buy")) or 三兵指标==`buy ),"buy",iif( (((CCI操作=="sell") and (影线操作 =="sell") and (BOLL_flag="sell")) or 三兵指标==`sell ),"sell","TODO"  )) as `操作 from tq context  by 合约名称 csort 当前时间
select top 1 当前时间,合约名称,最新价, iif(momflag==`buy and mombetaflag ==`mom_take and (BOLL_flag="buy") ,`buy,  iif( momflag==`sell and mombetaflag ==`mom_take and (BOLL_flag="sell"),`sell,`TODO               )           )   as `操作  from tq context  by 合约名称 csort 当前时间 desc having ( 当前时间.hour()>=9 and 当前时间.minute()>=43 and 当前时间.hour()<=15) or ( 当前时间.hour()>=21 and 当前时间.minute()>=15 and 当前时间.hour()<=23) and 当前时间>=(now()-1000*60*0.95);
//select top 10000000 当前时间,合约名称,最新价, iif(momflag==`buy and mombetaflag ==`mom_take and (BOLL_flag="buy") ,`buy,  iif( momflag==`sell and mombetaflag ==`mom_take and (BOLL_flag="sell"),`sell,`TODO               )           )   as `操作  from tq context  by 合约名称 csort 当前时间 desc //having ( 当前时间.hour()>=9 and 当前时间.minute()>=43 and 当前时间.hour()<=15) or ( 当前时间.hour()>=21 and 当前时间.minute()>=15 and 当前时间.hour()<=23) and 当前时间>=(now()-1000*60*0.95);

'''
    if showsql:
        print(qout)
    try:
        res=con.run(qout)
    except:
        print('out.exit()')
        return
    # res.to_csv("res.csv",encoding='utf-8-sig')
    print(res)
    res['当前时间']=res['当前时间'].apply(lambda x:x.strftime("%Y-%m-%d %H:%M:%S"))
    # res['合约名称']=onruncontract
    # res['最新价']=0
    nowvalue='''
    select top 1 当前时间,合约名称,最新价 from tq context by 合约名称 csort 当前时间 desc
    '''
    if showsql:
        print(nowvalue)
    if first:
        nowvalue=con.run(nowvalue)
        nowvalue=nowvalue.to_dict('record')
    # tq=con.run("tq")
    # print(res)
    ###集成了xgboost算法
    try:
        xgb=load_xgboost(con=con,allcode=res['合约名称'].to_list(),st=(ed-datetime.timedelta(days=0,seconds=1*60*60)),ed=ed)
    except:
        print('xgb.exit()')
        return
    # res.to_csv("res.csv",encoding='utf-8-sig')
    # xgb.to_csv("xgb.csv",encoding='utf-8-sig')
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
        # res['合约名称']=onruncontract
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
    try:
        con.upload({"ins":futures_base.ins,
                'flag':8
                })
        st,ed=pre_con(st=(now-datetime.timedelta(days=0,hours=5,minutes=35,seconds=135)),ed=(now+datetime.timedelta(hours=0,minutes=0)))
        ###macd_inverse
        res,nowvalue,xgb=run(st,ed)
        # print(res)
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
        # print(flagres)
        if (not flagres) or (not 继续交易flag.get('flag')):
            return
        flagres=np.array([f'{i.get("合约名称")}:{i.get("当前时间")}' for i in flagres])
        try:
            last=last.tolist()
        except:
            pass
        if ( (len(last) if last else []) != len(flagres) ) or any(last!=flagres ) :
            # print(flagres[last!=flagres])
            try:
                c=flagres[last!=flagres]
                if not isinstance(c[0],list):
                    c=[i.split(':')[0] for i in c]
                else:
                    c=[i.split(':')[0] for i in c[0]]
            except:
                c=[i.split(':')[0] for i in flagres]
            # print(c)
            for i in filter(lambda x:x.get('合约名称') in c,res):
                # i.update({"操作":"buy"}) if i.get('操作')=='sell' else i.update({"操作":"sell"})
                if i.get('操作') == 'TODO':
                    continue
                elif i.get('操作') == 'buy':
                    xgb_res=filter(lambda x:x.get('code')==i.get('合约名称'),xgb).__next__()

                    # if xgb_res['flag']==0 and float(xgb_res['score'])>0.65:
                    #     continue

                    if spread[i.get('合约名称')]<=maxloss:
                        continue
                    if handstate[i.get('合约名称')]['sell_position']:
                        continue
                    #
                    # if xgb_res['flag']==1 and float(xgb_res['score'])<0.35:
                    #     continue
                    try:
                        if handstate.get(i.get('合约名称')).get('buy_price')[-1] < (lastprice.get(i.get('合约名称'))+买滑点):
                            continue
                    except:
                        pass
                    futures_base.xgb_res=xgb_res
                    # buy(contractname=i.get('合约名称'),currentprice=i.get('最新价'),flag='open')
                    # if (lastprice.get(i.get('合约名称')) > i['最高']):
                    #     continue
                    buy(contractname=i.get('合约名称'),currentprice=float(lastprice.get(i.get('合约名称'))+买滑点),flag='凯莉')
                    # buy(contractname=i.get('合约名称'),currentprice=lastprice.get(i.get('合约名称'))+买滑点,flag=2)
                    email=True
                elif i.get('操作') == 'sell':
                    xgb_res=filter(lambda x:x.get('code')==i.get('合约名称'),xgb).__next__()
                    if handstate[i.get('合约名称')]['buy_position']:
                        continue
                    # if xgb_res['flag']==1 and float(xgb_res['score'])>0.65:
                    #     continue
                    # if xgb_res['flag']==0 and float(xgb_res['score'])<0.45:
                    #     continue
                    futures_base.xgb_res=xgb_res
                    try:
                        if handstate.get(i.get('合约名称')).get('sell_price')[-1] > (lastprice.get(i.get('合约名称'))-卖滑点):
                            continue
                        # continue
                    except:
                        pass
                    # if  (lastprice.get(i.get('合约名称')) < i['最低']):
                    #     continue
                    # print("")
                    sell(contractname=i.get('合约名称'),currentprice=float( lastprice.get(i.get('合约名称'))-卖滑点),flag='凯莉')
                    # sell(contractname=i.get('合约名称'),currentprice=lastprice.get(i.get('合约名称'))-卖滑点,flag=2)
                    email=True

            res.append(spread)
            res.append(handstate)
            # res.append(xgb)
            email_content=json.dumps(res,ensure_ascii=False)
            c=f'''以下合约加时间点触发了反向交易信号  :{email_content}'''
            print(handstate)
            # print(spread)
            print('\n')
            if email:
                # sen_email(msg_to='834235185@qq.com',content=str(c))
                # sen_email(msg_to='1922335454@qq.com,yangjiarui0032021@163.com',content=c)
                # sen_email(msg_to='1922335454@qq.com',content=c)
                email=False
            last=np.array( flagres)
        else:
            pass
    except Exception as e:
        print(e.__str__())
        return
        #time.sleep(1)
        #os._exit(1)
        # time.sleep(2)
        # subprocess.Popen(['killall','python'])
        # # tick_CCI_BOLL_k上下突破()
        # main()

            # sys.exit(0)
def 文字2拼音(word):
    f=lambda x:''.join( i[0][0] if not re.match('.*?\d+.*',i[0]) else i[0] for i in pypinyin.pinyin(x, style=pypinyin.NORMAL))\
        .replace('dp','m')  ##中文合约名转换
    word.iloc[:,1]=word.iloc[:,1].apply(f)
    con.upload({
        "lastday_W":word
    })
    return word.iloc[:,1].to_list()
    # sen_email(msg_to='834235185@qq.com')



def tick_CCI_BOLL_k上下突破():
    global showsql,con
    con=ddb.session()
    showsql=True
    print("开始交易")
    继续交易flag.update({"flag":True})
    #     # con.fconnect('localhost',8922,'admin','123456')
    #     con.connect('server.natappfree.cc',38815,'admin','123456')
    con.connect(futures_base.DBhost,futures_base.DBport2,'admin','123456',highAvailability=True)
    sched1=BlockingScheduler()
    # main()
    # finding_2_cansole_trade()
    con.run(获取数据.format(bBandsperid='14',
                            CCIperiod=14,
                            st='2022.01.01 08:58:59',
                            ed='2023.01.01 08:58:59',
                            ins=["豆粕2305",'玉米2305','鸡蛋2305','生猪2303','聚乙烯2305','RM301','纸浆2305','焦煤2305','豆油2305','螺纹钢2305','铁矿石2305','AP2305'][:]))
    con.run('''
    all3=select top 1 * from all3 context by 商品名称,交割月份 csort 时间 desc;
    all3[`合约名称]=all3[`合约名称].strReplace('鸡蛋',`jd);
    all3[`合约名称]=all3[`合约名称].strReplace('豆粕',`m);
    all3[`合约名称]=all3[`合约名称].strReplace('豆一',`a);
    all3[`合约名称]=all3[`合约名称].strReplace('玉米',`c);
     all3[`合约名称]=all3[`合约名称].strReplace('生猪',`lh);
       all3[`合约名称]=all3[`合约名称].strReplace('焦煤',`j);
    all3[`合约名称]=all3[`合约名称].strReplace('梗米',`rr);
    all3[`合约名称]=all3[`合约名称].strReplace('豆油',`y);
    all3[`合约名称]=all3[`合约名称].strReplace('聚乙烯',`l);
  //  all3[`合约名称]=all3[`合约名称].strReplace('RM301',`RM2301);
    all3[`合约名称]=all3[`合约名称].strReplace('纸浆',`sp);
    all3[`合约名称]=all3[`合约名称].strReplace('铁矿石',`i);
    all3[`合约名称]=all3[`合约名称].strReplace('螺纹钢',`rb);
update all3 set 日期= all3[`时间]
                 ''')
    # fetchins.update({"data":['jm2205','jd2205','c2205','m2205','p2205']})
    fetchins.update({"data":['l2305','jd2305','c2305','m2305','lh2303','j2305','RM301','sp2305','y2305','rb2305','i2305','AP305']})
    # 初始金额=10000/len(ins)
    # main()

    ##订阅CTP行情
    threading.Thread(target=CTPmarketfetch).start()
    sched1.add_job(finding_2_cansole_trade,'interval',seconds=7)
    #     ###实时寻找平仓机会
    threading.Thread(target=finding_2_trade).start()
    time.sleep(1)
    sched1.add_job(main,'interval',seconds=28)
    try:
        sched1.start()
    except Exception as e:
        print('sched1.exit()')
        time.sleep(1)
        os._exit(1)
#


if __name__ == '__main__':
    try:
        tick_CCI_BOLL_k上下突破()
    except:
        print('tick_CCI_BOLL_k上下突破.exit()')
        time.sleep(1)
        os._exit(1)
        # time.sleep(3)
        # subprocess.Popen(['killall','python'])
        # time.sleep(2)
        # subprocess.Popen(['killall','python'])
        # tick_CCI_BOLL_k上下突破()






