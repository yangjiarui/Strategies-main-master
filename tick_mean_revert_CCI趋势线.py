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
import requests
from apscheduler.schedulers.blocking import BlockingScheduler
from numpy.lib.stride_tricks import as_strided as stride

from futures_base import *
import futures_base
import warnings,threading

from futures_base.wc2 import send_msg

warnings.filterwarnings('ignore')
# con.connect('localhost',8922,'admin','123456')


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
all=select top 10000000 *,
 ta::bBands(收盘价,bBandsperid,2,2,0)[0] as `upper,ta::bBands(收盘价,bBandsperid,2,2,0)[1] as `middle ,ta::bBands(收盘价,bBandsperid,2,2,0)[2] as `lower 
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


def pre_con(st,ed,con2):
    # global con2
    sql='''
        login(`admin,`123456)
db=database('dfs://jerry');
futures=loadTable(db,`futures)
use ta;
use mytt;

defg dynimic(x,y){{
	ymin=string(y[y.argmin()])
	ymax=string(y[y.argmax()])
	xmin=string(x[y.argmin()])
	xmax=string(x[y.argmax()])
	return xmax+'_'+ymax+'_'+xmin+'_'+ymin
	}};


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
all=select  top 1000000  *  
,(accumulate(max,   REF(最新价,90) )- REF(最新价,90))/accumulate(max, REF(最新价,90) ) as `当前亏损值,accumulate(max, (accumulate(max, REF(最新价,90) )- REF(最新价,90))/accumulate(max, REF(最新价,90) )) as `当前最大亏损,
(( REF(最新价,90)-accumulate(min,   REF(最新价,90) ) )/accumulate(min, REF(最新价,90) ) ) as `当前盈利值,accumulate(max,( REF(最新价,90)- accumulate(min, REF(最新价,90) ))/accumulate(min, REF(最新价,90) )) as `当前最大盈利
,MACD( 最新价,5,14,9)[0] as `short_dif , MACD( 最新价,5,14,9)[1] as `short_dea , MACD( 最新价,24,47,9)[2] as `short_macd
  , MACD( 最新价,12,24,9)[0] as `mid_dif ,   MACD( 最新价,12,24,9)[1] as `mid_dea  , MACD( 最新价,24,47,9)[2] as `mid_macd
,move(成交量,1),move(持仓量,1),0 as `后4  ,int(当前时间),最新价
,iif( (最新价>= 开盘价 ),最高价-最新价,最高价-开盘价   ) as `上影线长度 , iif( (最新价>= 开盘价 ),开盘价-最低价,最新价-最低价   ) as `下影线长度
, iif( (最新价>= 开盘价 ), 最新价 - 开盘价 ,  开盘价 -最新价   ) as `实体线长度 
, CCI(最高价,最低价,最新价,5) as `CCI
,iif( (最新价>开盘价 ),`阳线,`阴线) as `flag线
 from futures where 当前时间>={st} and 当前时间 <={ed} and 种名简称 = 品种名 and  (  insrtument.count()==0 || 合约名称 in insrtument ) context by date(当前时间),种名简称,合约名称   csort 当前时间 asc
	
all=select top 1000000 *,abs( mbeta( 最新价 ,int( timestamp(当前时间 )), 5)) as `movbeta  , moving(dynimic,[int_当前时间,CCI],4 ) as `calu
,move( moving(dynimic,[int_当前时间,CCI],4 ) ,1) as `diffcalu
from all context by  种名简称,合约名称 ,date(当前时间 ) csort 当前时间 asc;
 //update all set 成交量=( 成交量 - move_成交量 ) , 持仓量=(  持仓量-move_持仓量) 
 all=select top 1000000 *,BARSLAST(CROSS( mid_dif,mid_dea  ) ==1) as `MACD金叉,BARSLAST(CROSS( mid_dea,mid_dif  ) ==1) as `MACD死叉 from all
 context by 种名简称,合约名称,date(当前时间 ) csort 当前时间;
  all=select top 1000000 *, iif(MACD金叉<MACD死叉 and MACD金叉!=REF(MACD金叉,1) ,`MACD金叉,iif(MACD金叉>MACD死叉 and MACD死叉!=REF(MACD死叉,1) ,`MACD死叉 ,`TODO) ) as `MACD信号,BARSLAST(CROSS( mid_dea,mid_dif  ) ==1) as `MACD死叉 from all
 context by 种名简称,合约名称,date(当前时间 ) csort 当前时间
nullFill!(all,0)
return  all
}};
  addFunctionView(pocess_ins_vol);
'''.format(bbandperiod=14,CCIperiod=14,st=st.strftime("%Y.%m.%d %H:%M:%S"),ed=ed.strftime("%Y.%m.%d %H:%M:%S"))
    if showsql:
        print(sql)
    try:
        con2.run(sql)
    except:
        print('exit()')
        time.sleep(1)
        return
    q='''
all2=pocess_ins_vol(futures,ins[0])    
def mergetables( futures,mutable f ,lis ){
	for ( i in lis){	
	try{
i=pocess_ins_vol(futures,i) 
		f.append!(i) }catch(ex){}
		}
	}
mer=mergetables{ futures,all2}
ploop(mer, ins[1:]  );
'''
    if showsql:
        print(q)
    try:
        con2.run(q)
    except Exception as e:
        print('pocess_ins_vol.exit()',e.__str__())
        # time.sleep(1)
       # requests.get('http://localhost:1234/index.html?action=restartall')
        return
        # os._exit(1)
        # 多列滚动函数
    # handle对滚动的数据框进行处理
    return st,ed

def run(st,ed,con2,onruncontract=None):
    global first
    q=f'''
all2=select top 100000 *,iif(short_dea>=0 and short_dif>=0 and CROSS(short_dea,short_dif )==1,`sell,iif( mid_dea<=0 and mid_dif<=0  and CROSS( mid_dif,mid_dea  ) ==1 ,`buy,`TODO))  as `MACD信号2  from all2 context by 种名简称,合约名称 ,date(当前时间 ) csort 当前时间;
nullFill!(all2,0);
    all2=select top 100000 * ,
iif(  当前亏损值>= 当前最大亏损>0 and   MACD信号==`MACD金叉  and MACD信号2==`buy and mid_macd>=0 and  movbeta<=move(movbeta,1) ,`buy,iif(  当前盈利值>= 当前最大盈利>0 and   MACD信号==`MACD死叉 and MACD信号2==`sell  and mid_macd<=0  and movbeta<=move(movbeta,1) ,`sell  ,`TODO) ) as `原始操作
from all2  context by  种名简称,合约名称 ,date(当前时间 ) csort 当前时间;

fulllis=[]
flaglis=[]
def  CCI趋势线(  tb, mutable fulllis, mutable flaglis ){{
    for (i in tb ){{
    try{{
    n=i.calu.split('_')
    l=i.diffcalu.split('_')
    if(n[3]>l[3] and n[3]>l[3]){{// print "下穿则卖出 "
    分子=( i.CCI- float(n[3] )  )/( i.int_当前时间-int(n[2]) )
    分母=( i.CCI- float( l[3] )  )/( i.int_当前时间-int( l[2] ))
    aw=分子-分母;
    fulllis.append!(aw);
    flaglis.append!(`下穿则卖出 );
    }}
    else if(n[1]<l[1] and n[0]>l[0]){{
    分子=( i.CCI- float(n[1] )  )/( i.int_当前时间- int( n[0]))
    分母=( i.CCI- float( l[1] )  )/( i.int_当前时间-int( l[0] ))
    aw=分子-分母;
    fulllis.append!(aw);
    flaglis.append!( `上穿则买入);
 	}} else{{  fulllis.append!(0);flaglis.append!(`TODO )  }}   }} catch(ex){{  fulllis.append!(0);flaglis.append!(`TODO )   }}
    }};
    }}
    CCI趋势线(all2,fulllis,flaglis )
    update all2 set 方程值= fulllis$FLOAT
    update all2 set 方向=flaglis$STRING
     all2=select * from lj(all2,shareall3,`合约名称,`合约名称) ;
all2=select top 10000000 *,iif(最新价>min(lower,shareall3_最低价) or 最新价<max(upper,shareall3_最高价) ,`cando,`TODO  ) as `日度boll ,  BARSLAST( 方程值>0 and REF(方程值,1)<0) as `CCI趋势线金叉  , BARSLAST(  方程值<0 and REF(方程值,1)>0 ) as `CCI趋势线死叉 from all2 context by 种名简称,合约名称,date(当前时间 ) csort 当前时间 asc
all2=select top 10000000 *,  iif(CCI趋势线金叉<CCI趋势线死叉 and CCI趋势线金叉!=REF(CCI趋势线金叉,1) and 方向==`上穿则买入 ,`CCI趋势线金叉,iif(CCI趋势线金叉>CCI趋势线死叉 and CCI趋势线死叉!=REF(CCI趋势线死叉,1) and 方向==`下穿则卖出 ,`CCI趋势线死叉 ,`TODO) ) as `CCI趋势线信号  from all2 context by 种名简称,合约名称,date(当前时间 ) csort 当前时间 asc
  nullFill!(all2,0);
   allout=select  top 1000000 当前时间,合约名称,最新价 , iif( 日度boll==`cando and CCI趋势线信号==`CCI趋势线死叉 and 原始操作==`sell , `sell,iif( 日度boll==`cando and CCI趋势线信号==`CCI趋势线金叉 and 原始操作==`buy  ,`buy,`TODO )) as `操作 from all2 context by 种名简称,合约名称,date(当前时间 ) csort 当前时间 当前时间 desc
    '''
    if showsql:
        print(q)
    try:
        con2.run(q)
    except Exception as e:
        print('tq.exit()',e.__str__())
        #requests.get('http://localhost:1234/index.html?action=restartall')
        return
        # os._exit(1)
    # print(now)
    ####记得实际交易时  ::   and 当前时间>= (now()+1000*3600*8-1000*20)
    qout='''
select top 1 当前时间,合约名称,最新价,操作  from allout
 context by 合约名称,date(当前时间 ) csort 当前时间  desc  having (( 当前时间.hour()>=9 and 当前时间.minute()>=43 and 当前时间.hour()<=15) or ( 当前时间.hour()>=21 and 当前时间.minute()>=15 and 当前时间.hour()<=23)) and 当前时间>=(now()-1000*60*2.95);
'''
    if showsql:
        print(qout)
    try:
        res=con2.run(qout)
    except Exception as e:
        print('out.exit()')
        #requests.get('http://localhost:1234/index.html?action=restartall')
        return
    # res.to_csv("res.csv",encoding='utf-8-sig')
    # print(res)
    res['当前时间']=res['当前时间'].apply(lambda x:x.strftime("%Y-%m-%d %H:%M:%S"))
    # res['合约名称']=onruncontract
    # res['最新价']=0
    nowvalue='''
    select top 1 当前时间,合约名称,最新价 from allout context by 合约名称 csort 当前时间 desc
    '''
    if showsql:
        print(nowvalue)
    if first:
        nowvalue=con2.run(nowvalue)
        nowvalue=nowvalue.to_dict('record')
    # tq=con.run("tq")
    print(res)
    ###集成了xgboost算法
    try:
        xgb=load_xgboost(con=con2,allcode=res['合约名称'].to_list(),st=(ed-datetime.timedelta(days=0,seconds=38*60*60)),ed=ed)
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
    con2=ddb.session()
    con2.connect(futures_base.DBhost,futures_base.DBport2,'admin','123456',highAvailability=False)
    now=datetime.datetime.now()
    try:
        con2.upload({"ins":futures_base.ins,
                'flag':8
                })
        st,ed=pre_con(st=(now-datetime.timedelta(days=0,hours=20,minutes=35,seconds=20)),ed=(now+datetime.timedelta(hours=0,minutes=20)),con2=con2)
        # st,ed=pre_con(st=(now.date()),ed=(now+datetime.timedelta(hours=0,minutes=0)))
        ###macd_inverse
        res,nowvalue,xgb=run(st,ed,con2=con2)
        con2.close()
        send_msg([[i.get('当前时间'),i.get('合约名称'),i.get('操作')] for i in res])
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
                    # buy(contractname=i.get('合约名称'),currentprice=float(lastprice.get(i.get('合约名称'))+买滑点),flag='凯莉')
                    buy(contractname=i.get('合约名称'),currentprice=lastprice.get(i.get('合约名称'))+买滑点,flag=400)
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
                    # sell(contractname=i.get('合约名称'),currentprice=float( lastprice.get(i.get('合约名称'))-卖滑点),flag='凯莉')
                    sell(contractname=i.get('合约名称'),currentprice=lastprice.get(i.get('合约名称'))-卖滑点,flag=400)
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
        if 'Couldn' in e.__str__():
            # sys.exit(1)
            requests.get('http://localhost:1234/index.html?action=restartall')
        print(e.__str__())
        return
        #time.sleep(1)
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
    con.connect(futures_base.DBhost,futures_base.DBport2,'admin','123456',highAvailability=False)
    sched1=BlockingScheduler()
    # main()
    # finding_2_cansole_trade()
    con.run(获取数据.format(bBandsperid='14',
                        CCIperiod=14,
                        st='2022.01.01 08:58:59',
                        ed='2023.10.01 08:58:59',
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
update all3 set 日期= all3[`时间];
share all3 as shareall3
                 ''')
    # con.close()
    # fetchins.update({"data":['jm2205','jd2205','c2205','m2205','p2205']})
    fetchins.update({"data":['l2305','jd2305','c2305','m2305','lh2303','j2305','RM301','sp2305','y2305','rb2305','i2305','AP305']})
    # 初始金额=10000/len(ins)
    # main()

    ##订阅CTP行情
    threading.Thread(target=CTPmarketfetch).start()
    sched1.add_job(finding_2_cansole_trade,'interval',seconds=12)
    sched1.add_job(自动回撤订单,'interval',seconds=125)
    #     ###实时寻找平仓机会`
    threading.Thread(target=finding_2_trade).start()
    time.sleep(1)
    sched1.add_job(main,'interval',seconds=38)
    try:
        sched1.start()
    except Exception as e:
        print('sched1.exit()')
        time.sleep(1)
        sys.exit(1)

if __name__ == '__main__':
    try:
        tick_CCI_BOLL_k上下突破()
    except Exception as e:
        print('tick_CCI_BOLL_k上下突破.exit()')
        time.sleep(1)
        sys.exit(1)
        # time.sleep(3)
        # subprocess.Popen(['killall','python'])
        # time.sleep(2)
        # subprocess.Popen(['killall','python'])
        # tick_CCI_BOLL_k上下突破()






