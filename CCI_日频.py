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


def pre_con(PD,st,ed):
    # global st,ed
    sql='''
           login(`admin,`123456)
db=database('dfs://jerry');
futures=loadTable(db,`futuresbase)
use ta;
all0=select trim(商品名称)+trim(交割月份) as `合约 ,* from futures where 时间 >={st} and 时间<={ed}  and 商品名称 =  "{PD}"  order by 时间 asc ,交割月份 desc 
t=select top 100000 时间,交割月份,商品名称,sum(成交额) from all0 context by 时间,商品名称 csort 时间 asc
all=lj(all0, t,`时间`商品名称`交割月份,`时间`商品名称`交割月份)
update all set 收盘价_W =收盘价*成交额/sum_成交额
update all set 开盘价_W =开盘价*成交额/sum_成交额
update all set 最高价_W =最高价*成交额/sum_成交额
update all set 最低价_W =最低价*成交额/sum_成交额
update all set 成交量_W =成交量*成交额/sum_成交额
update all set 持仓量_W =持仓量*成交额/sum_成交额
all=select 时间,sum(收盘价_W) as `收盘价,sum(开盘价_W) as`开盘价,sum(最高价_W)  as`最高价 ,sum(最低价_W)  as`最低价,
sum(成交量_W) as `成交量,sum(持仓量_W) as `持仓量
from all group by 时间,商品名称
all=select  *, move(成交量,1),move(持仓量,1),move(收盘价,-4) as `后4  ,timestamp(时间),ta::cci(最高价,最低价,收盘价,14) as `CCI from all order by 时间 asc
nullFill!(all,0)
'''.format(PD=PD,st=st.strftime("%Y.%m.%d %H:%M:%S"),ed=ed.strftime("%Y.%m.%d %H:%M:%S"))
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
       tq=select top 1000000 *  ,NONE as `flag, NONE as `fx ,moving(dynimic,[timestamp_时间,CCI],flag ) as `calu , move( moving(dynimic,[timestamp_时间,CCI],flag ),1  ) as `diffcalu from all context by 商品名称 csort 时间 asc
   nullFill!(tq,0)
'''
    if showsql:
        print(q)
    con.run(q)
    return st,ed

def run(st,ed):
    global first
    q='''
    full=table(10000000:5, `当前时间`合约名称`最新价`方程值`方向, `DATETIME`SYMBOL`DOUBLE`DOUBLE`SYMBOL)//;(当前时间, 合约名称,最新价,方程值,方向 )
 for ( i in tq) {
 try{
 	n=i.calu.split('_')
 	l=i.diffcalu.split('_') 
 	//update tq set fx=aw where 当前时间=i.当前时间 and 合约名称=i.合约名称
if(n[3]>l[3]){ //print "下穿则卖出 " 
 	分子=( i.CCI- float(n[3] )  )/( i.timestamp_时间- timestamp( n[2]))
 	分母=( i.CCI- float( l[3] )  )/( i.timestamp_时间-timestamp( l[2] )) 
		 aw=分子-分母;
		 		 		 if ( aw==NULL ){continue }

//update tq set flag=1 where 当前时间=i.当前时间 and 合约名称=i.合约名称
 full.append!( table([[ i.时间 ],[i.商品名称],[i.收盘价],[aw],["下穿则卖出"]   ])   )
}
else if(n[1]<l[1]){ //print "上穿则买入" 
 	分子=( i.CCI- float(n[1] )  )/( i.timestamp_时间- timestamp( n[0]))
 	分母=( i.CCI- float( l[1] )  )/( i.timestamp_时间-timestamp( l[0] )) 
		 aw=分子-分母;
		 		 		 if ( aw==NULL ){continue }
//update tq set flag=2 where 当前时间=i.当前时间 and 合约名称=i.合约名称} 	

 full.append!( table([ [i.时间],[i.商品名称],[i.收盘价],[aw],["上穿则买入"]   ]  ) )
 	} }catch(ex){}	
 }
 L8=select top 1 时间,商品名称,合约 from all0  context by 时间,商品名称 csort 成交量 desc
select * from lj(full,L8,`当前时间`合约名称,`时间`商品名称  ) where 当前时间>=2001.01.01
'''
    q反='''
    full=table(10000000:5, `当前时间`合约名称`最新价`方程值`方向, `DATETIME`SYMBOL`DOUBLE`DOUBLE`SYMBOL)//;(当前时间, 合约名称,最新价,方程值,方向 )
 for ( i in tq) {
 try{
 	n=i.calu.split('_')
 	l=i.diffcalu.split('_') 
 	//update tq set fx=aw where 当前时间=i.当前时间 and 合约名称=i.合约名称
if(n[3]>l[3]){ //print "下穿则卖出 " 
 	分子=( i.CCI- float(n[3] )  )/( i.timestamp_时间- timestamp( n[2]))
 	分母=( i.CCI- float( l[3] )  )/( i.timestamp_时间-timestamp( l[2] )) 
		 aw=分子-分母;
		 		 		 if ( aw==NULL ){continue }

//update tq set flag=1 where 当前时间=i.当前时间 and 合约名称=i.合约名称
 full.append!( table([[ i.时间 ],[i.商品名称],[i.收盘价],[aw],["上穿则买入"]   ])   )
}
else if(n[1]<l[1]){ //print "上穿则买入" 
 	分子=( i.CCI- float(n[1] )  )/( i.timestamp_时间- timestamp( n[0]))
 	分母=( i.CCI- float( l[1] )  )/( i.timestamp_时间-timestamp( l[0] )) 
		 aw=分子-分母;
		 		 		 if ( aw==NULL ){continue }
//update tq set flag=2 where 当前时间=i.当前时间 and 合约名称=i.合约名称} 	

 full.append!( table([ [i.时间],[i.商品名称],[i.收盘价],[aw],["下穿则卖出"]   ]  ) )
 	} }catch(ex){}	
 }
 L8=select top 1 时间,商品名称,合约 from all0  context by 时间,商品名称 csort 成交量 desc
select * from lj(full,L8,`当前时间`合约名称,`时间`商品名称  ) where 当前时间>=2001.01.01
'''
    if showsql:
        print(q)
    res=con.run(q反)
    ####记得实际交易时  ::   and 当前时间>= (now()+1000*3600*8-1000*20)

    to_xgb=res.apply(lambda x:f"{x['合约']}_{x['当前时间'].__str__()[:10].replace('-','.')}",axis=1)
    ###集成了xgboost算法
    xgb=load_xgboost(con=con,allcode=to_xgb.to_list(),st=(ed-datetime.timedelta(days=700)),ed=ed)
    # print(xgb)
    xgb['time']=xgb['time'].apply(lambda x:x.strftime("%Y-%m-%d %H:%M:%S"))
    res.rename({
        "合约":'code',
        "当前时间":'time'
    },axis=1,inplace=True)
    xgb['time']=pd.to_datetime(xgb['time'])
    return pd.merge(res,xgb,on=['time','code'],how='left')


def load_xgboost(allcode,con,st,ed):
    w='''
    try{loadPlugin("/home/DolphinDB_Linux64_V1.20.20/server/plugins/xgboost/PluginXgboost.txt")}catch(ex){print ex}
go;
 def mynormal(flag_col,mutable wa){
        sstd=ploop(std,wa.values())
    smean=ploop(mean,wa.values())
    wa=table((matrix(wa)-smean)/sstd)
    try{
    update wa set flag= flag_col[`flag] } catch(ex){}
    return wa
    };
    
    def main(all,trainsize,mutable ftable,codestr){
        code,timm=codestr.split("_")
        wa= select 当前时间 as `时间, 合约名称 as `代码, 合约名称 as `简称, 最新价 as `收盘价, 开盘价, 最低价, 最高价 , 持仓量 as `成交金额,  成交量  as `成交量_股 from all
        wa=select 时间 , 代码, 简称, 收盘价, 开盘价, 最低价, 最高价 , 成交金额,  成交量_股,move(收盘价,-1) as `cur,moving(first,开盘价,2) as `q开盘价,moving(first,最高价,2) as `q最高价,moving(first,最低价,2) as `q最低价,moving(first,成交量_股,2) as `q成交量,moving(first,成交金额,2) as `q成交金额,0 as `flag from wa where 代码 =code and 时间 <= temporalParse(timm,"yyyy.MM.dd") and 时间 >= (temporalParse(timm,"yyyy.MM.dd")-30)
        num=wa.count()
        update wa set flag=1 where 收盘价>=cur
        flag_col=select flag from wa
        alldf=mynormal(flag_col,select cur,收盘价,q开盘价,q最低价,q最高价,q成交金额,q成交量 from wa  )
        train=select * from  alldf[:int(num*trainsize)]  
        test=select col0, col1,col2,col3,col4,col5,col6,flag   from alldf[int(num*trainsize):]
      params = {objective: "multi:softmax", num_class: 2}
model = xgboost::train(train.flag , train[:,0:6], params)
res = xgboost::predict(model, select col0,col2,col3,col4,col5,col6 from test )    
        
     good=mynormal( [`hjk`bjhkb`nkl],select 收盘价,开盘价,最低价,最高价,成交金额,成交量_股 from wa )
    good=  select col0.last() as `col0 ,col1.last() as `col2,col2.last() as `col3,col3.last() as `col4,col4.last() as `col5,col5.last() as `col6 from good
     good= xgboost::predict(model,  select * from good )
    //res=matrix(res)
    flag_col=select flag from test
    //print res,flag_col
    sss=select 时间,收盘价 from wa[int(num*trainsize):]
    //update sss set flag=flag_col[`flag]
    // print sss
    res=table(res,flag_col)
    resa=select count(*) from res where res=flag
    tqqw=wa.时间.last()
    try{
    ftable.append!( table([  [timestamp( tqqw )] ,  [code],[resa[`count][0]*1.000  /res.count()[0]],[good[0] ]        ]     )  )
    } catch (ex) {}
       }
     '''
    # print(w)
    con.run(w)
    ####变换原始数据
    sql='''
     login(`admin,`123456)
        db=database('dfs://jerry');
        futures=loadTable(db,`futuresbase)
all0=select trim(商品名称)+trim(交割月份) as `合约名称 ,* from futures where 时间 >={st} and 时间<={ed}  and 商品名称 =  "{PD}"  order by 时间 asc ,交割月份 desc 
all2=select top 100000000 合约名称,时间 as `当前时间,开盘价,最高价,最低价,收盘价 as `最新价,结算价,前结算价 as `昨结算, 持仓量-move(持仓量,1) as `持仓量,成交量- move(成交量,1) as `成交量 
       from all0 context by 合约名称 csort 时间 asc
    nullFill!(all2,0);
    '''.format(PD=PD,st=st.strftime("%Y.%m.%d %H:%M:%S"),ed=ed.strftime("%Y.%m.%d %H:%M:%S"))
    # print(sql)
    all=con.run(sql)
    con.upload({
        "lis":allcode
    })
    try:
        g='''
    fq=table(1000000:0,`time`code`score`flag,[TIMESTAMP,STRING,STRING,INT]  )
mainss=main{all2,0.755,fq}
try{ loop(mainss, lis ) }catch(ex){}
   fq
    '''
        # print(g)
        start=con.run(g)
        return start
    except Exception as e:
        print(e.__str__())


def main():
    global last,lastspread,email,PD
    PD="酒单"
    now=datetime.datetime.now()
    con.upload({
                'flag':8
                })
    st,ed=pre_con(PD=PD,st=(now-datetime.timedelta(days=500,hours=10,minutes=0,seconds=135)),ed=(now+datetime.timedelta(hours=0,minutes=0)))
    ###macd_inverse
    res=run(st,ed)
    res.sort_values(by=['time','score'],ascending=[1,0],inplace=True)

    res.to_csv(f"{PD}交易smart.csv",encoding='utf-8-sig')

if __name__ == '__main__':
    # lastprice={}
    con=ddb.session()
    showsql=True
#     # con.fconnect('localhost',8922,'admin','123456')
#     con.connect('server.natappfree.cc',38815,'admin','123456')
    con.connect(futures_base.DBhost,futures_base.DBport2,'admin','123456')
    main()







