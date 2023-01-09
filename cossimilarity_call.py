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
import sys
import time

import dolphindb as ddb
import pandas as pd
import numpy as np
from apscheduler.schedulers.blocking import BlockingScheduler
from futures_base import *
import futures_base
import warnings
warnings.filterwarnings('ignore')

def pre(lis:list):
    con.upload({
        "ins":lis
    })

    sql='''login(`admin,`123456)
db=database('dfs://jerry');
futures=loadTable(db,`futures)
alls=[]
'''
    con.run(sql)



def docosine(lis,st,ed):
    pre(lis)
    sql='''
if((exec count(*) from getFunctionViews() where name="pocess_ins_cos")==1){{  dropFunctionView("pocess_ins_cos")  }};
go;
    def pocess_ins_cos(futures,mutable insrtuments){{ 
    ix=0
endindex=insrtuments.regexFind("[0-9]")
if (endindex==-1){{
	品种名=insrtuments.lower()
	insrtuments=''
	}}else{{
品种名=insrtuments[0:endindex].lower()
	}}
insrtument= distinct(trim(split(insrtuments,',')));   if(insrtuments.count()>0 and insrtuments.last()=="") insrtuments.pop!();
all=select  * from futures where 当前时间>={st} and 当前时间 <={ed}  and 种名简称 = 品种名 and  (  insrtument.count()==0 || 合约名称 in insrtument ) 
all=select 最新价 from all pivot by 当前时间,合约名称
return all
}}
  addFunctionView(pocess_ins_cos);
  
extract_raw=pocess_ins_cos{{ futures }}
lis=ploop( extract_raw ,ins )
all=lis[0]

for (i in lis[1:]  ){{
try{{
     all=lj(all,i,`当前时间`当前时间   ) }}catch(ex){{ }}
}}
    nullFill!(all,0)
names=all.columnNames()[1:]
a=matrix(all[:,1:])
eur_a=table(pow(a,2))
eur_a= ploop(sum,table(eur_a).values())
eur_a=matrix( pow(eur_a,0.5) )
分子=( transpose(a) dot a )
分母= ( transpose( eur_a) dot eur_a   )
s=acos( 分子/分母 )
def hhh(s ) {{ return   s*(180/pi)   }}
角度=hhh(s)
角度=triu(角度,1)
alls.append!(角度 )
'''
    round=2
    while round:
        q=sql.format(st=st.strftime("%Y.%m.%d %H:%M:%S"),ed=(st+datetime.timedelta(seconds=50*60)).strftime("%Y.%m.%d %H:%M:%S"))
        print(round)
        try:
            # print(q)
            con.run(q)
            if round==1:
                break
            round-=1
            time.sleep(20)
        except Exception as e:
            if round==1:
                break
            round-=1
            time.sleep(20)
            continue

    findsql='''
try{
角度=alls[1]-alls[0]
  nullFill!(角度,0)
   //角度 
  角度max=角度[角度 == max(max(角度))]
   nullFill!(角度max,0)
角度=角度[角度!=0]
角度min=角度[角度 == min(min(角度))]
}catch(ex){ print "dda" }
角度
'''
    res=con.run(findsql)
    print(res)
    try:
        names=con.run('''names''')
        角度max=con.run('''角度max''')
        角度min=con.run('''角度min''')
        return 角度max,角度min,names
    except:
        return





def check_cos_diff():
    now=datetime.datetime.now()
    角度max,角度min,names=docosine(["rb2201","m2201",'fu2201',"zn2201",'p2201'],
                               st=now-datetime.timedelta(days=0,hours=0,minutes=5,seconds=60),
                               ed=now
                               )
    ###查找偏离程度最大的pair
    # res[0].argmax(1)
    print(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    ###查找偏离程度最大的pair
    res=np.where((角度max[0]>0) | (角度max[0]<0))
    print('查找偏离程度最大的pair')
    for i,j in zip(*res):
        print(f'{names[i]}-{names[j]}')
    ###查找偏离程度最小的pair
    print('查找偏离程度最小的pair')
    res=np.where((角度min[0]>0) | (角度min[0]<0))
    for i,j in zip(*res):
        print(f'{names[i]}-{names[j]}')



if __name__ == '__main__':
    con=ddb.session()
    con.connect(futures_base.DBhost,futures_base.DBport2,'admin','123456')

    sched1=BlockingScheduler()
# #     # real_time(code)
# #
# #
#     check_cos_diff()
    sched1.add_job(check_cos_diff,'interval',seconds=60)
    try:
        sched1.start()
    except Exception as e:
        pass







