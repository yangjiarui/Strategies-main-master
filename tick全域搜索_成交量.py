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
all=select  *,  minute(当前时间) as `到分钟 ,timestamp(当前时间) as `int_当前时间,最新价 from futures where 当前时间>={st} and 当前时间 <={ed}  and 种名简称 = 品种名 and  (  insrtument.count()==0 || 合约名称 in insrtument ) order by 当前时间 asc
all=select top 1000000 *,move(成交量,1),move(持仓量,1),move(最新价,1) as `前1,move(最新价,-4) as `后4   from all context by 种名简称,合约名称 csort 当前时间
	update all set 成交量=( 成交量 - move_成交量 ) , 持仓量=(  持仓量-move_持仓量) 
return select 合约名称,到分钟,当前时间,开盘价,最高价,最低价,买价,卖价,最新价,前1,结算价,买量,卖量,持仓量,成交量,种名简称,日期,后4,int_当前时间 from all 
}}.
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
'''
    if showsql:
        print(q)
    con.run(q)
    return st,ed

def run(st,ed,onruncontract):
    global first
    q=f'''
      t=select top 1000000 *,(最新价-前1) as `待解释值 , (成交量)/sum(成交量) as `占比 from all2 where 合约名称 !=`{onruncontract} context by 种名简称,合约名称,到分钟 csort 当前时间 
可解释=select last( 当前时间),sum(待解释值*占比) from t group by 种名简称,到分钟
nu=t[`合约名称].distinct().count()
t=select top 1000000 *,(最新价-前1) as `待解释值 , (成交量)/sum(成交量) as `占比 from all2 where 合约名称 =`{onruncontract} context by 种名简称,合约名称,到分钟 csort 当前时间 
待解释=select  last( 当前时间),sum(待解释值*占比) from t group by 种名简称,到分钟
'''
    if showsql:
        print(q)
    con.run(q)
    # print(now)
    ####记得实际交易时  ::   and 当前时间>= (now()+1000*3600*8-1000*20)
    q='''
t= lj(待解释,可解释,`到分钟`到分钟   ) 
update t set 可解释_sum=可解释_sum/nu
res=select top 1 *, iif(  可解释_sum>=sum>0, `buy   , iif(   可解释_sum<=sum<0 ,`sell  ,`TODO    )    ) as `操作    from  t    order by last_当前时间 desc    
        select * from res where abs(sum-可解释_sum)>0.02
'''
    if showsql:
        print(q)

    res=con.run(q)
    res['当前时间']=res['last_当前时间'].apply(lambda x:x.strftime("%Y-%m-%d %H:%M:%S"))
    res['合约名称']=onruncontract
    res['最新价']=0

    nowvalue=f'''
    s=select top 1 当前时间,合约名称,最新价 from all2 context by 合约名称 csort 当前时间 desc ;
       update s set 合约名称=`{onruncontract}
       s
       '''
    if showsql:
        print(nowvalue)
    if first:
        nowvalue=con.run(nowvalue)
        nowvalue=nowvalue.to_dict('record')
    # tq=con.run("tq")

    # print(res)
    ###集成了xgboost算法
    xgb=load_xgboost(con=con,allcode=res['合约名称'].to_list(),st=(ed-datetime.timedelta(seconds=10*60)),ed=ed)
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
    # ins=['i', 'bb', 'v', 'wr', 'LR', 'rb', 'eb', 'fu', 'eg', 'jd', 'lu', 'ni', 'WH', 'p', 'c', 'MA', 'pb', 'lh', 'bu', 'SM', 'PF', 'ru', 'OI', 'SA', 'CF', 'UR', 'ss', 'IC', 'CJ', 'JR', 'T', 'bc', 'rr', 'SR', 'y', 'IH', 'hc', 'RM', 'IF', 'AP', 'cs', 'a', 'm', 'RS', 'l', 'j', 'PM', 'sp', 'cu', 'pp', 'nr', 'b', 'TA', 'sc', 'jm', 'SF', 'ag', 'TF', 'sn', 'RI', 'zn', 'FG', 'al', 'TS', 'au', 'CY', 'fb', 'ZC', 'PK', 'pg']
    # ins=['cf209','rm201','ma202','pvc2201',"rb2201","m2201",'fu2201',"zn2201",'p2201']#[2:4]
    # insrtuments=['AP111','zn2205']

    con.upload({"ins":futures_base.ins,
                'flag':8
                })
    st,ed=pre_con(st=(now-datetime.timedelta(days=0,hours=10,minutes=0,seconds=135)),ed=(now+datetime.timedelta(hours=0,minutes=0)))
    ###macd_inverse
    res,nowvalue,xgb=run(st,ed,onruncontract='m2205')
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
    print(flagres)
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
                buy(contractname=i.get('合约名称'),currentprice=lastprice.get(i.get('合约名称'))+买滑点,flag=1)
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
                sell(contractname=i.get('合约名称'),currentprice=lastprice.get(i.get('合约名称'))-卖滑点,flag=1)
                email=True

        res.append(spread)
        res.append(handstate)
        # res.append(xgb)

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
    f=lambda x:''.join( i[0][0] if not re.match('.*?\d+.*',i[0]) else i[0] for i in pypinyin.pinyin(x, style=pypinyin.NORMAL))\
        .replace('dp','m')  ##中文合约名转换
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
    PD='豆粕'
    fetchins.update({"data":文字2拼音(con.run(f"""
            login(`admin,`123456)
db=database('dfs://jerry');
futuresbase=loadTable(db,`futuresbase)
    all1=select trim(商品名称)+trim(交割月份) as `合约,*  from futuresbase where 时间 >=2022.01.26 and 时间<=2023.01.01  and 商品名称 =  "{PD}"  order by 时间 asc ,交割月份 desc 
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
    sched1.add_job(main,'interval',seconds=7)
    try:
        sched1.start()
    except Exception as e:
        pass







