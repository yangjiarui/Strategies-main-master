# -*- encoding: utf-8 -*-
"""
@File    : 余弦相似度_tick.py
@Time    : 2021/2/13 13:02
@Author  : handsomejerry
@Email   : 834235185@qq.com
@Software: PyCharm
"""
import argparse
import asyncio
import datetime
import random
import re

# import tushare as ts
import time
import pandas as pd
import dolphindb as ddb
#import matplotlib.pyplot as plt
# from apscheduler.schedulers.blocking import BlockingScheduler
import numpy as np

# from adboostclf import load_adboost
#from XGBoostreg import XGB_score
from tqdm import tqdm

# sched1=BlockingScheduler()
from itertools import combinations, permutations

from futures_base.xgboostclf import load_xgboost

con=ddb.session()
con.connect('localhost',8921,'admin','123456')
# con.connect('192.168.1.13',8922,'admin','123456')
con.run('''
        login(`admin,`123456)
        db=database('dfs://jerry');
        ''')

class seek_simi(object):
    # def __init__(self):
    #     self.con=ddb.session()
    #     self.con.connect('localhost',8848,'admin','123456')
    #     self.con.run('''
    #     login(`admin,`123456)
    #     db=database('dfs://jerry');
    #     ''')

    def pre_table(self,begin,end):
        # global con
        self.begin=begin
        self.end=end
        w=f'''
    if (  not existsTable('dfs://jerry',`Fcosine) ){{ new= db.createPartitionedTable(table(100000000:0,`codepair`time`cosvalue`pairs`corrlation,[STRING,TIMESTAMP,DOUBLE,SYMBOL,DOUBLE]  ),`Fcosine,`time`pairs)  }}else {{ 
   //  dropTable(db,`Fcosine); 
     new=loadTable(db,`Fcosine)
     }}
futures=loadTable(db,`futures);
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
all=select  *,minute(当前时间) as `到分钟 ,timestamp(当前时间) as `int_当前时间,最新价 from futures where 当前时间>={self.begin} and 当前时间 <={self.end}  and 种名简称 = 品种名 and  (  insrtument.count()==0 || 合约名称 in insrtument ) order by 当前时间 asc
nullFill!(all,0)
return  all 
}};
  addFunctionView(pocess_ins_vol);
all2=pocess_ins_vol(futures,ins[0])      //select  * from futures where 日期<=2021.12.21  and 当前时间>={{st}} and 当前时间 <={{ed}} and 种名简称 = ins[0]
def mergetables( futures,mutable f ,lis ){{
	for ( i in lis){{	
	try{{
i=pocess_ins_vol(futures,i)               //select  * from futures where 日期<=2021.12.21  and 当前时间>={{st}} and 当前时间 <={{ed}} and 种名简称 = i
		f.append!(i) }}catch(ex){{}}
		}}
	}}
mer=mergetables{{ futures,all2}}
ploop(mer, ins[1:]  );
        '''
        # print(w)
        con.run(w)
        # print(1)


    def insert_dolphin_def(self,diff):
        con.run('''
     //   db=database('dfs://jerry');
     //   ticks=loadTable(db,`ticks)
        if((exec count(*) from getFunctionViews() where name="mycosin")==1){{  dropFunctionView("mycosin")  }};
        go;
        def mycosin(r1,r2){{
        x1=   matrix(r1)
        x2=matrix(r2)
        n1=transpose(x1) dot x2
        n2=sqrt( sum( pow( x1-x2 ,2 ) )[0])
        //if (n2==0){{  return  n1/n2     }}else{{
        return    n1/n2 
        }};
                    addFunctionView(mycosin);  
if((exec count(*) from getFunctionViews() where name="mymoving")==1){{  dropFunctionView("mymoving")  }};
go;
def mymoving(s){{
diff={diff}
wa=select 代码,时间,收盘价,(最高价-最低价) as 价格差异 from daybar where 时间>=2020.10.01 and 交易所 in `SZ`SH;
wa=select * from wa where 代码=s
	wa=select 代码[diff-1:],时间[diff-1:],收盘价[diff-1:],价格差异[diff-1:],rolling( mean,收盘价,diff,1) as `收盘_roll,rolling( std,收盘价,diff,1) as `收盘_roll_std,rolling( mean,价格差异,diff,1) as `价格差异_roll,rolling( std,价格差异,diff,1) as `价格差异_roll_std from wa // where 代码 =s
	wa=select top 1 * from wa context by 代码_at csort 时间_at desc ;    
if ( wa.收盘价_at<( wa.收盘_roll+1.165*wa.收盘_roll_std) ){{
	if (wa.价格差异_at<wa.价格差异_roll+0.85*wa.价格差异_roll_std ){{
		if ( wa.收盘价_at>wa.收盘_roll ){{ return 'up' }} else{{ return 'down'   }}  
		}}else {{ return 'unkonwn'}}
		}}else{{ return 'unkonwn'}}

}};
addFunctionView(mymoving); 
if((exec count(*) from getFunctionViews() where name="corrmatrix")==1){{  dropFunctionView("corrmatrix")  }};
go;
def corrmatrix( mutable a  ){{  a=a[:,1:]
a=matrix(a)
a=table(a)
//st=transpose(s)
std_s= ploop(std,a.values())
//n_s=ploop(count,a.values())-1
std_s=matrix(std_s)
std_s=(transpose(std_s) dot std_s)
//std_s=pow(std_s,2)
mean_s= ploop(avg,a.values())
 ass=(a.values() -mean_s )
 s=matrix(ass)
sT=transpose(s)
cov_marix=(sT dot s) / a.count()
cor=cov_marix/std_s
 factor= ( 1 / cor[0,0])
return cor*factor   }};
addFunctionView(corrmatrix);                         
        '''.format(diff=diff))
        print('functions inserted')
    def caculations(self,all=True,x1=None,x2=None):
        con.run('''
        login(`admin,`123456)
        db=database('dfs://jerry');
        new=loadTable(db,`Fcosine);
        ''')
        q1='''       
rawbase= select mean(最新价) from all2 group by 合约名称,到分钟  ;
a2=select avg_最新价 from rawbase pivot by 到分钟 as 时间,合约名称  
         
nullFill!(a2,0)
new=loadTable(db,`Fcosine)
a=matrix(a2[:,1:])
corrmatrix=pcross(corr,a)
eur_a=table(pow(a,2))
eur_a= ploop(sum,table(eur_a).values())
eur_a=matrix( pow(eur_a,0.5) )
分子=( transpose(a) dot a )
分母= ( transpose( eur_a) dot eur_a   )
s=acos( 分子/分母 )
def hhh(s ) {{ return   s*(180/pi)   }}
角度=hhh(s)
undef( `a`eur_a`分子`分母`s)
names=a2.columnNames()[1:]
last_time=a2[`时间].last()
 角度=flatten( 角度) 
 corrmatrix=flatten( corrmatrix) 
 
s1=select * from table(names)
pairs=cj(s1,s1)
pairs=select (names+"_"+s1_names) as `codepair , substr(names,0, names.regexFind("[0-9]") )+"_"+substr(s1_names,0 ,  s1_names.regexFind("[0-9]") )  as `pairs from pairs
//undef( `s1`names`)
heighty=pairs.count()/2

cosin=table( pairs[:heighty,0] as `a , pairs[:heighty,1] as `b , 角度[:heighty] as`cosvalue,corrmatrix[:heighty] as `corrlation  )

cosin.count()

        '''.format(bd=self.begin,ed=self.end)
        # print(q1)
        count,diff=con.run(q1),2
        count=con.run(f'''
cosin=select top {count} codepair, timestamp({obj.end}) as `time ,cosvalue,pairs,corrlation  from cosin where abs(corrlation)>0.35 
cosin.count() ''')
        print('待插入Fcosine:',count)

        for i in tqdm(range(0,count-diff,diff)):
            con.run(f'''
            try{{
new.append!(cosin[{i}:{i+diff},:])}} catch(ex){{}}
            ''')
        con.run('''
        clearAllCache();
        undef( `s1`names`a2`pairs`cosin`corrmatrix`角度)
        ''')
        print('全部计算插入完毕!!')
        time.sleep(2)

    def find(self,diff):
        con.run(f'''
        login(`admin,`123456)
        db=database('dfs://jerry');
         new2=loadTable(db,`Fcosine);
        daybar=loadTable(db,`daybar);
        diff={diff}
        ''')
        q='''
       // ttt=select top 1 * from  loadTable(db,`Fcosinetemp3)  
//if ( existsTable("dfs://jerry",`Fcosinetemp3)){{
//dropTable(db,`Fcosinetemp3);
//delete from loadTable(db,`Fcosinetemp3);
//temptb= loadTable(db,`Fcosinetemp3)
//temptb= db.createTable(table(100000000:0,`codepair`time`corrlation`flag,[STRING,TIMESTAMP,DOUBLE,DOUBLE]  ),`Fcosinetemp3) 
//	}}else{{
//		temptb= db.createTable(table(100000000:0,`codepair`time`corrlation`flag,[STRING,TIMESTAMP,DOUBLE,DOUBLE]  ),`Fcosinetemp3) 
//	}}
temptb=table(100000000:0,`codepair`time`corrlation`flag,[STRING,TIMESTAMP,DOUBLE,DOUBLE]  )
def insert_temp( mutable temptb,new,st ){{    
try{{	
	sss=select top 10000000  codepair,time, corrlation, ( cosvalue  - moving(first, cosvalue  , 2) ) as flag ,count(*) as `num   from new where  time>={bd} and time<={ed} and  pairs =st context by codepair csort time;
	//share sss as sharetb
	m=select mean(num) from sss group by codepair 
        m_num=avg(m[`avg_num])
v=`m
mem(true)
clearAllCache()
undef( v) 
sss=select codepair,time,corrlation,flag from sss where (num>=m_num>=8 and flag<0) or (num>=m_num>=8 and flag>0)
//sss=select * from sss where num>=m_num;
//sss=select * from sss where flag<0 or flag>0
sss.nullFill!(0)
temptb.append!( sss );
mem(true)
clearAllCache()
 undef( `sss)        
 }} catch(ex) {{ }}	
	}}

qwe= insert_temp{{ temptb ,loadTable(db,`Fcosine) }}
ploop(qwe,  {pairsins}  )

res=select top 10000 time ,  flag,std(flag), sum(flag),  avg( corrlation) ,  codepair, mean(flag)+1.05*std(flag) as upper,mean(flag)-1.05*std(flag) as lowwer 
 from temptb
 context by codepair csort time //desc
   
res.nullFill!(0)
res=select * from res where std_flag>0 
res=select *,iif(flag>upper,'变大' , iif( flag<lowwer ,'变小','ujty')  )  as `er from res 
res=select * from res  where er in `变大`变小`  order by  avg_corrlation desc,sum_flag desc,std_flag and time >=2021.10.14
res=select top 1 * from res context by codepair csort time desc;

wa=select  合约名称,到分钟,最新价,(最高价-最低价) as 价格差异 from all2 //where 当前时间>=2020.10.01 and 交易所 in `SZ`SH ;
diff=3
wa=select top 1000000000    合约名称, 到分钟,最新价,价格差异,  moving(mean,[最新价],diff )   as `收盘_roll , moving( std,最新价,diff,1) as `收盘_roll_std,moving( mean,价格差异,diff,1) as `价格差异_roll,moving( std,价格差异,diff,1) as `价格差异_roll_std
  ,moving(mean,最新价,4) as `近期收盘价均值,moving(std,最新价,2) as `近期收盘价标准差
  from wa context by 合约名称 csort 到分钟  
wa=select *,"" as `flag, ( 最新价< 收盘_roll+1.165*收盘_roll_std ) as `judge1, ( 价格差异<价格差异_roll+0.85*价格差异_roll_std ) as `judge2 ,  最新价>收盘_roll as `judge3 from wa
update wa set flag=`up where judge1==1 and judge2==1 and judge3==1
update wa set flag=`down where judge1==1 and judge2==1 and judge3==0
wa=select top 1 * from wa context by 合约名称 csort 到分钟 desc ;
wa=select * from wa where flag !=""

rescodepair=select substr(codepair,0, codepair.regexFind("_") ) as `code1 , substr(codepair,codepair.regexFind("_")+1 ) as `code2,time 
from res
//code1`direct1`code2`direct2
wa=lj( lj(rescodepair,wa,`code1,`合约名称 ) ,wa,`code2,`合约名称  )
wa=select * from wa where wa_judge3 =1 or wa_judge3 =0
//delete from loadTable(db,`Fcosinetemp3);
select code1,flag as `direct1 ,code2,wa_flag as `direct2 ,最新价 as `code1收盘价 ,近期收盘价均值 as `code1近期收盘价均值 ,近期收盘价标准差 as `code1近期收盘价标准差  ,  wa_最新价 as `code2收盘价, wa_近期收盘价均值 as `code2近期收盘价均值 ,wa_近期收盘价标准差 as `code2近期收盘价标准差 from wa where flag != ""                
'''.format(bd=self.begin[:10]+'T00:00:00',ed=self.end,pairsins=self.lookpairs)
        # print(q)
        res2=con.run(q)
        res1=con.run('''res''')
        # res2=con.run('''f''')
        res2['codepair']=res2['code1']+'_'+res2['code2']
        # print(res1)
        res=pd.merge(res1,res2,how='inner',on=['codepair'])
        res=res.dropna()
        # print(res)
        return res
    def look(self,df,df_sum):
        f={}
        try:
            # f['time']=df['time'].values[0].__str__()[:19]
            for _,i in df.iterrows():
                buy,sell=i['decision'].split(' and ')
                if 'buy' in buy:
                    buy,sell=buy,sell
                else:
                    buy,sell=sell,buy
                try:
                    # f[buy.split(':')[-1]]+=(i['flag']/df_sum)
                    f[buy.split(':')[-1]] += (1)
                except Exception as e:
                    f[buy.split(':')[-1]]=(1)
                try:
                    f[sell.split(':')[-1]]-=(1)
                except Exception as e:
                    f[sell.split(':')[-1]]=-(1)
            f=pd.Series(f).sort_values(ascending=False)
            f.name=df['time'].values[0]
            #print(f)
            fnew=pd.DataFrame({'code':f.index.tolist(),'pairscore':f})
            # res=XGB_score(con=con,codes=fnew.iloc[:4,:]['code'].to_list(),date=f.name.__str__()[:10])
            print(self.end.replace('.','-'),f.name.__str__()[:10])
            if self.end.replace('.','-')>=f.name.__str__()[:19]:
                #adb=load_adboost(con=con,allcode=fnew.iloc[:,:]['code'].to_list(),date=f.name.__str__()[:10])
                xgb=load_xgboost(con=con,allcode=fnew.iloc[:,:]['code'].to_list()
                                 ,st=pd.to_datetime(obj.begin),ed=pd.to_datetime(obj.end))
                res=pd.merge(fnew,xgb,on='code',how='left')
                res.index=res['time']
                try:
                    res['score']=res['score'].astype('float64')
                except:
                    pass
                res.drop('time',axis=1,inplace=True)
                # con.upload({
                #     "pair_res":res
                # })
                # res.to_csv('/mnt/d/emidata/myself/mystock_final_now/mystock_final_now/配对交易/日度级别/res/adboost_%s.csv'%df['time'].values[0].__str__()[:10])
                buy=res.query('score>=0.5 & pairscore>=0.5 & flag==1 ')
                sell = res.query('score>=0.5 & pairscore<0.3 & flag==0 ')
                print(sell)
                # con.run('share pair_res as pair_resuat')
            else:
                print('没有adboost')
            return df
        except:
            return

def jujde(x):
    overprice,underprice=None,None
    if abs(x['avg_corrlation'])<0.8 or x['code1']==x['code2']:
        return
    try:
        add1=x['code1收盘价']-(x['code1近期收盘价均值']+0.55*x['code1近期收盘价标准差'])
        add2=x['code2收盘价']-(x['code2近期收盘价均值']+0.55*x['code2近期收盘价标准差'])
        diff1=x['code1收盘价']-(x['code1近期收盘价均值']-0.455*x['code1近期收盘价标准差'])
        diff2=x['code2收盘价']-(x['code2近期收盘价均值']-0.555*x['code2近期收盘价标准差'])
        if (add1>add2) :
            overprice = x['code1']
            underprice=x['code2']
        elif  (diff1<diff2):
            overprice = x['code2']
            underprice = x['code1']
        # if x['direct1']=='up':
        #     overprice=x['code1']
        #     underprice=x['code2']
        # else:
        #     overprice=x['code2']
        #     underprice=x['code1']
        if (not overprice) or (not underprice):
            return
        if x['avg_corrlation']>0:
            if x['er']=='变小':
                return #'sell:'+overprice+' and buy:'+underprice
            elif x['er']=='变大':
                return 'buy:'+underprice+' and sell:'+overprice
        else:
            # if x['er']=='变小':afjl/k;d66666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666afjlk;dafjl/k;d66666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666afjlk;dafjl/k;d6666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666/afjlk;dafffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffafjlk;dkkkkkkkkkkkkkkaakfjl;dkkkkkkkkkkkafjllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllafjlkkaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaafjlk;dafjl/k666666afjlk;dafjl/k;d666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666afjlk
            #     return 'sell:'+overprice+' and buy:'+underprice
            # else:
            if x['er']=='变小':
                return 'sell:'+underprice+' and buy:'+overprice
            elif x['er']=='变大':
                return #'buy:'+underprice+' and sell:'+overprice
    except:
        return

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="pair_generate")        # 这些参数都有默认值，当调用parser.print_help()或者运行程序时由于参数不正确(此时python解释器其实也是调用了pring_help()方法)时，
    # 会打印这些描述信息，一般只需要传递description参数，如上。
    help = "like 2022.04.01"
    parser.add_argument('--date',help = help)                   # 步骤三，后面的help是我的描述
    args = parser.parse_args()
    obj=seek_simi()
    end_date=args.date
    obj.begin=(pd.to_datetime(end_date)-pd.to_timedelta('35min')).strftime('%Y.%m.%dT%H:%M:%S')
    obj.end=end_date
    ins=['l2209','jd2209','c2209','m2209','RM209','sp2209','y2209','rb2209']

    lookpairs=['_'.join(i) for i in permutations((re.findall('\D+',i)[-1] for i in ins),2)]
    obj.lookpairs="`"+'`'.join(lookpairs)
    while 1:
        con.upload({
            "ins":ins
        })
        # time.sleep(5)
        obj.pre_table(obj.begin,obj.end)  ##当天前三天
        obj.insert_dolphin_def(diff=5)
        # con=ddb.session()
        # port=random.choice([8922,8921])
        # con.connect('localhost',port,'admin','123456')
        # # con.connect('192.168.1.13',8922,'admin','123456')
        # con.run('''
        #         login(`admin,`123456)
        #         db=database('dfs://jerry');
        #         ''')
        print(obj.begin,obj.end)
        flaa=con.run(f'select top 1 * from new where pairs in {obj.lookpairs} and time=timestamp({obj.end})')
        if pd.to_datetime(obj.end).dayofweek in [5,6]:
            # obj.begin=(pd.to_datetime(obj.begin)+datetime.timedelta(days=1)).strftime('%Y.%m.%d')
            # obj.end=(pd.to_datetime(obj.end)+datetime.timedelta(days=1)).strftime('%Y.%m.%d')
            continue
        try:
            if not flaa.empty:
                print('已计算过,跳过计算',obj.end)
#                 con.run('''
# v=objs().name
# mem(true)
# clearAllCache()
# undef( v)
#            ''')
                res=obj.find(diff=5)
            else:
                df=obj.caculations(all=True)
                # con.run('''
# v=objs().name
# mem(true)
# clearAllCache()
# undef( v)
#            ''')
                res=obj.find(diff=5)
            # df=obj.caculations(all=True)
            # df=obj.caculations(all=False,x1='sz000001',x2='sh600809')
            # res=obj.find()
            # print(res)
            if not res.empty:
                res['decision']=res.apply(jujde,axis=1)
                # print(res[['time','decision','avg_corrlation']].head(20))
                res=res[res['decision'].notnull()]
                # print(res)
                # res.groupby('time').apply(obj.look,res['avg_corrlation'].sum())
                res.groupby('time').apply(obj.look, res['flag'].sum())
        except Exception as e:
            print(e.__str__())
        try:
            con.run('''
v=objs().name
mem(true)
clearAllCache()
undef( v)           
           ''')
            con.close()
            print("关闭dolphin cons")
        except:
            pass
        time.sleep(3)
        con=ddb.session()
        port=random.choice([8922,8921])
        con.connect('localhost',port,'admin','123456')
        # con.connect('192.168.1.13',8922,'admin','123456')
        print("打开con")
        con.run('''
                login(`admin,`123456)
                db=database('dfs://jerry');
             new=loadTable(db,`Fcosine)
                ''')
        # pd.to_datetime('2015.01.01')
        obj.begin=(pd.to_datetime(obj.begin)+pd.to_timedelta('1min')).strftime('%Y.%m.%dT%H:%M:%S')
        obj.end=(pd.to_datetime(obj.end)+pd.to_timedelta('1min')).strftime('%Y.%m.%dT%H:%M:%S')
        if obj.end>datetime.datetime.now().strftime("%Y.%m.%dT%H:%M:%S"):#'2022.03.11':
            con.run('''
v=objs().name
mem(true)
clearAllCache()
undef( v)           
           ''')
            con.close()
            print("关闭dolphin cons")
            break

    # sched1.add_job(main,'interval',seconds=6)
    # sched1.start()

    # df=obj.caculations(all=False,x1='sz000001',x2='sh600809')
    # resjld


    #=obj.find()
