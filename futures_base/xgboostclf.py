# -*- encoding: utf-8 -*-
"""
@File    : adboostclfdolphin.py
@Time    : 2021/3/12 22:45
@Author  : handsomejerry
@Email   : 834235185@qq.com
@Software: PyCharm
"""
import time
import pandas as pd
import dolphindb as ddb
from tqdm import tqdm

# con=ddb.session()
# con.connect('localhost',8910,'admin','123456')
# con.run('''
#         login(`admin,`123456)
#         db=database('dfs://jerry');
#                 daybar=loadTable(db,`daybar)
#                 if (  not existsTable('dfs://jerry',`adaboostscore) ){  fdisk= createTable(db,table(10000:0,`time`code`score`flag,[TIMESTAMP,STRING,STRING,INT]),`adaboostscore)  }else {
#      dropTable(db,`adaboostscore);
#      fdisk= createTable(db,table(10000:0,`time`code`score`flag,[TIMESTAMP,STRING,STRING,INT]),`adaboostscore)
#      fdisk=loadTable(db,`adaboostscore) }
#         ''')
def load_xgboost(allcode,con,st,ed):
    w='''
    try{loadPlugin("F:/DolphinDB_Win64_V2.00.8/server/plugins/xgboost/PluginXgboost.txt")}catch(ex){print ex}
go;
 def mynormal(flag_col,mutable wa){
        sstd=ploop(std,wa.values())
    smean=ploop(mean,wa.values())
  //  wa=table((matrix(wa)-smean)/sstd)
        wa=  table(  (wa.values()-smean)/sstd )
    try{
    update wa set flag= flag_col[`flag] } catch(ex){}
    return wa
    };
    
    def main(all,trainsize,mutable ftable,code){
        wa= select 当前时间 as `时间, 合约名称 as `代码, 种名简称 as `简称, 最新价 as `收盘价, 开盘价, 最低价, 最高价 , 持仓量 as `成交金额,  成交量  as `成交量_股 from all
        wa=select 时间 , 代码, 简称, 收盘价, 开盘价, 最低价, 最高价 , 成交金额,  成交量_股,moving(first,收盘价,2) as `cur,moving(first,开盘价,2) as `q开盘价,moving(first,最高价,2) as `q最高价,moving(first,最低价,2) as `q最低价,moving(first,成交量_股,2) as `q成交量,moving(first,成交金额,2) as `q成交金额,0 as `flag from wa where 代码 =code

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
//allq=select   *  from futures where 当前时间>={st} and 当前时间<={ed} and 种名简称  in 
//`jd`rb`zn//`鸡蛋`甲醇`铝`锌 //order by 当前时间 desc 
all2=select top 100000000 合约名称,当前时间,开盘价,最高价,最低价,买价,卖价,最新价,结算价,买量,卖量, 持仓量,成交量,种名简称,日期  
       from all2 context by 合约名称 csort 当前时间 asc；
       all2=select * from all2 where 当前时间>={st} and 当前时间<={ed};
    nullFill!(all2,0);
    '''.format(st=st.strftime("%Y.%m.%d %H:%M:%S"),ed=ed.strftime("%Y.%m.%d %H:%M:%S"))
    # print(sql)
    all=con.run(sql)
    try:
        g='''
    fq=table(1000000:0,`time`code`score`flag,[TIMESTAMP,STRING,STRING,INT]  )
mainss=main{{all2,0.725,fq}}
try{{ loop(mainss, {lis} ) }}catch(ex){{}}
   fq
    '''.format(lis='`'+'`'.join(allcode[:]))
        # print(g)
        start=con.run(g)
        return start
    except Exception as e:
        print(e.__str__())


#
# try:
#     resdf=con.run('''
# login(`admin,`123456)
#         db=database('dfs://jerry');
# fn=loadTable(db,`adaboostscores)
# select * from fn''')
#     print(resdf)
# except:
#     pass
