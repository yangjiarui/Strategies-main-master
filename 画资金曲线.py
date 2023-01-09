# -*- encoding: utf-8 -*-
"""
@File    : 画资金曲线.py
@Time    : 2022/3/15 16:29
@Author  : handsomejerry
@Email   : 834235185@qq.com
@Software: PyCharm
"""
from pprint import pprint
import matplotlib.pyplot as plt
import pandas as pd
all=[]
赚=3
df=pd.read_csv('res.csv')
df['当前时间']=pd.to_datetime(df['当前时间'])
for i in df['合约名称'].unique():
    c=df[df['合约名称']==i]
    equity=0
    openflag=True
    state={}
    f=[]
    print(i+'\n')
    for index,row in c.sort_values(by='当前时间',ascending=[1]).iterrows():
        if ((row['当前时间'].hour<=10) and (row['当前时间'].minute<=40)):
            continue
        if openflag and (row['操作']!='TODO'):
            if row['操作']=='sell':
                equity+=row['最新价']
                state.update({
                    "sell":1,
                   "sellprice":row['最新价']
                              })
            elif row['操作']=='buy':
                equity-=row['最新价']
                state.update({"buy":1,
                              "buyprice":row['最新价']})
            openflag=False
        else:
            if state.get("sell"):
                if (state.get("sellprice")-row['最新价'])>赚:
                    equity-=row['最新价']
                    openflag,state=True,{}
            elif state.get("buy"):
                if (row['最新价']-state.get("buyprice"))>赚:
                    equity+=row['最新价']
                    openflag,state=True,{}
        f.append(equity)
    ##强行平仓
    if state.get("sell"):
        # if (state.get("sellprice")-row['最新价'])>2:
        equity-=state.get("sellprice")
        openflag,state=True,{}
    elif state.get("buy"):
        # if (row['最新价']-state.get("buyprice"))>2:
        equity+=state.get("buyprice")
        openflag,state=True,{}
    f.append(equity)
    print(equity)
    plt.plot(f)
    plt.show()
    all.append(equity)
print("一共",sum(all))
        # pass
