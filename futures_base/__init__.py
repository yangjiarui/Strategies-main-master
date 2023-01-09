# -*- encoding: utf-8 -*-
"""
@File    : config.py
@Time    : 2021/9/18 8:56
@Author  : handsomejerry
@Email   : 834235185@qq.com
@Software: PyCharm
"""
import json
import re

import demjson
import numpy as np
import os

pathlog=os.path.join(os.getcwd(),'futures_base')
pathlog=os.path.join(pathlog,'log')
print(pathlog)
pathlog=os.path.join(pathlog,'trading.json')
if os.path.exists(pathlog):
    with open(pathlog,'r',encoding='utf-8') as f:
        ress=f.readlines()
# r=re.findall("([[].*?[]])",ress[-1].strip())[-1]
try:
    ress=demjson.decode(ress[-1],'utf-8')
except:
    r=re.findall("([[].*?[]])",ress[-1].strip())[-1]
    try:
        ress=demjson.decode(txt=r,encoding= 'utf-8')
    except:
        ress=json.loads(r,encoding= 'utf-8')
res=ress if ress else {}
print('initmoney',res)

反向初始化={}
try:
    [反向初始化.update({'sellclose' if v.get('buy_position') else 'buyclose':k}) for k,v in res[0].items() if isinstance(v,dict)]
except:
    pass

try:
    currentdiff=res[-1].get('当前价差',0)
except:
    pass

# futures_base.currentdiff=abs(价差)
# ins=["al2201",'al2202']  # zn2201  #m2201
# ins=['jd2201','jd2202']
fetchins={}
ins=['l2305','jd2305','c2305','m2305','RM301','lh2303','j2305','sp2305','y2305','rb2305','i2305','AP305']
# ins=['SZ','SH']
# ins=['j2201','pvc2201',"rb2201","m2201","zn2201",'p2201']
##已下是参数配置信息
# DBhost='server.natappfree.cc'
# DBport=38768
# DBhost='116.235.91.80'
DBhost='host.docker.internal'
# DBhost='192.168.1.12'
DBport1=8902
DBport2=8903
# 初始金额=1000000
初始金额=int(20000000/7)

##交易价格损失
卖滑点=1
买滑点=1
赚=3
点位=0
# currentdiff=0

# 点位=0
是否下单=True

email=False
first=True
配对跨月交易flag=True


spread={}

maxloss=1000000000
handstate={
    # "buy_position":0,
    # "sell_position":0
}
last =np.array([])
# lastprice={}



###以下是中间变量
xgb_res=None
lastprice={}
lastflag={}
继续交易flag={}
实时显示配对平仓结果={}


from .xgboostclf import *
from .exceutor import *
from .CTP_get import *