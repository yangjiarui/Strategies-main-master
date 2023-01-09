# -*- encoding: utf-8 -*-
"""
@File    : exceutor.py
@Time    : 2021/9/18 11:05
@Author  : handsomejerry
@Email   : 834235185@qq.com
@Software: PyCharm
"""
import copy
import datetime
import os
import sys
import time
from futures_base.CTP_get import MyTradeonj#.t.ReqOrderInsert()
import json
import numpy as np
import futures_base
from py_ctp.enums import DirectType,OffsetType
from jsonpath_ng import parse
from . import handstate, spread, 是否下单,赚
from .CTP_get import VolumeMultiple
from . import lastprice ,lastflag,卖滑点,买滑点,初始金额,继续交易flag
# from macd_inverse_new import spread,last,lastspread,handstate,maxloss

def buy(contractname,currentprice,flag='close'):
    '''
     DirectType 卖:1   买:0
    OffsetType  开仓: 0    平仓:1
    '''
    if (not currentprice) :
        return
    if flag=='close' :
        a=handstate[contractname]['sell_price']
        have_to=handstate[contractname]['sell_position']
        ###凯利公式
        # p_up=0
        # for i in range(len(a)-1):
        #     if a[i+1]<a[i]:
        #         p_up+=1
        # p_up/=handstate[contractname]['最近平仓卖出到现在计数']
        # if len(a)==1:
        #     p_up=1
        # p_down=1-p_up
        # odd=(currentprice*have_to)/np.sum(a)
        # interest=(odd*p_up-p_down)/odd
        # if interest<0:
        #     have_to=1
        # else:
        #     have_to=int(have_to*interest)
        #
        # # handstate[contractname]['buy_position']+=1
        # have_to=(1 if not have_to else have_to)
        spread[contractname]-=float( currentprice*have_to)#*VolumeMultiple.get(contractname).VolumeMultiple )
        # [handstate.update({i.get("合约名称"):{'buy_position':0,
        #                                   "sell_position":0,
        #                                   # "price":i.get('最新价')
        #                                   }}) for i in res]
        # handstate[contractname]={'buy_position':0,"sell_position":0}
        # handstate[contractname]={'buy_position':0,
        #                          'sell_position':handstate[contractname].get("sell_position")-have_to
        # 清掉部分仓位
        if handstate[contractname]['sell_position']!=have_to:
            handstate[contractname]['buy_position']=0
            handstate[contractname]['sell_position']=handstate[contractname].get("sell_position")-have_to
        else:
            if 是否下单:
                MyTradeonj.t.ReqOrderInsert(pInstrument=contractname,pDirection=DirectType(0),pOffset=OffsetType(1),pPrice=currentprice, pVolume=have_to)
            handstate[contractname]={'buy_position':0,"sell_position":0,
                                     "sell_price":[],"buy_price":[]}

    elif flag=='凯莉':
        ###计算凯莉仓位
        if futures_base.xgb_res.get("flag")==0:
            p_down=0.5*float(futures_base.xgb_res.get("score"))
            p_up=1-p_down
            exdown=currentprice* float( futures_base.xgb_res.get("score"))
            exup=0.000001
        else:
            p_up=0.5* float( futures_base.xgb_res.get("score"))
            p_down=1-p_up
            exup=currentprice * float( futures_base.xgb_res.get("score"))
            exdown=0.000001
        try:
            odd=exup/exdown
        except:
            odd=1
        interest=(odd*p_up-p_down)/odd
        if interest<0:
            have_to=1
        else:
            have_to=int((spread.get(contractname)//currentprice)*interest)
        ####
        # remaining=spread.get(contractname)//currentprice
        # have_to=2
        have_to=(1 if not have_to else have_to)
        handstate[contractname]['buy_position']+=have_to
        if handstate[contractname].get('最近平仓买入到现在计数'):
            pass
        else:
            handstate[contractname]['最近平仓买入到现在计数']=1
        try:
            handstate[contractname]['buy_price'].append( currentprice )
        except:
            handstate[contractname]['buy_price']=[currentprice]
        # handstate[contractname]['buy_price']/=handstate[contractname]['buy_position']
        if 是否下单:
            MyTradeonj.t.ReqOrderInsert(pInstrument=contractname,pDirection=DirectType(0),pOffset=OffsetType(0),pPrice=currentprice, pVolume=have_to)
        spread[contractname]-=float(currentprice*have_to)#*VolumeMultiple.get(contractname).VolumeMultiple)
    else:
        have_to=(1 if not flag else flag)
        handstate[contractname]['buy_position']+=have_to
        if handstate[contractname].get('最近平仓买入到现在计数'):
            pass
        else:
            handstate[contractname]['最近平仓买入到现在计数']=1
        try:
            handstate[contractname]['buy_price'].append( currentprice )
        except:
            handstate[contractname]['buy_price']=[currentprice]
        # handstate[contractname]['buy_price']/=handstate[contractname]['buy_position']
        if 是否下单:
            # print("下单")
            MyTradeonj.t.ReqOrderInsert(pInstrument=contractname,pDirection=DirectType(0),pOffset=OffsetType(0),pPrice=currentprice, pVolume=have_to)
        spread[contractname]-=float(currentprice*have_to)#*VolumeMultiple.get(contractname).VolumeMultiple)

def sell(contractname,currentprice,flag='close'):
    # MyTradeonj.t.ReqOrderInsert()
    if not currentprice:
        return
    if flag=='close':
        # handstate[contractname]['sell_position']=0
        a=handstate[contractname]['buy_price']
        have_to=handstate[contractname]['buy_position']
        ###凯利公式
        # p_up=0
        # for i in range(len(a)-1):
        #     if a[i+1]>a[i]:
        #         p_up+=1
        # p_up/=handstate[contractname]['最近平仓买入到现在计数']
        # if len(a)==1:
        #     p_up=1
        # p_down=1-p_up
        # odd=(currentprice*have_to)/np.sum(a)
        # interest=(odd*p_up-p_down)/odd
        # if interest<0:
        #     have_to=1
        # else:
        #     have_to=int(have_to*interest)
        # have_to=(1 if not have_to else have_to)
        spread[contractname]+=float(currentprice*have_to)#*VolumeMultiple.get(contractname).VolumeMultiple)
        # handstate[contractname]={'sell_position':0,
        #                          'buy_position':handstate[contractname].get("buy_position")-have_to
        #                          }
        if handstate[contractname]['buy_position']!=have_to:
            handstate[contractname]['sell_position']=0
            handstate[contractname]['buy_position']=handstate[contractname].get("buy_position")-have_to
        else:
            if 是否下单:
                # print("下单")
                MyTradeonj.t.ReqOrderInsert(pInstrument=contractname,pDirection=DirectType(1),pOffset=OffsetType(1),pPrice=currentprice, pVolume=have_to)
            handstate[contractname]={'buy_position':0,"sell_position":0,
                                     "sell_price":[],"buy_price":[]}
            # MyTradeonj.t.ReqOrderAction

    elif flag=='凯莉':
        ###计算凯莉仓位
        if futures_base.xgb_res.get("flag")==0:
            p_up=0.5*float(futures_base.xgb_res.get("score"))
            p_down=1-p_up
            exup=currentprice* float( futures_base.xgb_res.get("score"))
            exdown=0.000001
        else:
            p_down=0.5* float( futures_base.xgb_res.get("score"))
            p_up=1-p_down
            exdown=currentprice* float( futures_base.xgb_res.get("score"))
            exup=0.000001
        try:
            odd=exup/exdown
        except:
            odd=1
        # if odd<0:
        #     pass
        interest=(odd*p_up-p_down)/odd
        if interest<0:
            have_to=1
        else:
            have_to=int((spread.get(contractname)//currentprice)*interest)
        ####
        # have_to=2
        have_to=(1 if not have_to else have_to)
        handstate[contractname]['sell_position']+=(1 if not have_to else have_to)
        if handstate[contractname].get('最近平仓卖出到现在计数'):
            pass
        else:
            handstate[contractname]['最近平仓卖出到现在计数']=1
        try:
            handstate[contractname]['sell_price'].append(currentprice)
        except:
            handstate[contractname]['sell_price']=[currentprice]
        # handstate[contractname]['sell_price']+=currentprice
        # handstate[contractname]['sell_price']/=handstate[contractname]['sell_position']
        if 是否下单:
            MyTradeonj.t.ReqOrderInsert(pInstrument=contractname,pDirection=DirectType(1),pOffset=OffsetType(0),pPrice=currentprice, pVolume=have_to)
        spread[contractname]+=float(currentprice*(1 if not have_to else have_to))#*VolumeMultiple.get(contractname).VolumeMultiple)
    else:
        have_to=(1 if not flag else flag)
        handstate[contractname]['sell_position']+=have_to
        if handstate[contractname].get('最近平仓卖出到现在计数'):
            pass
        else:
            handstate[contractname]['最近平仓卖出到现在计数']=1
        try:
            handstate[contractname]['sell_price'].append(currentprice)
        except:
            handstate[contractname]['sell_price']=[currentprice]
            # handstate[contractname]['sell_price']+=currentprice
            # handstate[contractname]['sell_price']/=handstate[contractname]['sell_position']
        if 是否下单:
            MyTradeonj.t.ReqOrderInsert(pInstrument=contractname,pDirection=DirectType(1),pOffset=OffsetType(0),pPrice=currentprice, pVolume=have_to)
        spread[contractname]+=float(currentprice*have_to)#*VolumeMultiple.get(contractname).VolumeMultiple)


def pbuy(contractname,currentprice,flag='close'):
    if (not currentprice) :
        return
    if flag=='close' :
        a=handstate[contractname]['sell_price']
        have_to=handstate[contractname]['sell_position']
        ###凯利公式
        # p_up=0
        # for i in range(len(a)-1):
        #     if a[i+1]<a[i]:
        #         p_up+=1
        # p_up/=handstate[contractname]['最近平仓卖出到现在计数']
        # if len(a)==1:
        #     p_up=1
        # p_down=1-p_up
        # odd=(currentprice*have_to)/np.sum(a)
        # interest=(odd*p_up-p_down)/odd
        # if interest<0:
        #     have_to=1
        # else:
        #     have_to=int(have_to*interest)
        #
        # # handstate[contractname]['buy_position']+=1
        # have_to=(1 if not have_to else have_to)
        spread[contractname]-=currentprice*have_to
        # [handstate.update({i.get("合约名称"):{'buy_position':0,
        #                                   "sell_position":0,
        #                                   # "price":i.get('最新价')
        #                                   }}) for i in res]
        # handstate[contractname]={'buy_position':0,"sell_position":0}
        # handstate[contractname]={'buy_position':0,
        #                          'sell_position':handstate[contractname].get("sell_position")-have_to
        # 清掉部分仓位
        if handstate[contractname]['sell_position']!=have_to:
            handstate[contractname]['buy_position']=0
            handstate[contractname]['sell_position']=handstate[contractname].get("sell_position")-have_to
        else:
            handstate[contractname]={'buy_position':0,"sell_position":0}

    else:
        ###计算凯莉仓位
        if futures_base.xgb_res.get("flag")==0:
            p_down=0.5*float(futures_base.xgb_res.get("score"))
            p_up=1-p_down
            exdown=currentprice* float( futures_base.xgb_res.get("score"))
            exup=0.000001
        else:
            p_up=0.5* float( futures_base.xgb_res.get("score"))
            p_down=1-p_up
            exup=currentprice * float( futures_base.xgb_res.get("score"))
            exdown=0.000001
        try:
            odd=exup/exdown
        except:
            odd=1
        interest=(odd*p_up-p_down)/odd
        if interest<0:
            have_to=1
        else:
            have_to=int((spread.get(contractname)//currentprice)*interest)
        ####
        # remaining=spread.get(contractname)//currentprice
        have_to=None
        have_to=(1 if not have_to else have_to)
        handstate[contractname]['buy_position']+=have_to
        if handstate[contractname].get('最近平仓买入到现在计数'):
            pass
        else:
            handstate[contractname]['最近平仓买入到现在计数']=1
        try:
            handstate[contractname]['buy_price'].append( currentprice )
        except:
            handstate[contractname]['buy_price']=[currentprice]
        # handstate[contractname]['buy_price']/=handstate[contractname]['buy_position']
        spread[contractname]-=currentprice*have_to


def psell(contractname,currentprice,flag='close'):
    if not currentprice:
        return
    if flag=='close':
        # handstate[contractname]['sell_position']=0
        a=handstate[contractname]['buy_price']
        have_to=handstate[contractname]['buy_position']
        ###凯利公式
        # p_up=0
        # for i in range(len(a)-1):
        #     if a[i+1]>a[i]:
        #         p_up+=1
        # p_up/=handstate[contractname]['最近平仓买入到现在计数']
        #
        # if len(a)==1:
        #     p_up=1
        # p_down=1-p_up
        #
        # odd=(currentprice*have_to)/np.sum(a)
        # interest=(odd*p_up-p_down)/odd
        # if interest<0:
        #     have_to=1
        # else:
        #     have_to=int(have_to*interest)
        # have_to=(1 if not have_to else have_to)
        spread[contractname]+=currentprice*have_to
        # handstate[contractname]={'sell_position':0,
        #                          'buy_position':handstate[contractname].get("buy_position")-have_to
        #                          }
        if handstate[contractname]['buy_position']!=have_to:
            handstate[contractname]['sell_position']=0
            handstate[contractname]['buy_position']=handstate[contractname].get("buy_position")-have_to
        else:
            handstate[contractname]={'buy_position':0,"sell_position":0}
    else:
        ###计算凯莉仓位
        if futures_base.xgb_res.get("flag")==0:
            p_up=0.5*float(futures_base.xgb_res.get("score"))
            p_down=1-p_up
            exup=currentprice* float( futures_base.xgb_res.get("score"))
            exdown=0.000001
        else:
            p_down=0.5* float( futures_base.xgb_res.get("score"))
            p_up=1-p_down
            exdown=currentprice* float( futures_base.xgb_res.get("score"))
            exup=0.000001
        try:
            odd=exup/exdown
        except:
            odd=1
        # if odd<0:
        #     pass
        interest=(odd*p_up-p_down)/odd
        if interest<0:
            have_to=1
        else:
            have_to=int((spread.get(contractname)//currentprice)*interest)
        ####
        have_to=None
        have_to=(1 if not have_to else have_to)
        handstate[contractname]['sell_position']+=(1 if not have_to else have_to)
        if handstate[contractname].get('最近平仓卖出到现在计数'):
            pass
        else:
            handstate[contractname]['最近平仓卖出到现在计数']=1
        try:
            handstate[contractname]['sell_price'].append(currentprice)
        except:
            handstate[contractname]['sell_price']=[currentprice]
        # handstate[contractname]['sell_price']+=currentprice
        # handstate[contractname]['sell_price']/=handstate[contractname]['sell_position']
        spread[contractname]+=currentprice*(1 if not have_to else have_to)

def 自动回撤订单():
    allorders=MyTradeonj.t.orders
    待撤销订单=[i for i in list(allorders.keys()) if (not allorders.get(i).StatusMsg.__contains__('成交')) and (not allorders.get(i).StatusMsg.__contains__('已撤单'))
                and (not allorders.get(i).StatusMsg.__contains__('仓位不足')) and (not allorders.get(i).StatusMsg.__contains__('超过'))]
    if len(待撤销订单)>=1:
        [MyTradeonj.t.ReqOrderAction(i) for i in 待撤销订单]



def finding_2_cansole_trade():
    obj=MyTradeonj.t.positions
    # MyTradeonj.t.orders  .StatusMsg
    # allorders=MyTradeonj.t.orders
    # 待撤销订单=[i for i in list(allorders.keys()) if (not allorders.get(i).StatusMsg.__contains__('成交')) and (not allorders.get(i).StatusMsg.__contains__('已撤单'))
    #        and (not allorders.get(i).StatusMsg.__contains__('仓位不足')) and (not allorders.get(i).StatusMsg.__contains__('超过'))]
    # if len(待撤销订单)>=1:
    #     [MyTradeonj.t.ReqOrderAction(i) for i in 待撤销订单]
    # k='2096927770|9|000003000000'
    # MyTradeonj.t.orders.get(k).VolumeLeft
    # MyTradeonj.t.orders.get(k).LimitPrice
    # MyTradeonj.t.orders.get(k).Direction
    # MyTradeonj.t.orders.get(k).Offset
    # MyTradeonj.t.orders.get(k).InstrumentID
    # MyTradeonj.t.afjlkction(k)
    # print("control")
    # print(handstate)
    # f={}、
    ###TODO 撤去还未成交订单
    for k,v in obj.items():
        # if handstate:
        if v.TdPosition:
            c,d=k.split("_")
            if handstate.get(c):
                pass
                # handstate[c]={'buy_position':0, 'sell_position': 0, 'sell_price': [], 'buy_price': [], '最近平仓卖出到现在计数': 0, '最近平仓买入到现在计数': 0}
            else:
                handstate[c]={'buy_position':0, 'sell_position': 0, 'sell_price': [], 'buy_price': [], '最近平仓卖出到现在计数': 0, '最近平仓买入到现在计数': 0}

            if d=='Sell':
                d2="buy"
            else:
                d2="sell"
            list_path = parse(f"$.{c}.{d2.lower()}_position")
            list_path.update(handstate,0)
            # list_path.find(handstate)[0].value
            list_path = parse(f"$.{c}.{d2.lower()}_price")
            list_path.update(handstate,[])

            list_path = parse(f"$.{c}.{d.lower()}_position")
            list_path.update(handstate,v.TdPosition)
            # list_path.find(handstate)[0].value
            list_path = parse(f"$.{c}.{d.lower()}_price")
            try:
                old=list_path.find(handstate)[0].value
            except:
                # print(handstate,list_path,c,d)
                n_path = parse(f"$.{c}")
                oldvalue=n_path.find(handstate)[0].value
                oldvalue.update({'sell_price': [], 'buy_price': [], '最近平仓卖出到现在计数': 0, '最近平仓买入到现在计数': 0})
                n_path.update(handstate,oldvalue)
                old=list_path.find(handstate)[0].value
            try:
                # if d.lower=='sell':
                #     old[-1]=v.Price-卖滑点
                # else:
                #     old[-1]=v.Price-卖滑点
                old[-1]=v.Price
            except:
                old=[v.Price]
            list_path.update(handstate,copy.deepcopy(old))
        else:
            c,d=k.split("_")
            if d=='Sell':
                d2="buy"
            else:
                d2="sell"
            list_path = parse(f"$.{c}.{d.lower()}_position")
            list_path.update(handstate,v.TdPosition)
            # list_path.find(handstate)[0].value
            list_path = parse(f"$.{c}.{d.lower()}_price")
            list_path.update(handstate,[])

            list_path = parse(f"$.{c}.{d2.lower()}_position")
            list_path.update(handstate,0)
            # list_path.find(handstate)[0].value
            list_path = parse(f"$.{c}.{d2.lower()}_price")
            list_path.update(handstate,[])
            # handstate.update({c:{'buy_position': 0, 'sell_position': 0,'sell_price':[],'buy_price':[] ,"最近平仓卖出到现在计数":0,"最近平仓买入到现在计数":0 }})

    print("更新完成")

def finding_2_trade():
    def do(contractname):
        currentprice=lastprice.get(contractname)
        if not currentprice:
            return
        if handstate[contractname]['sell_position']:
            if lastflag.get(contractname) and lastflag.get(contractname)!=currentprice:
                handstate[contractname]['最近平仓卖出到现在计数']+=1
            lastflag[contractname]=currentprice
            # if lastprice[contractname]< np.mean(handstate[contractname]['sell_price']):
            # if lastprice[contractname]< np.quantile(handstate[contractname]['sell_price'],0.75):
            if np.quantile(handstate[contractname]['sell_price'],0.85)-currentprice>(赚):
            # if np.min(handstate[contractname]['sell_price'])-currentprice>买滑点:
                buy(contractname=contractname,currentprice=currentprice+买滑点,flag='close')
                print(handstate)
                print(spread)
                #
                # 继续交易flag.update({"flag":False})
                # s=input("是否继续交易Y(N)")
                # # time.sleep(10)
                # # s="Y" if not s else
                # if s=='Y':
                #     继续交易flag.update({"flag":True})
                # elif s=='N':
                #     with open(os.path.join('log','trading.json'),'a',encoding='utf-8') as wd:
                #         json.dump([handstate,spread],wd,ensure_ascii=False)
                #         wd.write('\n')
                #     sys.exit(0)
                # # sen_email(msg_to='834235185@qq.com',content=str(c))
        if handstate[contractname]['buy_position']:
            if lastflag.get(contractname) and lastflag.get(contractname)!=currentprice:
                handstate[contractname]['最近平仓买入到现在计数']+=1
            lastflag[contractname]=currentprice
            # if lastprice[contractname]>np.mean(handstate[contractname]['buy_price']):
            if currentprice-np.quantile(handstate[contractname]['buy_price'],0.275)>(赚):
            # if currentprice-np.max(handstate[contractname]['buy_price'])>卖滑点:
                sell(contractname=contractname,currentprice=currentprice-卖滑点,flag='close')
                print(handstate)
                print(spread)
                #
                # 继续交易flag.update({"flag":False})
                # s=input("是否继续交易Y(N)")
                # # time.sleep(10)
                # if s=='Y':
                #     继续交易flag.update({"flag":True})
                # elif s=='N':
                #     with open(os.path.join('log','trading.json'),'a',encoding='utf-8') as wd:
                #         json.dump([handstate,spread],wd,ensure_ascii=False)
                #         wd.write('\n')
                #     sys.exit(0)
    # print('xdsa')
    last={}
    while 1:
        # if last == lastprice:
        #     last=lastprice
        #     continue
        # print("rb2201",lastprice.get('rb2201'))
        ######实时监测是否有smart平仓机会
        contractnames=list(handstate.keys())
        # print(contractnames)
        # last=lastprice
        if len(contractnames)>=1:
            try:
                list(map(do ,contractnames ))
            except Exception as e:
                continue

def update_计数(contractname,currentprice,dig='最近平仓卖出到现在计数'):
    if lastflag.get(contractname) and lastflag.get(contractname)!=currentprice:
        handstate[contractname][dig]+=1
    lastflag[contractname]=currentprice
#

def finding_2_trade_pair():
    def do(contractname):
        contractname1,contractname2=contractname[0],contractname[1]
        contractprice1,contractprice2=lastprice.get(contractname1),lastprice.get(contractname2)
        if (not contractprice1) or (not contractprice2):
            return
        if (handstate[contractname1]['sell_position']==handstate[contractname2]['buy_position']>=1):
            update_计数(contractname1,contractprice1,'最近平仓卖出到现在计数')
            update_计数(contractname2,contractprice2,'最近平仓买入到现在计数')
            #####如果全部平仓则:
            eqname1,eqname2=copy.deepcopy(spread[contractname1]),copy.deepcopy(spread[contractname2])
            eqname1-=(contractprice1+买滑点)*handstate[contractname1]['sell_position']
            eqname2+=(contractprice2-卖滑点)*handstate[contractname2]['buy_position']
            res=eqname1+eqname2-初始金额*2
            # print('如果全部平仓则:',res)
            if res>abs(np.min(handstate[contractname1]['sell_price'])-np.max(handstate[contractname2]['buy_price']))+(买滑点+卖滑点): #(买滑点+卖滑点)+11111111:
                buy(contractname=contractname1,currentprice=contractprice1+买滑点,flag='close')
                sell(contractname=contractname2,currentprice=contractprice2-卖滑点,flag='close')

        elif handstate[contractname1]['buy_position'] == handstate[contractname2]['sell_position']>=1:
            update_计数(contractname2,contractprice2,'最近平仓卖出到现在计数')
            update_计数(contractname1,contractprice1,'最近平仓买入到现在计数')
            #####如果全部平仓则:
            eqname1,eqname2=copy.deepcopy(spread[contractname1]),copy.deepcopy(spread[contractname2])
            eqname2-=(contractprice2+买滑点)*handstate[contractname2]['sell_position']
            eqname1+=(contractprice1-卖滑点)*handstate[contractname1]['buy_position']
            res=eqname1+eqname2-初始金额*2
            # print('如果全部平仓则:',res)
            if res>abs(np.min(handstate[contractname2]['sell_price'])-np.max(handstate[contractname1]['buy_price']))+(买滑点+卖滑点):#(买滑点+卖滑点)+1111111:
                buy(contractname=contractname2,currentprice=contractprice2+买滑点,flag='close')
                sell(contractname=contractname1,currentprice=contractprice1-卖滑点,flag='close')
    last={}
    while 1:
        # if last == lastprice:
        #     last=lastprice
        #     continue
        # print("rb2201",lastprice.get('rb2201'))
        ######实时监测是否有smart平仓机会
        contractnames=list(handstate.keys())
        # print(contractnames)
        # last=lastprice
        # if not futures_base.配对跨月交易flag.get('data'):
        #     print('正在买入')
        if len(contractnames)>=2 and futures_base.配对跨月交易flag.get('data'):
            do(contractnames)
#
# def during_print(x):
#     now=datetime.datetime.now()
#     if now.second%5 in [1,3]
#
# #
# if __name__ == '__main__':
#     pass
    # while 1:
    #     pass