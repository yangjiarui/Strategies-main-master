# -*- encoding: utf-8 -*-
"""
@File    : script.py
@Time    : 2021/9/9 19:45
@Author  : handsomejerry
@Email   : 834235185@qq.com
@Software: PyCharm
"""
import re
import pandas as pd


def response(flow):
    #https://mp.weixin.qq.com/s?__biz=MjM5MDA4MzU2MA==&mid=2658313317&idx=1&sn=04610f082a05f269f2d360f4da5c5322&chksm=bdcc34e28abbbdf4ecffc6b4f44298fe84e15bf6282f3034adc44a5a1246f804bfe4c987c263&sessionid=0&scene=126&ascene=3&devicetype=android-28&version=27000834&nettype=WIFI&abtest_cookie=AAACAA%3D%3D&lang=zh_CN&pass_ticket=gbbWrwxrFZvx7mUK9TCjMtilMGgXIA31fLxNQP%2BRh6UVnqQ75IyqlwDC0nZw4qoN&wx_header=1
    url=flow.request.url
    if ('/hq.sinajs.cn/rn' in url) and ('0' in url):
        # print(url)
        # ins=re.findall("nf_(.*?),","http://hq.sinajs.cn/rn=3ada9cec&list=nf_RR2111,nf_RR0,nf_RR2112,nf_RR2201,nf_RR22")
        res=flow.response.text
        pairs=re.findall('nf_(.*?),',res,re.S)
        q=map(lambda x:x.replace('"','').split('='),pairs)
        q=[{"chinese":i[0],"code":i[1]} for i in q]
        pd.DataFrame(q).to_csv('sgfghbd.csv', mode="a", encoding='utf_8_sig', index=False, index_label=False, header=False)



        # cur.close()
        print(30 * '#' + 'insert ok!!' + 16 * '@')
        print(30 * '#' + 'insert ok!!' + 16 * '@')
        print(30 * '#' + 'insert ok!!' + 16 * '@')
        print(30 * '#' + 'insert ok!!' + 16 * '@')
        print(30 * '#' + 'insert ok!!' + 16 * '@')

        #

        # raw=flow.response.text
        # push_items(raw=raw,obj=obj,url=flow.request.url)
        # with open('qwdqsdqwdq.txt','a',encoding='utf-8') as f:
        #     f.write(raw+8*'\n')
