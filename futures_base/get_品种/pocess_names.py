# -*- encoding: utf-8 -*-
"""
@File    : pocess_names.py
@Time    : 2021/9/9 20:32
@Author  : handsomejerry
@Email   : 834235185@qq.com
@Software: PyCharm
"""
import re

import pandas as pd

df=pd.read_csv('sgfghbd.csv',header=None)
with open('dolphin_names.txt','w') as f:
    f.write(','.join('nf_'+i for i in df.iloc[:,0]))
    f.write('\n')
    f.write('\n')
    f.write('\n')
    q=re.findall('([\u4e00-\u9fa5]+).*?,',','.join(df.iloc[:,-1]))
    f.write('`'+'`'.join(list(set(q))))

print('done')


