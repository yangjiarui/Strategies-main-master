# -*- encoding: utf-8 -*-
"""
@File    : zcsadwasc.py
@Time    : 2021/10/13 17:35
@Author  : handsomejerry
@Email   : 834235185@qq.com
@Software: PyCharm
"""
import tushare as ts
# ts.get_cpi()
pro = ts.pro_api('91cc69761e7d6de70cc2f7214e6adf1154b8b5c6013953a09dd4b29b')

df = pro.shibor(start_date='20180101', end_date='20181101')
print(df)