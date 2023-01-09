# -*- encoding: utf-8 -*-
"""
@File    : 定时重启项目.py
@Time    : 2023/1/6 16:37
@Author  : handsomejerry
@Email   : 834235185@qq.com
@Software: PyCharm
"""
import requests
try:
    requests.get('http://localhost:1234/index.html?action=restartall',timeout=50)
except:
    pass