# -*- encoding: utf-8 -*-
"""
@File    : 自动下单.py
@Time    : 2022/5/30 12:27
@Author  : handsomejerry
@Email   : 834235185@qq.com
@Software: PyCharm
"""
from pywinauto import Application
from pywinauto.win32functions import SetFocus
# 获取窗口对象
# 通过title及ClassName获取窗体对象
# app = Application(backend='uia').connect(path="C:\\Program Files (x86)\\Windows Kits\\10\\bin\\10.0.22621.0\\inspect.exe")
app = Application(backend='uia').connect(process=17912)
weixin_pc_window = app.window(title=u"yangwin-上期技术")
