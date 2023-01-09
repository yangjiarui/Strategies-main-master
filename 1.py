# -*- encoding: utf-8 -*-
"""
@File    : 1.py
@Time    : 2022/2/21 11:52
@Author  : handsomejerry
@Email   : 834235185@qq.com
@Software: PyCharm
"""
import pymssql
import pyodbc
from sqlalchemy import create_engine

# conn = pymssql.connect(host='192.168.202.68',
#                                database='MOFANGBI',
#                                user='sa',
#                                password='52MoFang')

####python manage.py runserver 0.0.0.0:2235 --noreload
####start nginx.exe



# con=create_engine('mssql+pymssql://sa:52MoFang@192.168.202.68:1433/MOFANGBI')

#
connection = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=192.168.202.58;DATABASE=FM;UID=FM_ADM;PWD=f0rM@n@ger')
# connection = pyodbc.connect('DRIVER={MySQL ODBC 5.3 Unicode Driver};SERVER=192.168.3.6;PORT=3306;DATABASE=test;UID=jihui;PWD=111111')
# connection = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=123.56.196.177;DATABASE=Test;UID=eason.chen;PWD=v2t4f2ch')
curs = connection.execute('select GETDATE()')
curs.fetchone()
#
# connection = pyodbc.connect('DRIVER={SQL Server Native Client 10.0};SERVER=123.56.196.177;DATABASE=Test;UID=eason.chen;PWD=v2t4f2ch')
# curs = connection.execute('select GETDATE()')
# curs.fetchone()

# conn = pymssql.connect(host='123.56.196.177',
#                                database='Test',
#                                user='eason.chen',
#                                password='v2t4f2ch')
#
# conn = pymssql.connect(host='192.168.202.58',
#                                database='FM',
#                                user='FM_ADM',
#                                password='f0rM@n@ger')