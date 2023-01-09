# -*- encoding: utf-8 -*-
"""
@File    : sendmsg.py
@Time    : 2020/3/3 16:37
@Author  : handsomejerry
@Email   : 834235185@qq.com
@Software: PyCharm
"""
import datetime
import smtplib,requests
import numpy as np
from email.mime.text import MIMEText

from apscheduler.schedulers.blocking import BlockingScheduler


def sen_email(msg_to,content=None,subject='你好'):
    msg_from = '834235185@qq.com'  # 发送方邮箱地址。
    # msg_from = 'yangjiarui0032021@163.com'  # 发送方邮箱地址。
    password = 'nvllyfalxpgvbcaf'  # 发送方QQ邮箱授权码，不是QQ邮箱密码。
    # password = 'JGGFIVFNETJHZRLH'  # 发送方QQ邮箱授权码，不是QQ邮箱密码。

    # msg_to = '834235185@qq.com'  # 收件人邮箱地址。
    if not content:
        id=np.random.randint(300,500)
        content=requests.get(f'https://api.mcloc.cn/love/?type=string&id={id}').text

    # subject = "你好"  # 主题。
    # content = "i am zhangphil"  # 邮件正文内容。
    msg = MIMEText(content, 'plain', 'utf-8')
    now=datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    msg['Subject'] = subject+f' 现在时间:{now}'
    msg['From'] = msg_from
    msg['To'] = msg_to
    # msg['Content'] = f'\r\n\n  \t\t  {content}'

    try:
        client = smtplib.SMTP_SSL('smtp.qq.com', smtplib.SMTP_SSL_PORT)
        # client = smtplib.SMTP_SSL('smtp.163.com', smtplib.SMTP_SSL_PORT)
        print("连接到邮件服务器成功")
        client.login(msg_from, password)
        print("登录成功")
        # for i in msg_to:
        client.sendmail(msg_from, msg_to, msg.as_string())
        print("发送成功")
    except smtplib.SMTPException as e:
        print("发送邮件异常")
    finally:
        client.quit()
#
# if __name__ == '__main__':
#     sched1=BlockingScheduler()
#     # real_time(code)
#
#
#     sched1.add_job(sen_email,'interval',seconds=60,kwargs={"msg_to":'1922335454@qq.com'})
#     sched1.start()

