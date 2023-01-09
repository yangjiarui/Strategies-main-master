import json

import requests



webhookurl='https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=ba0c5e73-078d-419f-bf16-6f445fd4292c'

def send_msg(lis):
    f=''
    for i in lis:
        f+=(">**{time}**   **{bid}**:  <font color=\"warnning\">{operate}</font>".format(
            time=i[0],bid=i[1],operate=i[2]
        )+'\n')
    data= {
        "msgtype": "markdown",
        "markdown": {
            "content": f'''实时操作提示:，请注意。\n
                       {f}
                                       ''',
            "mentioned_list":["@all"],
            "mentioned_mobile_list":["@all"]
        }
    }
    df=requests.post(url=webhookurl,headers={"Content-Type": "application/json"}
                  ,data=json.dumps(data))
    print(df.text)

# send_msg([("2022-01-03","m2209","buy"),("2022-01-03","c2209","sell")])