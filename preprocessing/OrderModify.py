import pandas as pd
from sqlalchemy import create_engine
import random

'''
class OrderModify(object):

    @staticmethod
    def user_id_udf(userId):
        if int(userId) >= 950:
            return str(random.randint(1, 950))
        else:
            return userId

    @staticmethod
    def pay_code_udf(paymentcode):
        paycodeList = ["alipay", "wxpay", "chinapay", "cod", "kjtpay", "giftCard"]
        if paymentcode not in paycodeList:
            return random.choice(paycodeList)
        else:
            return paymentcode

    @staticmethod
    def pay_name_udf(paymentcode):
        payMap = {
            "alipay": "支付宝",
            "wxpay": "微信支付",
            "chinapay": "银联支付",
            "cod": "货到付款",
            "kjtpay": "快捷通",
            "giftCard": "礼品卡"
        }
        return payMap.get(paymentcode, "未知支付方式")

    @staticmethod
    def start():
        # MySQL 配置
        user = 'userbb'
        password = 'userbb'
        host = '8.130.94.175'
        port = '3306'
        database = 'test'
        url = f'mysql+pymysql://{user}:{password}@{host}:{port}/{database}'

        # 创建数据库引擎
        engine = create_engine(url)

        # 读取表
        df = pd.read_sql('SELECT * FROM tbl_orders', con=engine)

        # 修改数据
        df['memberId'] = df['memberId'].apply(OrderModify.user_id_udf)
        df['paymentCode'] = df['paymentCode'].apply(OrderModify.pay_code_udf)
        df['paymentName'] = df['paymentCode'].apply(OrderModify.pay_name_udf)
        df['finishTime'] = pd.to_datetime(df['finishTime'], unit='s') + pd.Timedelta(days=1640)

        # 显示转换后的DataFrame
        print(df.head(185))

        # 写回数据库
        df.to_sql('tbl_orders_new', con=engine, if_exists='replace', index=False)
        print("订单数据修改完成！")

if __name__ == '__main__':
    OrderModify.start()
'''