import pandas as pd
from sqlalchemy import create_engine

class DoPayTypeTag(object):

    @staticmethod
    def start():
        # MySQL 配置
        user = 'userbb'
        password = 'userbb'
        host = '8.130.94.175'
        port = '3306'
        database = 'tags_dat'
        url = f'mysql+pymysql://{user}:{password}@{host}:{port}/{database}'

        # 创建数据库引擎
        engine = create_engine(url)

        # 读取基础标签表 tbl_basic_tags
        df = pd.read_sql('SELECT * FROM tbl_basic_tags', con=engine)

        # 取四级标签的 rule，支付方式id=223
        rule_row = df.query("level == 4 and id == 223").iloc[0]
        rule = rule_row['rule'].split(";")
        selectTable = rule[0].split("=")[1]
        selectField = rule[1].split("=")[1].split("|")

        # 取五级标签
        attr = df.query("level == 5 and pid == 223")[['name', 'rule']]

        # 读取订单表
        df2 = pd.read_sql(f'SELECT {", ".join(selectField)} FROM {selectTable}', con=engine)

        # 按照用户ID分组，再按支付编码分组，统计次数并获取最多次数的支付方式
        payment_df = df2.groupby(['memberId', 'paymentCode']).size().reset_index(name='count')
        payment_df['rank'] = payment_df.groupby('memberId')['count'].rank(method='first', ascending=False)
        payment_df = payment_df[payment_df['rank'] == 1][['memberId', 'paymentCode']]

        # 合并支付方式标签
        result_df = payment_df.merge(attr, left_on='paymentCode', right_on='rule', how='left')
        result_df = result_df[['memberId', 'name']].rename(columns={'memberId': 'userId', 'name': 'payment'})

        # 存储打好标签的数据
        result_df.to_sql('tbl_payType_tag', con=engine, if_exists='replace', index=False)
        print("支付方式标签计算完成！")

if __name__ == '__main__':
    DoPayTypeTag.start()
