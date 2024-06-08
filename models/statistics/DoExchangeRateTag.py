import pandas as pd
from sqlalchemy import create_engine
from tools.TagTools import rule_to_tuple


class DoExchangeRateTag(object):

    # @staticmethod
    # def rule_to_tuple(rule):
    #     start, end = map(int, rule.split("-"))
    #     return start, end

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

        # 读取基础标签表 tbl_basic_tags
        df = pd.read_sql('SELECT * FROM tbl_basic_tags', con=engine)

        # 取四级标签的 rule
        rule_row = df.query("level == 4 and id == 244").iloc[0]
        rule = rule_row['rule'].split(";")
        selectTable = rule[0].split("=")[1]
        selectField = rule[1].split("=")[1].split("|")

        # 取五级标签
        attr = df.query("level == 5 and pid == 244")[['name', 'rule']]
        attr[['start', 'end']] = attr['rule'].apply(rule_to_tuple).apply(pd.Series)

        # 读取订单表
        df2 = pd.read_sql(f'SELECT {", ".join(selectField)} FROM {selectTable}', con=engine)

        # 将换货状态的改为1，其他变成0
        df2['orderStatus'] = df2['orderStatus'].apply(lambda x: 1 if x == 1 else 0)

        # 按照用户ID分组，获取订单数量
        count_exchange_df = df2.groupby('memberId')['orderStatus'].sum().reset_index()
        count_exchange_df.columns = ['userId', 'exchangeRate']

        # 打标签
        results = []
        for _, row in attr.iterrows():
            temp_df = count_exchange_df[
                (count_exchange_df['exchangeRate'] >= row['start']) & (count_exchange_df['exchangeRate'] <= row['end'])]
            temp_df['value'] = row['name']
            results.append(temp_df[['userId', 'value', 'exchangeRate']])

        rst = pd.concat(results)

        # 存储打好标签的数据
        rst.to_sql('tbl_exchangeRate_tag', con=engine, if_exists='replace', index=False)
        print("换货率标签计算完成！")


if __name__ == '__main__':
    DoExchangeRateTag.start()
