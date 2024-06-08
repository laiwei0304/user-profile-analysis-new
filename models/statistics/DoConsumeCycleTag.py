import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime
from tools.TagTools import rule_to_tuple


class DoConsumeCycleTag(object):

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
        rule_row = df.query("level == 4 and id == 208").iloc[0]
        rule = rule_row['rule'].split(";")
        selectTable = rule[0].split("=")[1]
        selectField = rule[1].split("=")[1].split("|")

        # 取五级标签
        attr = df.query("level == 5 and pid == 208")[['name', 'rule']]
        attr[['start', 'end']] = attr['rule'].apply(rule_to_tuple).apply(pd.Series)

        # 读取订单表
        df2 = pd.read_sql(f'SELECT {", ".join(selectField)} FROM {selectTable}', con=engine)

        # 将finishTime转换为datetime格式
        df2['finishTime'] = pd.to_datetime(df2['finishTime'], unit='s')

        # 按照用户ID分组，获取最大订单完成时间
        consumer_days_df = df2.groupby('memberId')['finishTime'].max().reset_index()
        consumer_days_df.columns = ['userId', 'max_finishTime']

        # 获取当前日期时间
        consumer_days_df['now_time'] = datetime.now()

        # 计算天数差
        consumer_days_df['consumer_days'] = (consumer_days_df['now_time'] - consumer_days_df['max_finishTime']).dt.days

        # 打标签
        results = []
        for _, row in attr.iterrows():
            temp_df = consumer_days_df[
                (consumer_days_df['consumer_days'] >= row['start']) & (consumer_days_df['consumer_days'] <= row['end'])]
            temp_df['consumptionCycle'] = row['name']
            results.append(temp_df[['userId', 'consumptionCycle']])

        rst = pd.concat(results)

        # 存储打好标签的数据
        rst.to_sql('tbl_consumeCycle_tag', con=engine, if_exists='replace', index=False)
        print("消费周期标签计算完成！")


if __name__ == '__main__':
    DoConsumeCycleTag.start()
