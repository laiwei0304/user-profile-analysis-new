import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime
from tools.TagTools import rule_to_tuple


class DoLogFrequencyTag(object):

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
        rule_row = df.query("level == 4 and id == 254").iloc[0]
        rule = rule_row['rule'].split(";")
        selectTable = rule[0].split("=")[1]
        selectField = rule[1].split("=")[1].split("|")

        # 取五级标签
        attr = df.query("level == 5 and pid == 254")[['name', 'rule']]
        attr[['start', 'end']] = attr['rule'].apply(rule_to_tuple).apply(pd.Series)

        # 读取日志表
        df2 = pd.read_sql(f'SELECT {", ".join(selectField)} FROM {selectTable}', con=engine)

        # 检查数据是否正确读取
        print("日志数据预览：")
        print(df2.head())

        # 获取当前日期时间
        now_time = datetime.now()

        # 转换 log_time 为 datetime 类型
        df2['log_time'] = pd.to_datetime(df2['log_time'])

        # 计算 log_time 与当前时间的差值，并过滤掉 log_time 在一个月之前的数据
        df2['logCycle'] = (now_time - df2['log_time']).dt.days
        df2 = df2[df2['logCycle'] < 120]

        # 检查过滤后的数据
        print("过滤后的日志数据预览：")
        print(df2.head())

        # 按照用户ID分组，获取一个月内的log数量
        count_log_df = df2.groupby('global_user_id')['logCycle'].count().reset_index()
        count_log_df.columns = ['userId', 'frequency']

        # 检查分组后的数据
        print("分组后的日志数据预览：")
        print(count_log_df.head())

        # 打标签
        results = []
        for _, row in attr.iterrows():
            temp_df = count_log_df[
                (count_log_df['frequency'] >= row['start']) & (count_log_df['frequency'] <= row['end'])].copy()
            if not temp_df.empty:
                temp_df['value'] = row['name']
                results.append(temp_df[['userId', 'value', 'frequency']])

        if results:
            rst = pd.concat(results)

            # 存储打好标签的数据
            rst.to_sql('tbl_logFrequency_tag', con=engine, if_exists='replace', index=False)
            print("访问频率标签计算完成！")
        else:
            print("没有符合条件的数据进行标签化。")


if __name__ == '__main__':
    DoLogFrequencyTag.start()
