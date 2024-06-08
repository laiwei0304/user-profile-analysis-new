import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime
from TagTools import rule_to_tuple

class DoLastLoginTag(object):

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
        database = 'tags_dat'
        url = f'mysql+pymysql://{user}:{password}@{host}:{port}/{database}'

        # 创建数据库引擎
        engine = create_engine(url)

        # 读取基础标签表 tbl_basic_tags
        df = pd.read_sql('SELECT * FROM tbl_basic_tags', con=engine)

        # 取四级标签的 rule
        rule_row = df.query("level == 4 and id == 248").iloc[0]
        rule = rule_row['rule'].split(";")
        selectTable = rule[0].split("=")[1]
        selectField = rule[1].split("=")[1].split("|")

        # 取五级标签
        attr = df.query("level == 5 and pid == 248")[['name', 'rule']]
        attr[['start', 'end']] = attr['rule'].apply(rule_to_tuple).apply(pd.Series)

        # 读取用户表
        df2 = pd.read_sql(f'SELECT {", ".join(selectField)} FROM {selectTable}', con=engine)

        # 将lastLoginTime转换为datetime格式
        df2['lastLoginTime'] = pd.to_datetime(df2['lastLoginTime'], unit='s')

        # 获取当前日期时间
        df2['now_time'] = datetime.now()

        # 计算天数差
        df2['lastLogin'] = (df2['now_time'] - df2['lastLoginTime']).dt.days

        # 打标签
        results = []
        for _, row in attr.iterrows():
            temp_df = df2[(df2['lastLogin'] >= row['start']) & (df2['lastLogin'] <= row['end'])]
            temp_df['loginCycle'] = row['name']
            results.append(temp_df[['id', 'loginCycle']].rename(columns={'id': 'userId'}))

        rst = pd.concat(results)

        # 存储打好标签的数据
        rst.to_sql('tbl_lastLogin_tag', con=engine, if_exists='replace', index=False)
        print("最近登录标签计算完成！")

if __name__ == '__main__':
    DoLastLoginTag.start()
