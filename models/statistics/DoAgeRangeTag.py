import pandas as pd
from sqlalchemy import create_engine
from TagTools import rule_to_tuple


class DoAgeRangeTag(object):

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
        rule_row = df.query("level == 4 and id == 201").iloc[0]
        rule = rule_row['rule'].split(";")
        selectTable = rule[0].split("=")[1]
        selectField = rule[1].split("=")[1].split("|")

        # 取五级标签
        attr = df.query("level == 5 and pid == 201")[['name', 'rule']]
        attr[['start', 'end']] = attr['rule'].apply(rule_to_tuple).apply(pd.Series)

        # 读取用户表
        df2 = pd.read_sql(f'SELECT {", ".join(selectField)} FROM {selectTable}', con=engine)

        # 转换生日日期格式
        df2['bornDate'] = df2['birthday'].str.replace("-", "").astype(int)

        # 打标签
        results = []
        for _, row in attr.iterrows():
            temp_df = df2[(df2['bornDate'] >= row['start']) & (df2['bornDate'] <= row['end'])]
            temp_df['ageRange'] = row['name']
            results.append(temp_df[['id', 'ageRange']].rename(columns={'id': 'userId'}))

        rst = pd.concat(results)

        # 存储打好标签的数据
        rst.to_sql('tbl_ageRange_tag', con=engine, if_exists='replace', index=False)
        print("年龄段标签计算完成！")


if __name__ == '__main__':
    DoAgeRangeTag.start()
