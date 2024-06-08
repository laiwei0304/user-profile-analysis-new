import pandas as pd
from sqlalchemy import create_engine

class DoPoliticalFaceTag(object):

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

        # 读取基础标签表tbl_basic_tags
        df = pd.read_sql('SELECT * FROM tbl_basic_tags', con=engine)

        # 从基础标签表tbl_basic_tags中提取规则，存入字典rule中
        rule_row = df.query("level == 4 and id == 109").iloc[0]
        rule = rule_row['rule']
        if not rule:
            raise Exception("政治面貌标签未提供数据源信息，无法获取业务数据")
        else:
            rule = rule.split(";")

        # 提取rule字典中的selectTable和selectField
        selectTable = rule[0].split("=")[1]
        selectField = rule[1].split("=")[1].split("|")

        # 从基础标签表中提取该4级标签对应5级标签的名称和规则
        attr = df.query("level == 5 and pid == 109")[["name", "rule"]]

        # 从selectTable中提取selectField字段
        df2 = pd.read_sql(f'SELECT {", ".join(selectField)} FROM {selectTable}', con=engine)

        # 转换politicalFace和rule列为字符串类型
        df2['politicalFace'] = df2['politicalFace'].astype(str)
        attr['rule'] = attr['rule'].astype(str)

        # 打标签（不同模型不一样）
        rst = df2.merge(attr, left_on="politicalFace", right_on="rule", how="left") \
            .drop(columns=["politicalFace", "rule"]) \
            .rename(columns={"name": "politicalFace", "id": "userId"}) \
            .sort_values(by="userId")

        # 存储打好标签的数据
        rst.to_sql('tbl_politicalface_tag', con=engine, if_exists='replace', index=False)
        print("政治面貌标签计算完成！")

# 运行代码
if __name__ == '__main__':
    DoPoliticalFaceTag.start()