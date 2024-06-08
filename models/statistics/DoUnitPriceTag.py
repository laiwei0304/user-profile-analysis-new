import pandas as pd
from sqlalchemy import create_engine
from tools.TagTools import rule_to_tuple

class DoUnitPriceTag(object):

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
        rule_row = df.query("level == 4 and id == 217").iloc[0]
        rule = rule_row['rule'].split(";")
        selectTable = rule[0].split("=")[1]
        selectField = rule[1].split("=")[1].split("|")

        # 取五级标签
        attr = df.query("level == 5 and pid == 217")[['name', 'rule']]
        attr[['start', 'end']] = attr['rule'].apply(rule_to_tuple).apply(pd.Series)

        # 读取订单表
        df2 = pd.read_sql(f'SELECT {", ".join(selectField)} FROM {selectTable}', con=engine)

        # 按照用户ID分组，获取所有订单总金额和订单数量
        order_amount_df = df2.groupby('memberId').agg(total_orderAmount=('orderAmount', 'sum'), total_order=('memberId', 'count')).reset_index()

        # 计算客单价
        order_amount_df['unitPrice'] = round(order_amount_df['total_orderAmount'] / order_amount_df['total_order'], 2)

        # 打标签
        results = []
        for _, row in attr.iterrows():
            temp_df = order_amount_df[(order_amount_df['unitPrice'] >= row['start']) & (order_amount_df['unitPrice'] <= row['end'])].copy()
            temp_df = temp_df.assign(unitPriceRange=row['name'])
            results.append(temp_df[['memberId', 'unitPriceRange', 'unitPrice']].rename(columns={'memberId': 'userId'}))

        if results:
            rst = pd.concat(results)

            # 存储打好标签的数据
            rst.to_sql('tbl_unitPrice_tag', con=engine, if_exists='replace', index=False)
            print("客单价标签计算完成！")
        else:
            print("没有符合条件的数据进行标签化。")


if __name__ == '__main__':
    DoUnitPriceTag.start()
