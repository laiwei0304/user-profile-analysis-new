import pandas as pd
from sqlalchemy import create_engine
import numpy as np


class DoMaxOrderTag(object):

    @staticmethod
    def start():
        # 数据库连接配置
        engine = create_engine('mysql+pymysql://userbb:userbb@8.130.94.175:3306/test')

        # 读取表数据
        df = pd.read_sql('SELECT * FROM tbl_basic_tags', engine)

        # 取四级标签的rule
        rule = df[(df['level'] == 4) & (df['id'] == 230)]['rule'].iloc[0].split(";")
        selectTable = rule[0].split("=")[1]
        selectField = rule[1].split("=")[1].split("|")

        # 取五级标签
        # attr = df[(df['level'] == 5) & (df['pid'] == 230)]
        # attr = attr[['name', 'rule']].apply(lambda x: (x['name'], *eval(x['rule'])), axis=1, result_type='expand')
        # attr = attr[['name', 'rule']].apply(lambda x: (x['name'], eval(x['rule'])), axis=1, result_type='expand')
        # attr.columns = ['name', 'start', 'end']

        # 过滤出level为5且pid为230的行
        filtered_df = df[(df['level'] == 5) & (df['pid'] == 230)]

        def rule_to_tuple(rule_str):
            # 这里应该是将rule字符串转换为(start, end)元组的逻辑
            # 例如，如果rule的格式是"start-end"，你可以这样实现：
            start, end = rule_str.split('-')
            return int(start), int(end)

            # 应用上面的函数到'rule'列，并创建新的列'start'和'end'

        filtered_df[['start', 'end']] = pd.DataFrame(filtered_df['rule'].apply(rule_to_tuple).tolist(),
                                                     index=filtered_df.index)

        # 选择需要的列
        attr = filtered_df[['name', 'start', 'end']]
        print(attr.head())
        # 读取order_new表
        df2 = pd.read_sql(f'SELECT {", ".join(selectField)} FROM {selectTable}', engine)

        # 按照用户ID分组，获取最大订单金额
        maxOrderDF = df2.groupby('memberId')['orderAmount'].max().reset_index().rename(
            columns={'orderAmount': 'max_orderAmount'})
        #
        print(maxOrderDF.head())

        # Filter rows where max_orderAmount is between start and end
        # filtered_df= joined_df[(joined_df['max_orderAmount'] >= joined_df['start']) &
        #                         (joined_df['max_orderAmount'] <= joined_df['end'])]

        # Select and rename the columns
        newtable = maxOrderDF[['memberId', 'max_orderAmount']].copy()

        # 确定每个memberId的max_orderAmount所属的name范围
        def find_range_name(row):
            for _, attr_row in attr.iterrows():
                if attr_row['start'] <= row['max_orderAmount'] <= attr_row['end']:
                    return attr_row['name']
            return None  # 如果没有找到匹配的范围，则返回None（理论上应该不会发生）

        # 应用上面的函数到每一行数据上
        newtable['name'] = newtable.apply(find_range_name, axis=1)

        # 显示结果
        print(newtable)

        rst = newtable[['memberId', 'max_orderAmount', 'name']].rename(
            columns={'memberId': 'userId', 'name': 'maxOrderRange'})

        #
        # 连接attr，并筛选出max_orderAmount在start和end之间的行
        # rst = pd.merge(maxOrderDF, attr, how='left', on=None)  # 在pandas中，on=None表示笛卡尔积，然后通过query进行筛选
        # rst = rst.query('max_orderAmount >= start & max_orderAmount <= end')

        # 重命名列并选择需要的列
        # rst = rst.rename(columns={'memberId': 'userId'}).loc[:, ['userId', 'max_orderAmount', 'name']]
        # rst = rst.rename(columns={'name': 'maxOrderRange'})

        # 将结果写回数据库
        rst.to_sql('tbl_maxOrder_tag', engine, if_exists='replace', index=False)

        print("单笔最高标签计算完成！")


if __name__ == "__main__":
    # 当脚本作为主程序运行时，调用start方法
    DoMaxOrderTag.start()
