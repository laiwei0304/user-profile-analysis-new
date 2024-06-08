import pandas as pd
from sqlalchemy import create_engine
from MLModelTools import MLModelTools


class DoUsgTag(object):

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

        # 读取数据
        df = pd.read_sql('SELECT * FROM tbl_goods', con=engine)
        orders_df = pd.read_sql('SELECT memberId, orderSn AS cOrderSn FROM tbl_orders_new', con=engine)

        # 关联订单数据
        goods_df = df.merge(orders_df, on='cOrderSn', how='left')

        # 构建颜色和商品类别列
        color_map = {"香槟色": 1, "樱花粉": 2, "月光银": 3, "灰色": 4, "金属灰": 5, "金色": 6, "布朗灰": 7, "蓝色": 8,
                     "金属银": 9, "香槟金": 10, "乐享金": 11, "粉色": 12, "玫瑰金": 13, "浅金棕": 14, "银色": 15,
                     "布鲁钢": 16, "卡其金": 17, "白色": 18, "黑色": 19}
        product_map = {"Haier/海尔冰箱": 1, "波轮洗衣机": 2, "燃气灶": 3, "净水机": 4, "取暖电器": 5, "智能电视": 6,
                       "烤箱": 7,
                       "挂烫机": 8, "嵌入式厨电": 9, "吸尘器/除螨仪": 10, "烟灶套系": 11, "微波炉": 12, "LED电视": 13,
                       "电水壶/热水瓶": 14, "电饭煲": 15, "冷柜": 16, "Leader/统帅冰箱": 17, "前置过滤器": 18,
                       "冰吧": 19,
                       "电风扇": 20, "4K电视": 21, "电热水器": 22, "破壁机": 23, "燃气热水器": 24, "料理机": 25,
                       "滤芯": 26,
                       "电磁炉": 27, "空气净化器": 28, "其他": 29}

        goods_df['color'] = goods_df['ogColor'].map(color_map).fillna(0).astype(int)
        goods_df['product'] = goods_df['productType'].map(product_map).fillna(0).astype(int)

        # 标注标签
        label_conditions = ((goods_df['ogColor'].isin(["樱花粉", "粉色", "白色", "玫瑰金", "香槟色", "香槟金"])) |
                            (goods_df['productType'].isin(["料理机", "烤箱", "电饭煲", "挂烫机", "破壁机", "微波炉",
                                                           "波轮洗衣机", "取暖电器", "吸尘器/除螨仪"])))
        goods_df['label'] = label_conditions.astype(int)

        # 使用模型进行预测
        model = MLModelTools.load_model(goods_df[['color', 'product', 'label']], "usg")
        predictions = model.predict(goods_df[['color', 'product']])
        goods_df['prediction'] = predictions

        # 按照用户ID分组，统计每个用户购物男性或女性商品个数及占比
        gender_df = goods_df.groupby('memberId').agg(
            total=('memberId', 'count'),
            maleTotal=('prediction', lambda x: (x == 0).sum()),
            femaleTotal=('prediction', lambda x: (x == 1).sum())
        ).reset_index()

        # 自定义函数，计算占比，确定标签值
        def gender_tag(row):
            male_rate = row['maleTotal'] / row['total']
            female_rate = row['femaleTotal'] / row['total']
            if male_rate >= 0.5:
                return '0'
            elif female_rate >= 0.5:
                return '1'
            else:
                return '-1'

        gender_df['usg'] = gender_df.apply(gender_tag, axis=1)

        # 读取基础标签表tbl_basic_tags
        df2 = pd.read_sql('SELECT * FROM tbl_basic_tags', con=engine)
        attr = df2.query("level == 5 and pid == 318")[['name', 'rule']]

        # 转换rule列为字符串类型
        attr['rule'] = attr['rule'].astype(str)

        # 打标签
        rst = gender_df.merge(attr, left_on='usg', right_on='rule', how='left')
        rst = rst.drop(columns=['rule']).rename(columns={'name': 'usg'})
        rst = rst.sort_values(by='memberId')

        # 存储打好标签的数据
        rst.to_sql('tbl_usg_tag', con=engine, if_exists='replace', index=False)
        print("用户购物性别标签计算完成！")


if __name__ == '__main__':
    DoUsgTag.start()
