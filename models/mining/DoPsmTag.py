import pandas as pd
from sqlalchemy import create_engine
from sklearn.preprocessing import MinMaxScaler
from MLModelTools import MLModelTools


class DoPsmTag(object):

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
        df = pd.read_sql('SELECT * FROM tbl_orders_new', con=engine)

        # 计算PSM值
        df['ra'] = df['orderAmount'] + df['couponCodeValue']
        df['da'] = df['couponCodeValue']
        df['pa'] = df['orderAmount']
        df['state'] = df['couponCodeValue'].apply(lambda x: 0 if x == 0 else 1)

        grouped = df.groupby('memberId').agg(
            tdon=('state', 'sum'),
            ton=('state', 'count'),
            tda=('da', 'sum'),
            tra=('ra', 'sum')
        ).reset_index()

        grouped['tdonr'] = grouped['tdon'] / grouped['ton']
        grouped['tdar'] = grouped['tda'] / grouped['tra']
        grouped['adar'] = (grouped['tda'] / grouped['tdon']) / (grouped['tra'] / grouped['ton'])
        grouped['psm'] = grouped['tdonr'] + grouped['tdar'] + grouped['adar']
        grouped['psm_score'] = grouped['psm'].fillna(0.00000001)

        # 使用RFM_SCORE进行KMeans单列聚类（K=5）
        model = MLModelTools.load_model(grouped[['psm_score']], "psm")
        grouped['prediction'] = model.predict(grouped[['psm_score']])

        # 获取聚类中心，并根据psm大小修改索引
        centers = model.cluster_centers_
        index_map = {i: v for i, v in enumerate(centers.flatten())}
        grouped['cluster'] = grouped['prediction'].map(index_map)

        # 读取基础标签表tbl_basic_tags
        df2 = pd.read_sql('SELECT * FROM tbl_basic_tags', con=engine)
        attr = df2[(df2['level'] == 5) & (df2['pid'] == 312)][['name', 'rule']]

        # 转换rule列为字符串类型
        attr['rule'] = attr['rule'].astype(str)

        # 打标签
        rst = grouped.merge(attr, left_on='prediction', right_on='rule', how='left')
        rst = rst.drop(columns=['prediction', 'rule']).rename(columns={'name': 'psm'})
        rst = rst.sort_values(by='memberId')

        # 存储打好标签的数据
        rst.to_sql('tbl_psm_tag', con=engine, if_exists='replace', index=False)
        print("价格敏感度标签计算完成！")


if __name__ == '__main__':
    DoPsmTag.start()
