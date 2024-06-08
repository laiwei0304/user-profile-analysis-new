import pandas as pd
from sqlalchemy import create_engine
from sklearn.preprocessing import MinMaxScaler
from sklearn.cluster import KMeans
from MLModelTools import MLModelTools


class DoRfmTag(object):

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

        # 计算RFM值
        rfm_df = df.groupby("memberId").agg(
            max_finishTime=pd.NamedAgg(column="finishTime", aggfunc="max"),
            frequency=pd.NamedAgg(column="orderSn", aggfunc="count"),
            monetary=pd.NamedAgg(column="orderAmount", aggfunc="sum")
        ).reset_index()
        rfm_df['recency'] = (pd.to_datetime('now') - pd.to_datetime(rfm_df['max_finishTime'])).dt.days - 90

        # 按照RFM值进行打分
        rfm_df['r_score'] = pd.cut(rfm_df['recency'], bins=[0, 3, 6, 9, 15, float('inf')], labels=[5, 4, 3, 2, 1],
                                   right=False).astype(float)
        rfm_df['f_score'] = pd.cut(rfm_df['frequency'], bins=[0, 49, 99, 149, 199, float('inf')],
                                   labels=[1, 2, 3, 4, 5], right=False).astype(float)
        rfm_df['m_score'] = pd.cut(rfm_df['monetary'], bins=[0, 9999, 49999, 99999, 199999, float('inf')],
                                   labels=[1, 2, 3, 4, 5], right=False).astype(float)

        # 使用RFM_SCORE进行KMeans聚类（K=5）
        model = MLModelTools.load_model(rfm_df[['r_score', 'f_score', 'm_score']], "rfm")
        rfm_df['prediction'] = model.predict(rfm_df[['r_score', 'f_score', 'm_score']])


        # 获取聚类中心，并根据rfm大小修改索引
        centers = model.cluster_centers_
        index_map = {i: v for i, v in enumerate(centers.flatten())}
        rfm_df['cluster'] = rfm_df['prediction'].map(index_map)

        # 读取基础标签表tbl_basic_tags
        df2 = pd.read_sql('SELECT * FROM tbl_basic_tags', con=engine)
        attr = df2.query("level == 5 and pid == 301")[['name', 'rule']]

        # 转换rule列为字符串类型
        attr['rule'] = attr['rule'].astype(str)

        # 打标签
        rst = rfm_df.merge(attr, left_on='prediction', right_on='rule', how='left')
        rst = rst.drop(columns=['prediction', 'rule']).rename(columns={'name': 'rfm'})
        rst = rst.sort_values(by='memberId')

        # 存储打好标签的数据
        rst.to_sql('tbl_rfm_tag', con=engine, if_exists='replace', index=False)
        print("用户价值标签计算完成！")


if __name__ == '__main__':
    DoRfmTag.start()
